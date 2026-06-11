"""Extractor FRED (Federal Reserve Bank of St. Louis).

Descarga series macroeconómicas de USA desde la API pública de FRED, agrega
metadata de ingesta y sube el JSON crudo a S3 Bronze.

Series descargadas:
    DTWEXBGS — Broad USD Index (equivalente al DXY, actualización semanal)
    CPIAUCSL  — CPI All Urban Consumers (inflación USA, actualización mensual)

Ruta S3:
    s3://{bucket}/bronze/fred/year={YYYY}/month={MM}/fred_{YYYYMM}.json

    La partición es mensual (no diaria) porque las series tienen frecuencia
    mensual o semanal; se descarga la ventana del mes completo en cada ejecución.

Variables de entorno requeridas:
    S3_DATA_LAKE_BUCKET  — nombre del bucket S3
    AWS_ACCESS_KEY_ID    — credencial AWS (o perfil/role configurado)
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION
    FRED_API_KEY         — API key gratuita obtenida en https://fred.stlouisfed.org/

Notas de diseño:
    - Límite del free tier: 120 requests/minuto. Sin riesgo práctico a este volumen.
    - Se descarga siempre un rango de 90 días atrás desde la fecha de referencia
      para garantizar que el último dato mensual/semanal esté incluido aunque la
      ejecución sea a principios de mes.
    - La partición S3 se basa en el año/mes de la fecha de referencia, no en la
      última fecha de cada serie.

Referencias:
    https://fred.stlouisfed.org/docs/api/fred/series_observations.html
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
SOURCE_NAME = "fred"
REQUEST_TIMEOUT_SECONDS = 20
LOOKBACK_DAYS = 90  # ventana de descarga hacia atrás desde la fecha de referencia

# Series FRED de interés: serie_id → nombre semántico
SERIES: dict[str, str] = {
    "DTWEXBGS": "dxy_broad_usd_index",
    "CPIAUCSL": "cpi_usa_all_urban",
}


# ── Funciones públicas ────────────────────────────────────────────────────────


def extract(run_id: str | None = None, date: datetime | None = None) -> str:
    """Descarga, enriquece con metadata y sube a S3 Bronze.

    Args:
        run_id: identificador del pipeline run. Si es None se genera un UUID.
        date:   fecha de referencia del dato. Si es None se usa utcnow().

    Returns:
        s3_key: clave S3 donde quedó almacenado el archivo.

    Raises:
        RuntimeError: si la descarga o el upload fallan.
    """
    run_id = run_id or str(uuid.uuid4())
    date = date or datetime.now(timezone.utc)

    logger.info("Iniciando extracción FRED | run_id=%s | date=%s", run_id, date.date())

    api_key = _get_api_key()
    raw_payload = _fetch_all_series(date=date, api_key=api_key, run_id=run_id)
    enriched = _enrich(raw_payload, run_id=run_id, ingested_at=date)
    s3_key = _build_s3_key(date)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"fred_{date.strftime('%Y%m')}_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(enriched, tmp, ensure_ascii=False, indent=2)
        tmp_path = Path(tmp.name)

    try:
        _upload_to_s3(local_path=tmp_path, s3_key=s3_key)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("Extracción FRED completada | s3_key=%s", s3_key)
    return s3_key


# ── Funciones internas ────────────────────────────────────────────────────────


def _get_api_key() -> str:
    """Lee la FRED API key desde variables de entorno.

    Raises:
        RuntimeError: si la variable FRED_API_KEY no está definida.
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Variable de entorno FRED_API_KEY no definida. "
            "Obtené una gratis en https://fred.stlouisfed.org/"
        )
    return api_key


def _fetch_series(series_id: str, date: datetime, api_key: str) -> dict[str, Any]:
    """GET de observaciones de una serie FRED en la ventana de lookback.

    Args:
        series_id: identificador de la serie en FRED (ej: "CPIAUCSL").
        date:      fecha de referencia (extremo derecho del rango).
        api_key:   API key de FRED.

    Returns:
        Diccionario con la respuesta cruda del endpoint observations.

    Raises:
        RuntimeError: ante timeout, HTTP error o error de red.
    """
    observation_end = date.strftime("%Y-%m-%d")
    observation_start = (date - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    params: dict[str, str] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "observation_end": observation_end,
        "sort_order": "desc",
    }
    logger.debug(
        "GET FRED series=%s | start=%s | end=%s",
        series_id,
        observation_start,
        observation_end,
    )

    try:
        response = requests.get(
            FRED_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Timeout al consultar serie {series_id}"
        ) from exc
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] HTTP {exc.response.status_code} consultando "
            f"serie {series_id}: {FRED_BASE_URL}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error de red consultando serie {series_id}: {exc}"
        ) from exc

    data = response.json()

    # FRED devuelve HTTP 200 incluso en errores de API; verificar el campo error
    if "error_code" in data:
        raise RuntimeError(
            f"[{SOURCE_NAME}] FRED API error {data['error_code']}: "
            f"{data.get('error_message', 'sin detalle')} | serie={series_id}"
        )

    return data


def _fetch_all_series(
    date: datetime, api_key: str, run_id: str
) -> dict[str, Any]:
    """Itera sobre todas las series de interés y consolida en un dict.

    Las series fallidas se loguean pero no detienen la extracción del resto.

    Args:
        date:    fecha de referencia.
        api_key: API key de FRED.
        run_id:  identificador del run (para trazabilidad en logs).

    Returns:
        Diccionario con una clave por serie semántica.
    """
    results: dict[str, Any] = {}

    for series_id, semantic_name in SERIES.items():
        try:
            data = _fetch_series(series_id=series_id, date=date, api_key=api_key)
            results[semantic_name] = data
            obs_count = len(data.get("observations", []))
            logger.debug(
                "Serie %s (%s) obtenida | %d observaciones",
                series_id,
                semantic_name,
                obs_count,
            )
        except RuntimeError as exc:
            logger.warning(
                "Serie %s falló | run_id=%s | error=%s",
                series_id,
                run_id,
                exc,
            )
            results[semantic_name] = None

    return results


def _enrich(payload: dict[str, Any], *, run_id: str, ingested_at: datetime) -> dict[str, Any]:
    """Envuelve el payload crudo con metadata de ingesta.

    El JSON resultante preserva el response original bajo la clave 'data'
    para que Bronze sea siempre reproducible desde la fuente.
    """
    return {
        "source_name": SOURCE_NAME,
        "pipeline_run_id": run_id,
        "ingested_at": ingested_at.isoformat(),
        "series_requested": list(SERIES.keys()),
        "lookback_days": LOOKBACK_DAYS,
        "data": payload,
    }


def _build_s3_key(date: datetime) -> str:
    """Construye la clave S3 con particionado Hive-style (mensual).

    FRED tiene frecuencia mensual/semanal, se particiona por año/mes.
    """
    return (
        f"bronze/{SOURCE_NAME}/"
        f"year={date.year}/"
        f"month={date.month:02d}/"
        f"{SOURCE_NAME}_{date.strftime('%Y%m')}.json"
    )


def _upload_to_s3(local_path: Path, s3_key: str) -> None:
    """Sube el archivo local al bucket configurado en S3_DATA_LAKE_BUCKET."""
    bucket = os.environ.get("S3_DATA_LAKE_BUCKET")
    if not bucket:
        raise RuntimeError("Variable de entorno S3_DATA_LAKE_BUCKET no definida.")

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    try:
        s3 = boto3.client("s3", region_name=region)
        s3.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/json"},
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error al subir a s3://{bucket}/{s3_key}: {exc}"
        ) from exc

    logger.info("Upload exitoso | s3://%s/%s", bucket, s3_key)


# ── Ejecución local ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    key = extract()
    print(f"\nArchivo subido a S3: {key}")
