"""Extractor BCRA.

Descarga variables monetarias seleccionadas desde la API pública del Banco
Central de la República Argentina, agrega metadata de ingesta y sube el JSON
crudo a S3 Bronze.

Variables descargadas (por idVariable):
    1   — Tipo de cambio de referencia (mayorista)
    6   — BADLAR total (tasa en % nominal anual)
    7   — BADLAR bancos privados (tasa en % nominal anual)
    27  — Tasa de política monetaria (pases pasivos, antigua Leliq 28d)
    400 — Reservas internacionales del BCRA (en millones de USD)

Ruta S3:
    s3://{bucket}/bronze/bcra/year={YYYY}/month={MM}/day={DD}/bcra_{YYYYMMDD}.json

Variables de entorno requeridas:
    S3_DATA_LAKE_BUCKET  — nombre del bucket S3
    AWS_ACCESS_KEY_ID    — credencial AWS (o perfil/role configurado)
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

Referencias:
    https://api.bcra.gob.ar/estadisticas/v3.0/datosvariable/{idVariable}/{desde}/{hasta}
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

BCRA_BASE_URL = "https://api.bcra.gob.ar/estadisticas/v3.0/datosvariable"
SOURCE_NAME = "bcra"
REQUEST_TIMEOUT_SECONDS = 20

# idVariable → nombre semántico (documentados en el portal BCRA)
VARIABLES: dict[int, str] = {
    1: "tipo_cambio_mayorista_ref",
    6: "badlar_total",
    7: "badlar_privados",
    27: "tasa_politica_monetaria",
    400: "reservas_internacionales_musd",
}

_HEADERS: dict[str, str] = {
    "Accept": "application/json",
    "User-Agent": "macro-intelligence-pipeline/1.0",
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
        RuntimeError: si la descarga o el upload fallan después de reintentos.
    """
    run_id = run_id or str(uuid.uuid4())
    date = date or datetime.now(timezone.utc)

    logger.info("Iniciando extracción BCRA | run_id=%s | date=%s", run_id, date.date())

    raw_payload = _fetch_all_variables(date=date, run_id=run_id)
    enriched = _enrich(raw_payload, run_id=run_id, ingested_at=date)
    s3_key = _build_s3_key(date)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"bcra_{date.strftime('%Y%m%d')}_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(enriched, tmp, ensure_ascii=False, indent=2)
        tmp_path = Path(tmp.name)

    try:
        _upload_to_s3(local_path=tmp_path, s3_key=s3_key)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("Extracción BCRA completada | s3_key=%s", s3_key)
    return s3_key


# ── Funciones internas ────────────────────────────────────────────────────────


def _fetch_variable(id_variable: int, date: datetime) -> dict[str, Any]:
    """GET de una variable BCRA para la fecha indicada.

    La API acepta rango de fechas; se solicita solo el día deseado.
    Formato de fecha esperado por la API: YYYY-MM-DD.

    Args:
        id_variable: identificador numérico de la variable BCRA.
        date:        fecha de referencia.

    Returns:
        Diccionario con la respuesta cruda de la API para esa variable.

    Raises:
        RuntimeError: ante timeout, HTTP error o error de red.
    """
    date_str = date.strftime("%Y-%m-%d")
    url = f"{BCRA_BASE_URL}/{id_variable}/{date_str}/{date_str}"
    logger.debug("GET %s", url)

    try:
        response = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Timeout al consultar variable {id_variable}: {url}"
        ) from exc
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] HTTP {exc.response.status_code} consultando variable "
            f"{id_variable}: {url}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error de red consultando variable {id_variable}: {exc}"
        ) from exc

    return response.json()


def _fetch_all_variables(date: datetime, run_id: str) -> dict[str, Any]:
    """Itera sobre todas las variables de interés y consolida en un dict.

    Las variables fallidas se loguean pero no detienen la extracción de las
    demás (tolerancia a fallos parciales según diseño del pipeline).

    Args:
        date:   fecha de referencia.
        run_id: identificador del run (para trazabilidad en logs).

    Returns:
        Diccionario con una clave por variable semántica.
    """
    results: dict[str, Any] = {}

    for id_var, name in VARIABLES.items():
        try:
            payload = _fetch_variable(id_variable=id_var, date=date)
            results[name] = payload
            logger.debug("Variable %s (%s) obtenida correctamente", id_var, name)
        except RuntimeError as exc:
            logger.warning(
                "Variable %s (%s) falló | run_id=%s | error=%s",
                id_var,
                name,
                run_id,
                exc,
            )
            results[name] = None  # presencia explícita del campo aunque sea nulo

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
        "variables_requested": list(VARIABLES.values()),
        "data": payload,
    }


def _build_s3_key(date: datetime) -> str:
    """Construye la clave S3 con particionado Hive-style (diario)."""
    return (
        f"bronze/{SOURCE_NAME}/"
        f"year={date.year}/"
        f"month={date.month:02d}/"
        f"day={date.day:02d}/"
        f"{SOURCE_NAME}_{date.strftime('%Y%m%d')}.json"
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
