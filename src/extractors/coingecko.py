"""Extractor CoinGecko.

Descarga precios de cierre de BTC/USD y ETH/USD desde la API pública v3 de
CoinGecko (free tier), agrega metadata de ingesta y sube el JSON crudo a S3 Bronze.

Monedas descargadas:
    bitcoin  → BTC/USD
    ethereum → ETH/USD

Ruta S3:
    s3://{bucket}/bronze/coingecko/year={YYYY}/month={MM}/day={DD}/coingecko_{YYYYMMDD}.json

Variables de entorno requeridas:
    S3_DATA_LAKE_BUCKET   — nombre del bucket S3
    AWS_ACCESS_KEY_ID     — credencial AWS (o perfil/role configurado)
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION
    COINGECKO_API_KEY     — (opcional) API key Pro; si no está definida se usa el
                            endpoint público con rate limit reducido (30 rpm)

Notas de diseño:
    - El free tier de CoinGecko limita a 30 requests/minuto. Se agrega un sleep
      de 2 s entre llamadas para no acercarse al límite con garantía.
    - Se consulta el endpoint /coins/{id}/history que devuelve el precio de
      mercado del día indicado (precio de cierre UTC).

Referencias:
    https://docs.coingecko.com/reference/coins-id-history
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
SOURCE_NAME = "coingecko"
REQUEST_TIMEOUT_SECONDS = 20
SLEEP_BETWEEN_CALLS_SECONDS = 2.0  # respetar rate limit del free tier (30 rpm)

# Monedas de interés: coin_id CoinGecko → nombre semántico
COINS: dict[str, str] = {
    "bitcoin": "btc_usd",
    "ethereum": "eth_usd",
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

    logger.info(
        "Iniciando extracción CoinGecko | run_id=%s | date=%s", run_id, date.date()
    )

    raw_payload = _fetch_all_coins(date=date, run_id=run_id)
    enriched = _enrich(raw_payload, run_id=run_id, ingested_at=date)
    s3_key = _build_s3_key(date)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"coingecko_{date.strftime('%Y%m%d')}_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(enriched, tmp, ensure_ascii=False, indent=2)
        tmp_path = Path(tmp.name)

    try:
        _upload_to_s3(local_path=tmp_path, s3_key=s3_key)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("Extracción CoinGecko completada | s3_key=%s", s3_key)
    return s3_key


# ── Funciones internas ────────────────────────────────────────────────────────


def _build_headers() -> dict[str, str]:
    """Construye los headers HTTP; agrega la API key si está configurada."""
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "macro-intelligence-pipeline/1.0",
    }
    api_key = os.environ.get("COINGECKO_API_KEY")
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    return headers


def _fetch_coin(coin_id: str, date: datetime) -> dict[str, Any]:
    """GET del historial de una moneda para la fecha indicada.

    El endpoint /coins/{id}/history devuelve datos del mercado para una fecha
    específica en formato DD-MM-YYYY.

    Args:
        coin_id: identificador de la moneda en CoinGecko (ej: "bitcoin").
        date:    fecha de referencia.

    Returns:
        Diccionario con la respuesta cruda del endpoint history.

    Raises:
        RuntimeError: ante timeout, HTTP error o error de red.
    """
    date_str = date.strftime("%d-%m-%Y")  # formato requerido por CoinGecko
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/history"
    params: dict[str, str] = {
        "date": date_str,
        "localization": "false",
    }
    logger.debug("GET %s?date=%s", url, date_str)

    try:
        response = requests.get(
            url,
            headers=_build_headers(),
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Timeout al consultar {coin_id} en {date_str}: {url}"
        ) from exc
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] HTTP {exc.response.status_code} consultando "
            f"{coin_id} en {date_str}: {url}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error de red consultando {coin_id}: {exc}"
        ) from exc

    return response.json()


def _fetch_all_coins(date: datetime, run_id: str) -> dict[str, Any]:
    """Itera sobre todas las monedas de interés y consolida en un dict.

    Incluye sleep entre llamadas para respetar el rate limit del free tier.
    Las monedas fallidas se loguean pero no detienen la extracción del resto.

    Args:
        date:   fecha de referencia.
        run_id: identificador del run (para trazabilidad en logs).

    Returns:
        Diccionario con una clave por moneda semántica.
    """
    results: dict[str, Any] = {}

    coins_list = list(COINS.items())
    for i, (coin_id, semantic_name) in enumerate(coins_list):
        try:
            data = _fetch_coin(coin_id=coin_id, date=date)
            results[semantic_name] = data
            logger.debug("Moneda %s (%s) obtenida correctamente", coin_id, semantic_name)
        except RuntimeError as exc:
            logger.warning(
                "Moneda %s falló | run_id=%s | error=%s",
                coin_id,
                run_id,
                exc,
            )
            results[semantic_name] = None

        # sleep entre llamadas excepto tras la última
        if i < len(coins_list) - 1:
            time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)

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
        "coins_requested": list(COINS.keys()),
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
