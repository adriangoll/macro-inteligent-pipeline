"""Extractor Bluelytics.

Descarga cotizaciones dólar (oficial, MEP, blue) desde la API pública de
Bluelytics, agrega metadata de ingesta y sube el JSON crudo a S3 Bronze.

Ruta S3:
    s3://{bucket}/bronze/bluelytics/year={YYYY}/month={MM}/day={DD}/bluelytics_{YYYYMMDD}.json

Variables de entorno requeridas:
    S3_DATA_LAKE_BUCKET  — nombre del bucket S3
    AWS_ACCESS_KEY_ID    — credencial AWS (o perfil/role configurado)
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

BLUELYTICS_URL = "https://api.bluelytics.com.ar/v2/latest"
SOURCE_NAME = "bluelytics"
REQUEST_TIMEOUT_SECONDS = 15


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

    logger.info("Iniciando extracción Bluelytics | run_id=%s | date=%s", run_id, date.date())

    raw_payload = _fetch(run_id=run_id)
    enriched = _enrich(raw_payload, run_id=run_id, ingested_at=date)
    s3_key = _build_s3_key(date)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"bluelytics_{date.strftime('%Y%m%d')}_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(enriched, tmp, ensure_ascii=False, indent=2)
        tmp_path = Path(tmp.name)

    try:
        _upload_to_s3(local_path=tmp_path, s3_key=s3_key)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("Extracción completada | s3_key=%s", s3_key)
    return s3_key


# ── Funciones internas ────────────────────────────────────────────────────────


def _fetch(run_id: str) -> dict:
    """GET a la API Bluelytics. Lanza RuntimeError en caso de fallo."""
    logger.debug("GET %s", BLUELYTICS_URL)
    try:
        response = requests.get(BLUELYTICS_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Timeout al conectar con {BLUELYTICS_URL}"
        ) from exc
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] HTTP {exc.response.status_code} en {BLUELYTICS_URL}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error de red: {exc}"
        ) from exc

    return response.json()


def _enrich(payload: dict, *, run_id: str, ingested_at: datetime) -> dict:
    """Envuelve el payload crudo con metadata de ingesta.

    El JSON resultante preserva el response original bajo la clave 'data'
    para que Bronze sea siempre reproducible desde la fuente.
    """
    return {
        "source_name": SOURCE_NAME,
        "pipeline_run_id": run_id,
        "ingested_at": ingested_at.isoformat(),
        "data": payload,
    }


def _build_s3_key(date: datetime) -> str:
    """Construye la clave S3 con particionado Hive-style."""
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
        raise RuntimeError(
            "Variable de entorno S3_DATA_LAKE_BUCKET no definida."
        )

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