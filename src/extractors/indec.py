"""Extractor INDEC.

Scrapea indicadores macroeconómicos seleccionados desde la página principal del
Instituto Nacional de Estadística y Censos (INDEC), agrega metadata de ingesta
y sube el JSON crudo a S3 Bronze.

Indicadores extraídos (desde los widgets `.tooltip-resp`):
    Precios al consumidor  → IPC variación mensual (%)
    Actividad              → EMAE variación mensual (%)
    (otros que sigan el mismo patrón HTML se capturan automáticamente)

Método de extracción:
    Web scraping con BeautifulSoup sobre https://www.indec.gob.ar/
    INDEC no expone API pública; el dato se publica en la home como widgets
    con clase `.tooltip-resp` que contienen:
        .font-1 → valor numérico
        .font-2 → nombre del indicador
        .font-3 → período de referencia

Ruta S3:
    s3://{bucket}/bronze/indec/year={YYYY}/month={MM}/indec_{YYYYMM}.json

    La partición es mensual porque los indicadores del INDEC tienen frecuencia
    mensual o trimestral.

Variables de entorno requeridas:
    S3_DATA_LAKE_BUCKET   — nombre del bucket S3
    AWS_ACCESS_KEY_ID     — credencial AWS (o perfil/role configurado)
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

Notas de diseño:
    - No requiere API key; es scraping de la página pública.
    - Se incluye User-Agent para identificar el bot correctamente.
    - El indicador principal de interés es "Precios al consumidor" (IPC).
    - Se capturan todos los indicadores disponibles en los widgets para
      maximizar el valor de cada request (best-effort).
    - El valor crudo se preserva como string (ej: "2,6%") en Bronze;
      la conversión a float se hará en la capa Silver/dbt.

Referencias:
    https://www.indec.gob.ar/
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from bs4 import BeautifulSoup
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

INDEC_URL = "https://www.indec.gob.ar/indec/Portada"
SOURCE_NAME = "indec"
REQUEST_TIMEOUT_SECONDS = 20

_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-AR,es;q=0.9",
    "User-Agent": "macro-intelligence-pipeline/1.0",
    "X-Requested-With": "XMLHttpRequest",
}

# Indicadores de interés: texto parcial en `.font-2` → nombre semántico.
# Se busca con `in` para tolerar diferencias menores de redacción del INDEC.
INDICADORES: dict[str, str] = {
    "Precios al consumidor": "ipc_variacion_mensual",
    "Actividad": "emae_variacion",
    "Producto interno bruto": "pib_variacion",
    "Desocupación": "desocupacion_tasa",
    "Intercambio comercial": "intercambio_comercial",
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

    logger.info("Iniciando extracción INDEC | run_id=%s | date=%s", run_id, date.date())

    raw_html = _fetch_html(run_id=run_id)
    indicadores = _parse_indicadores(raw_html, run_id=run_id)
    enriched = _enrich(indicadores, run_id=run_id, ingested_at=date, raw_html=raw_html)
    s3_key = _build_s3_key(date)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"indec_{date.strftime('%Y%m')}_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(enriched, tmp, ensure_ascii=False, indent=2)
        tmp_path = Path(tmp.name)

    try:
        _upload_to_s3(local_path=tmp_path, s3_key=s3_key)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("Extracción INDEC completada | s3_key=%s", s3_key)
    return s3_key


# ── Funciones internas ────────────────────────────────────────────────────────


def _fetch_html(run_id: str) -> str:
    """GET de la página principal del INDEC.

    Args:
        run_id: identificador del run (para trazabilidad en logs).

    Returns:
        HTML crudo de la página como string.

    Raises:
        RuntimeError: ante timeout, HTTP error o error de red.
    """
    logger.debug("GET %s", INDEC_URL)

    try:
        response = requests.get(
            INDEC_URL,
            headers=_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Timeout al conectar con {INDEC_URL}"
        ) from exc
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] HTTP {exc.response.status_code} en {INDEC_URL}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"[{SOURCE_NAME}] Error de red: {exc}"
        ) from exc

    # Forzar encoding correcto (el INDEC a veces no declara charset en headers)
    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def _parse_indicadores(html: str, run_id: str) -> list[dict[str, Any]]:
    """Parsea los widgets `.tooltip-resp` y extrae indicadores.

    Estructura HTML esperada por widget:
        <div class="tooltip-resp" data-title="...">
            <div class="font-1">2,6%</div>        <!-- valor -->
            <div class="font-2">Precios al consumidor</div>  <!-- nombre -->
            <div class="font-3">Abril 2026</div>  <!-- período -->
        </div>

    Se extraen TODOS los widgets encontrados (best-effort), marcando los que
    coinciden con INDICADORES como `is_tracked: true`.

    Args:
        html:   HTML crudo de la página INDEC.
        run_id: identificador del run (para trazabilidad en logs).

    Returns:
        Lista de diccionarios con los indicadores encontrados.
    """
    soup = BeautifulSoup(html, "html.parser")
    widgets = soup.select(".tooltip-resp")

    if not widgets:
        logger.warning(
            "No se encontraron widgets .tooltip-resp en INDEC | run_id=%s | "
            "Posible cambio en la estructura HTML",
            run_id,
        )
        return []

    resultados: list[dict[str, Any]] = []

    for widget in widgets:
        font_1 = widget.select_one(".font-1")
        font_2 = widget.select_one(".font-2")
        font_3 = widget.select_one(".font-3")

        nombre_raw = font_2.get_text(strip=True) if font_2 else None
        valor_raw = font_1.get_text(strip=True) if font_1 else None
        periodo_raw = font_3.get_text(strip=True) if font_3 else None
        tooltip_title = widget.get("data-title", "")

        # Determinar nombre semántico si es un indicador de interés
        semantic_name: str | None = None
        for patron, nombre_semantico in INDICADORES.items():
            if nombre_raw and patron.lower() in nombre_raw.lower():
                semantic_name = nombre_semantico
                break

        indicador: dict[str, Any] = {
            "nombre_raw": nombre_raw,
            "valor_raw": valor_raw,
            "periodo_raw": periodo_raw,
            "tooltip_title": tooltip_title,
            "semantic_name": semantic_name,
            "is_tracked": semantic_name is not None,
        }
        resultados.append(indicador)

        if semantic_name:
            logger.debug(
                "Indicador encontrado: %s = %s (%s)",
                semantic_name,
                valor_raw,
                periodo_raw,
            )

    tracked_count = sum(1 for r in resultados if r["is_tracked"])
    logger.info(
        "Widgets parseados: %d total, %d tracked | run_id=%s",
        len(resultados),
        tracked_count,
        run_id,
    )

    return resultados


def _get_ipc(indicadores: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Extrae el indicador IPC de la lista parseada (conveniencia).

    Args:
        indicadores: lista de indicadores ya parseados.

    Returns:
        Diccionario del indicador IPC si fue encontrado, None si no.
    """
    for ind in indicadores:
        if ind.get("semantic_name") == "ipc_variacion_mensual":
            return ind
    return None


def _enrich(
    indicadores: list[dict[str, Any]],
    *,
    run_id: str,
    ingested_at: datetime,
    raw_html: str,
) -> dict[str, Any]:
    """Envuelve los indicadores parseados con metadata de ingesta.

    El JSON resultante preserva los datos parseados bajo 'data.indicadores'
    y el HTML crudo bajo 'data.raw_html' para que Bronze sea siempre
    reproducible desde la fuente.

    Args:
        indicadores: lista de indicadores extraídos del HTML.
        run_id:      identificador del pipeline run.
        ingested_at: timestamp de ingesta.
        raw_html:    HTML crudo completo (para reproducibilidad en Bronze).

    Returns:
        Diccionario enriquecido listo para serializar a JSON.
    """
    ipc = _get_ipc(indicadores)

    return {
        "source_name": SOURCE_NAME,
        "pipeline_run_id": run_id,
        "ingested_at": ingested_at.isoformat(),
        "indicadores_tracked": list(INDICADORES.values()),
        "ipc_summary": {
            "valor": ipc["valor_raw"] if ipc else None,
            "periodo": ipc["periodo_raw"] if ipc else None,
            "found": ipc is not None,
        },
        "data": {
            "indicadores": indicadores,
            "raw_html": raw_html,
        },
    }


def _build_s3_key(date: datetime) -> str:
    """Construye la clave S3 con particionado Hive-style (mensual).

    INDEC publica indicadores con frecuencia mensual/trimestral;
    se particiona por año/mes.
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

    # Modo local: solo parsea y muestra, sin subir a S3
    print("=" * 60)
    print("INDEC Extractor — Modo local (solo scraping, sin S3)")
    print("=" * 60)

    html = _fetch_html(run_id="local-test")
    indicadores = _parse_indicadores(html, run_id="local-test")

    if not indicadores:
        print("\n⚠ No se encontraron indicadores. Posible cambio en el HTML del INDEC.")
        sys.exit(1)

    print(f"\nIndicadores encontrados: {len(indicadores)}")
    print("-" * 60)

    for ind in indicadores:
        marker = "✓" if ind["is_tracked"] else " "
        print(
            f"  [{marker}] {ind['nombre_raw']:<30s} "
            f"| {ind['valor_raw']:<10s} "
            f"| {ind['periodo_raw']}"
        )

    ipc = _get_ipc(indicadores)
    if ipc:
        print(f"\n📊 IPC (Precios al consumidor): {ipc['valor_raw']} — {ipc['periodo_raw']}")
    else:
        print("\n⚠ IPC no encontrado en los widgets.")
