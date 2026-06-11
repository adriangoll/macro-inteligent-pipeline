"""Tests unitarios para el extractor de INDEC.

Valida el scraping, parsing de widgets HTML, enriquecimiento de metadata
y la subida a S3 Bronze usando mocks para peticiones HTTP y cliente de AWS.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import BotoCoreError

from src.extractors.indec import (
    _build_s3_key,
    _enrich,
    _fetch_html,
    _get_ipc,
    _parse_indicadores,
    extract,
)

# ── Datos de Prueba (Fixtures) ────────────────────────────────────────────────

HTML_VALIDO_PORTADA = """
<div id="content">
    <div class="tooltip-resp" data-title="Variación mensual de precios al consumidor">
        <div class="font-1">2,6%</div>
        <div class="font-2">Precios al consumidor</div>
        <div class="font-3">Abril 2026</div>
    </div>
    <div class="tooltip-resp" data-title="Variación mensual de la actividad económica">
        <div class="font-1">3,5%</div>
        <div class="font-2">Estimador mensual de actividad económica</div>
        <div class="font-3">Marzo 2026</div>
    </div>
    <div class="tooltip-resp" data-title="Otros datos no seguidos">
        <div class="font-1">99,9%</div>
        <div class="font-2">Indicador No Trackeado</div>
        <div class="font-3">Enero 2026</div>
    </div>
</div>
"""

HTML_SIN_WIDGETS = """
<div id="content">
    <p>No hay indicadores disponibles en este momento.</p>
</div>
"""


@pytest.fixture
def mock_env_s3_bucket() -> Generator[str, None, None]:
    """Configura temporalmente la variable de entorno de S3."""
    old_value = os.environ.get("S3_DATA_LAKE_BUCKET")
    os.environ["S3_DATA_LAKE_BUCKET"] = "mi-test-bucket-s3"
    yield "mi-test-bucket-s3"
    if old_value is not None:
        os.environ["S3_DATA_LAKE_BUCKET"] = old_value
    else:
        os.environ.pop("S3_DATA_LAKE_BUCKET", None)


# ── Tests de Parsing y Scraping ───────────────────────────────────────────────


@patch("requests.get")
def test_fetch_html_success(mock_get: MagicMock) -> None:
    """Valida la correcta obtención del HTML llamando al endpoint Portada."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = HTML_VALIDO_PORTADA
    mock_response.apparent_encoding = "utf-8"
    mock_get.return_value = mock_response

    html = _fetch_html(run_id="test-run")

    assert html == HTML_VALIDO_PORTADA
    mock_get.assert_called_once_with(
        "https://www.indec.gob.ar/indec/Portada",
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "es-AR,es;q=0.9",
            "User-Agent": "macro-intelligence-pipeline/1.0",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=20,
    )


@patch("requests.get")
def test_fetch_html_timeout(mock_get: MagicMock) -> None:
    """Valida el manejo de excepciones por timeout en la petición HTTP."""
    import requests

    mock_get.side_effect = requests.exceptions.Timeout("Timeout error")

    with pytest.raises(RuntimeError, match="Timeout al conectar con"):
        _fetch_html(run_id="test-run")


def test_parse_indicadores_validos() -> None:
    """Verifica que se parseen correctamente los widgets HTML esperados."""
    indicadores = _parse_indicadores(HTML_VALIDO_PORTADA, run_id="test-run")

    assert len(indicadores) == 3

    # Validar IPC (precios al consumidor)
    ipc = next(i for i in indicadores if i["semantic_name"] == "ipc_variacion_mensual")
    assert ipc["nombre_raw"] == "Precios al consumidor"
    assert ipc["valor_raw"] == "2,6%"
    assert ipc["periodo_raw"] == "Abril 2026"
    assert ipc["is_tracked"] is True

    # Validar EMAE (actividad económica)
    emae = next(i for i in indicadores if i["semantic_name"] == "emae_variacion")
    assert "actividad económica" in emae["nombre_raw"].lower()
    assert emae["valor_raw"] == "3,5%"
    assert emae["is_tracked"] is True

    # Validar indicador no seguido
    no_tracked = next(i for i in indicadores if i["semantic_name"] is None)
    assert no_tracked["nombre_raw"] == "Indicador No Trackeado"
    assert no_tracked["is_tracked"] is False


def test_parse_indicadores_sin_widgets() -> None:
    """Verifica el comportamiento cuando no hay widgets .tooltip-resp."""
    indicadores = _parse_indicadores(HTML_SIN_WIDGETS, run_id="test-run")
    assert indicadores == []


def test_get_ipc_helper() -> None:
    """Prueba la función de conveniencia para encontrar el indicador de IPC."""
    indicadores = _parse_indicadores(HTML_VALIDO_PORTADA, run_id="test-run")
    ipc = _get_ipc(indicadores)

    assert ipc is not None
    assert ipc["semantic_name"] == "ipc_variacion_mensual"
    assert ipc["valor_raw"] == "2,6%"

    # Si pasamos una lista vacía o sin IPC
    assert _get_ipc([]) is None


# ── Tests de Estructura y Metadata ───────────────────────────────────────────


def test_build_s3_key() -> None:
    """Comprueba el particionado e indexado Hive-style."""
    date = datetime(2026, 4, 15, tzinfo=timezone.utc)
    s3_key = _build_s3_key(date)
    assert s3_key == "bronze/indec/year=2026/month=04/indec_202604.json"


def test_enrich_payload() -> None:
    """Valida la envoltura de los datos con la metadata correcta."""
    date = datetime(2026, 6, 8, 12, 0, 0, tzinfo=timezone.utc)
    indicadores = _parse_indicadores(HTML_VALIDO_PORTADA, run_id="test-run")

    enriched = _enrich(
        indicadores,
        run_id="test-run-123",
        ingested_at=date,
        raw_html=HTML_VALIDO_PORTADA,
    )

    assert enriched["source_name"] == "indec"
    assert enriched["pipeline_run_id"] == "test-run-123"
    assert enriched["ingested_at"] == date.isoformat()
    assert enriched["ipc_summary"]["valor"] == "2,6%"
    assert enriched["ipc_summary"]["periodo"] == "Abril 2026"
    assert enriched["ipc_summary"]["found"] is True
    assert enriched["data"]["indicadores"] == indicadores
    assert enriched["data"]["raw_html"] == HTML_VALIDO_PORTADA


# ── Tests del Flujo Principal (extract) ───────────────────────────────────────


@patch("src.extractors.indec._upload_to_s3")
@patch("src.extractors.indec._fetch_html")
def test_extract_flow_success(
    mock_fetch: MagicMock,
    mock_upload: MagicMock,
    mock_env_s3_bucket: str,
) -> None:
    """Verifica que el flujo completo de extract funcione exitosamente."""
    mock_fetch.return_value = HTML_VALIDO_PORTADA
    date = datetime(2026, 4, 15, tzinfo=timezone.utc)

    s3_key = extract(run_id="test-run-id", date=date)

    expected_s3_key = "bronze/indec/year=2026/month=04/indec_202604.json"
    assert s3_key == expected_s3_key

    # Validar que _upload_to_s3 haya sido llamado con el key correcto
    mock_upload.assert_called_once()
    args, kwargs = mock_upload.call_args
    assert kwargs["s3_key"] == expected_s3_key

    # Validar que el archivo temporal se haya creado y contenga los datos válidos
    local_path = kwargs["local_path"]
    assert local_path.exists() is False  # Debería haber sido eliminado por la cláusula 'finally'


@patch("boto3.client")
def test_upload_to_s3_failure(
    mock_boto_client: MagicMock,
    mock_env_s3_bucket: str,
) -> None:
    """Valida el manejo de fallos al interactuar con el cliente de S3."""
    from src.extractors.indec import _upload_to_s3

    mock_s3 = MagicMock()
    mock_s3.upload_file.side_effect = BotoCoreError()
    mock_boto_client.return_value = mock_s3

    with pytest.raises(RuntimeError, match="Error al subir a s3://"):
        _upload_to_s3(local_path=Path("dummy_file.json"), s3_key="dummy_key.json")


def test_upload_to_s3_missing_bucket() -> None:
    """Valida que falle si la variable de entorno del bucket no está definida."""
    from src.extractors.indec import _upload_to_s3

    # Forzar remoción del bucket de las variables de entorno
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="Variable de entorno S3_DATA_LAKE_BUCKET no definida"):
            _upload_to_s3(local_path=Path("dummy_file.json"), s3_key="dummy_key.json")
