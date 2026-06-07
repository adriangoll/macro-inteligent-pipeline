"""DAG principal `macro_intelligence_dag` (placeholder, sin implementacion).

Diario (`0 6 * * *`). Flujo previsto (ver Architecture.md, seccion 5):

    extract_bluelytics ─┐
    extract_bcra        ─┼─► validate_sources ─► load_bronze_all
    extract_coingecko   ─┤        ─► build_silver ─► build_gold
    extract_fred        ─┘        ─► llm_insight_gen ─► notify_success

La definicion del DAG y la logica de las tasks se implementaran mas adelante.
"""

# TODO: definir el DAG con TaskFlow API (retries=2, retry_delay, catchup=False).
