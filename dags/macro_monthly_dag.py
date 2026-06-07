"""DAG secundario `macro_monthly_dag` (placeholder, sin implementacion).

Mensual (`0 8 1 * *`). Complementario para datos mensuales (ver Architecture.md):

    extract_indec ─► load_bronze_indec ─► build_silver_indec ─► trigger_gold_rebuild

La definicion del DAG y la logica de las tasks se implementaran mas adelante.
"""

# TODO: definir el DAG mensual con TaskFlow API.
