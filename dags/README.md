# Airflow DAGs

DAGs de orquestación (placeholders, sin lógica de negocio).

- `macro_intelligence_dag.py` — DAG diario principal.
- `macro_monthly_dag.py` — DAG mensual (datos INDEC/FRED).

Convenciones (ver `agents.md`):
- Un DAG por fuente / flujo, TaskFlow API (`@task`).
- `retries=2` y `retry_delay` en `default_args`.
- La lógica de negocio se importa desde `src/`, no se define en el DAG.
