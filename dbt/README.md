# dbt

Modelos de transformación (placeholders, sin SQL implementado).

- `models/staging/` — modelos `stg_*`.
- `models/intermediate/` — modelos `int_*`.
- `models/gold/` — modelos `fct_*` / `dim_*`.
- `tests/` — tests de dbt.

Convenciones (ver `agents.md`): manejar duplicados explícitamente, tests de
unicidad y `not_null` en `schema.yml` para todas las PKs.

> Scaffolding inicial. `dbt_project.yml` y `profiles.yml` se agregarán al implementar.
