# Macro Intelligence Pipeline — Cascade Agent Instructions

## Descripción del proyecto
Pipeline de datos end-to-end que combina indicadores macroeconómicos argentinos
(dólar, inflación, tasas BCRA) con señales globales (BTC, macro USA) para generar
inteligencia automatizada vía LLM y visualizarla en un dashboard Streamlit.

## Stack
- **Orquestación**: Apache Airflow (DAGs en /dags)
- **Transformaciones**: dbt (modelos en /dbt/models)
- **Data lake**: AWS S3 — arquitectura Medallion (bronze / silver / gold)
- **Query engine**: AWS Athena
- **Catalogado / ETL**: AWS Glue
- **Base de datos**: PostgreSQL
- **Infra como código**: Terraform (en /infra)
- **Contenedores**: Docker / docker-compose
- **Dashboard**: Streamlit (en /dashboard)
- **LLM layer**: capa de inteligencia sobre datos procesados (en /intelligence)
- **Lenguaje principal**: Python 3.11+

## Arquitectura de capas
```
Bronze  → datos crudos ingestados sin transformar (S3)
Silver  → datos limpios, tipados, sin duplicados (S3 + Athena)
Gold    → métricas de negocio, agregaciones, listos para consumo (S3 + Athena)
```

## Convenciones de código

### Python
- Type hints obligatorios en todas las funciones
- Docstrings en formato Google style
- Manejo explícito de errores con logging, nunca `except: pass`
- Variables de entorno vía `python-dotenv`, nunca hardcodear credenciales
- Tests en /tests usando pytest

### SQL / dbt
- Siempre manejar duplicados explícitamente (ROW_NUMBER, QUALIFY, etc.)
- Incluir tiebreakers en ORDER BY cuando corresponda
- Nombrado de modelos: `stg_` (staging), `int_` (intermediate), `fct_` (fact), `dim_` (dimension)
- Agregar tests de unicidad y not_null en schema.yml para todas las PKs

### Airflow DAGs
- Un DAG por fuente de datos
- Usar TaskFlow API (@task decorator) sobre operadores legacy
- Incluir `retries=2` y `retry_delay` en default_args
- Separar lógica de negocio de la definición del DAG (importar funciones desde /src)

### Terraform
- Un módulo por recurso AWS (s3, glue, athena, iam)
- Variables en variables.tf, outputs en outputs.tf
- Estado remoto en S3

## Fuentes de datos
- BCRA API — tipo de cambio oficial, tasas
- Ambito / Rava — dólar blue, MEP, CCL
- INDEC — inflación, IPC
- CoinGecko / Binance API — BTC/USD
- FRED (St. Louis Fed) — indicadores macro USA (DXY, Fed Funds Rate, CPI)

## Reglas para Cascade

1. **Antes de crear cualquier archivo**, revisar si ya existe algo similar en el proyecto para mantener consistencia de estilo.
2. **Al agregar una nueva fuente de datos**, crear el DAG de ingesta, el modelo dbt de staging y el test correspondiente.
3. **Nunca modificar datos en Bronze** — esa capa es append-only e inmutable.
4. **Al generar código Python**, incluir siempre el bloque `if __name__ == "__main__"` para ejecución local.
5. **Al tocar infraestructura Terraform**, no aplicar cambios directamente — generar el plan primero y esperar confirmación.
6. **Responder siempre en español.**
7. **Ante una tarea ambigua**, preguntar antes de escribir código. Una pregunta concreta es mejor que código que hay que tirar.

## Estructura de directorios esperada
```
/
├── dags/                  # Airflow DAGs
├── dbt/
│   ├── models/
│   │   ├── staging/       # stg_*
│   │   ├── intermediate/  # int_*
│   │   └── gold/          # fct_* dim_*
│   └── tests/
├── src/                   # lógica Python reutilizable
│   ├── extractors/        # connectors a APIs externas
│   ├── loaders/           # escritura a S3 / Postgres
│   └── intelligence/      # LLM layer
├── dashboard/             # Streamlit app
├── infra/                 # Terraform modules
├── tests/                 # pytest
├── docker-compose.yml
└── .env.example
```