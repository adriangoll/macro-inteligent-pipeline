# Macro Intelligence Pipeline

Pipeline de datos end-to-end que combina indicadores macroeconómicos argentinos
(dólar, inflación, tasas BCRA) con señales globales (BTC, macro USA) para generar
inteligencia automatizada vía LLM y visualizarla en un dashboard Streamlit.

> **Estado:** estructura inicial del repositorio (scaffolding). **No** incluye
> implementaciones de ETLs, DAGs ni lógica de negocio: todos los archivos son
> placeholders mínimos para completar más adelante. Ver el diseño completo en
> [`Architecture.md`](./Architecture.md) y las convenciones en [`agents.md`](./agents.md).

## Arquitectura (resumen)

```
APIs externas / scraping → [Airflow DAG] → S3 Bronze → S3 Silver → S3 Gold
                                                              │
                                                              ▼
                                                         AWS Athena
                                                              │
                                                              ▼
                                                   LLM Intelligence Layer
                                                              │
                                                              ▼
                                                    Streamlit Dashboard
```

## Estructura del repositorio

```text
.
├── dags/                  # Airflow DAGs (placeholders, sin lógica)
├── dbt/                   # Modelos dbt (staging / intermediate / gold) + tests
├── src/                   # Lógica Python reutilizable
│   ├── extractors/        # Conectores a APIs externas
│   ├── loaders/           # Escritura a S3 / Postgres
│   └── intelligence/      # Capa LLM
├── dashboard/             # App Streamlit (4 pantallas)
├── infra/                 # Terraform (módulos s3 / glue / athena / iam)
├── tests/                 # Pruebas (pytest)
├── scripts/               # Scripts auxiliares (p. ej. backfill)
├── docker-compose.yml     # Stack local (Postgres + Airflow + Streamlit)
├── requirements.txt       # Dependencias de runtime
├── requirements-dev.txt   # Dependencias de desarrollo
├── pyproject.toml         # Metadatos y config de herramientas
└── .env.example           # Variables de entorno de ejemplo
```

## Stack

- **Orquestación:** Apache Airflow (DAGs en `dags/`)
- **Transformaciones:** pandas (ETL) / dbt (modelos en `dbt/models/`)
- **Data lake:** AWS S3 — arquitectura Medallion (bronze / silver / gold)
- **Query engine:** AWS Athena + AWS Glue Data Catalog
- **Base de datos:** PostgreSQL (metadata de Airflow)
- **Infra como código:** Terraform (en `infra/`)
- **Contenedores:** Docker / docker-compose
- **Dashboard:** Streamlit (en `dashboard/`)
- **LLM layer:** capa de inteligencia sobre datos procesados (`src/intelligence/`)
- **Lenguaje:** Python 3.11+

## Primeros pasos

> Aún no hay lógica implementada. El flujo previsto una vez completados los
> placeholders es:

1. Copiar variables de entorno:
   ```bash
   cp .env.example .env
   ```
2. Crear entorno virtual e instalar dependencias:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt -r requirements-dev.txt
   ```
3. Levantar el stack local:
   ```bash
   docker compose up -d
   ```
   - Airflow UI: http://localhost:8080
   - Streamlit:  http://localhost:8501

## Estado

Scaffolding inicial. Pendiente: implementación de extractores, DAGs, modelos,
métricas Gold, capa LLM, dashboards e infraestructura Terraform.
