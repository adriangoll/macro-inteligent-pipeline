# Macro Intelligence Pipeline
## Diseño Técnico — Arquitectura End-to-End

> **Estado:** Borrador v1.0 — Pendiente de revisión y ajustes  
> **Alcance:** Solo lectura y planificación. No contiene código.  
> **Autor:** Adrian  

---

## Tabla de contenidos

1. [Arquitectura general](#1-arquitectura-general)
2. [Data sources](#2-data-sources)
3. [Data Lake Design (S3)](#3-data-lake-design-s3)
4. [Data Processing Layer](#4-data-processing-layer)
5. [Airflow Design](#5-airflow-design)
6. [Query Layer (Athena)](#6-query-layer-athena)
7. [Analytics & Metrics (Gold layer)](#7-analytics--metrics-gold-layer)
8. [AI Intelligence Layer (LLM)](#8-ai-intelligence-layer-llm)
9. [Dashboard (Streamlit)](#9-dashboard-streamlit)
10. [IaC — Terraform](#10-iac--terraform)
11. [Deployment (Docker + AWS)](#11-deployment-docker--aws)
12. [Constraints & Trade-offs](#12-constraints--trade-offs)

---

## 1. Arquitectura general

Pipeline unidireccional de datos con capa de inteligencia LLM intermedia.

```
APIs externas / scraping
        │
        ▼
  [Airflow DAG]  ─── orquesta todo el flujo
        │
        ▼
  S3 Bronze  ──►  S3 Silver  ──►  S3 Gold
  (raw JSON)     (Parquet,         (métricas,
                  tipado,           KPIs,
                  limpio)           rolling avg)
                                      │
                                      ▼
                               AWS Athena
                           (tablas externas)
                                      │
                                      ▼
                            LLM Intelligence Layer
                         (narratives / insights / NL query)
                                      │
                                      ▼
                           Streamlit Dashboard
```

**Componentes de infraestructura:**

| Componente | Rol |
|---|---|
| AWS S3 | Data lake (Bronze / Silver / Gold) |
| AWS Athena | SQL ad-hoc sobre Gold layer |
| AWS Glue Data Catalog | Schema registry para Athena |
| Apache Airflow | Orquestación de DAGs |
| Python + pandas | ETL y procesamiento |
| LLM API externa | Generación de insights y narrativas |
| Streamlit | Dashboard final |
| Docker | Entorno local reproducible |
| Terraform | Infraestructura como código (AWS) |

**Decisiones de diseño principales:**

- pandas sobre Spark: volumen diario total < 10 MB. Spark introduce overhead de cluster sin beneficio real a esta escala.
- Sin Kafka, sin streaming: todos los datasets tienen frecuencia diaria o mensual. Batch es suficiente.
- Sin ML clásico: la capa de inteligencia usa exclusivamente LLM vía API externa.
- Sin vector databases en esta fase: los datos estructurados se pasan directamente al LLM como contexto serializado.

---

## 2. Data sources

Máximo 5 fuentes. 3 Argentina, 2 globales.

### Argentina

| # | Fuente | Dataset | Frecuencia | Formato | Método de acceso |
|---|---|---|---|---|---|
| 1 | Bluelytics API (`bluelytics.com.ar/api/v2`) | Dólar oficial, MEP, blue (bid/ask/promedio) | Diaria | JSON REST | GET sin autenticación |
| 2 | INDEC sitio web (`indec.gob.ar`) | CPI mensual, variación % mensual y anual | Mensual | HTML table / Excel embebido | scraping con requests + BeautifulSoup |
| 3 | BCRA API (`api.bcra.gob.ar`) | BADLAR, Leliq 28d, reservas, tipo de cambio oficial detallado | Diaria | JSON REST | GET con header `Accept: application/json` |

### Global

| # | Fuente | Dataset | Frecuencia | Formato | Método de acceso |
|---|---|---|---|---|---|
| 4 | CoinGecko API v3 (free tier) | BTC/USD, ETH/USD precio de cierre | Diaria | JSON REST | GET sin auth (rate limit: 30 rpm) |
| 5 | FRED API (St. Louis Fed) | DXY (USD Index) o CPI USA (`CPIAUCSL`) | Mensual (CPI) / Semanal (DXY) | JSON REST | GET con API key gratuita |

### Notas de acceso

- **Bluelytics:** no requiere API key. Endpoint único `/api/v2/latest`. Estable históricamente pero sin SLA oficial.
- **INDEC:** el scraping es el punto más frágil del sistema. Se debe implementar validación de schema en la task de extracción. Si el HTML cambia, el extractor falla ruidosamente antes de escribir en Bronze.
- **BCRA API:** requiere header `Accept: application/json`. Las variables de interés se identifican por `idVariable` numérico (documentado en el portal BCRA).
- **CoinGecko:** free tier tiene límite de 30 requests por minuto. El extractor debe incluir `time.sleep` entre llamadas y retry con backoff exponencial.
- **FRED:** API key gratuita, límite 120 requests/minuto. Sin riesgo práctico para este volumen.

---

## 3. Data Lake Design (S3)

### Naming convention

```
Bucket: s3://macro-intelligence-{env}/
```

`{env}` = `dev` | `prod`. Un solo bucket con tres prefijos de capa. No se crean buckets separados por capa.

### Estructura completa

```
s3://macro-intelligence-prod/
│
├── bronze/
│   ├── bluelytics/
│   │   └── year=2025/month=01/day=15/
│   │       └── bluelytics_20250115.json
│   ├── indec_cpi/
│   │   └── year=2025/month=01/
│   │       └── indec_cpi_202501.json
│   ├── bcra/
│   │   └── year=2025/month=01/day=15/
│   │       └── bcra_20250115.json
│   ├── coingecko/
│   │   └── year=2025/month=01/day=15/
│   │       └── coingecko_20250115.json
│   └── fred/
│       └── year=2025/month=01/
│           └── fred_202501.json
│
├── silver/
│   ├── dolar_types/
│   │   └── year=2025/month=01/day=15/
│   │       └── dolar_20250115.parquet
│   ├── cpi_arg/
│   │   └── year=2025/month=01/
│   │       └── cpi_arg_202501.parquet
│   ├── bcra_rates/
│   │   └── year=2025/month=01/day=15/
│   │       └── bcra_20250115.parquet
│   ├── crypto_prices/
│   │   └── year=2025/month=01/day=15/
│   │       └── crypto_20250115.parquet
│   └── fred_indicators/
│       └── year=2025/month=01/
│           └── fred_202501.parquet
│
└── gold/
    ├── macro_ar_daily/
    │   └── year=2025/month=01/
    │       └── macro_ar_20250115.parquet
    ├── global_daily/
    │   └── year=2025/month=01/
    │       └── global_20250115.parquet
    ├── combined_metrics/
    │   └── year=2025/month=01/
    │       └── combined_20250115.parquet
    └── llm_insights/
        └── year=2025/month=01/week=03/
            └── insights_20250115.json
```

### Reglas de particionado

| Capa | Datos diarios | Datos mensuales | Formato |
|---|---|---|---|
| Bronze | `year=/month=/day=` | `year=/month=` | JSON crudo (inmutable) |
| Silver | `year=/month=/day=` | `year=/month=` | Parquet + Snappy |
| Gold | `year=/month=` | `year=/month=` | Parquet + Snappy |
| Gold (LLM) | `year=/month=/week=` | — | JSON |

**Reglas adicionales:**

- Bronze es append-only. Nunca se sobreescribe ni modifica un archivo existente.
- Silver y Gold son idempotentes: una re-ejecución del DAG del mismo día sobreescribe el Parquet existente sin consecuencias.
- Compresión Snappy (no Gzip) en todos los Parquet: mejor trade-off entre tamaño y velocidad de descompresión para Athena.
- Partition projection en Athena elimina la necesidad de `MSCK REPAIR TABLE` manual.

---

## 4. Data Processing Layer

### Herramienta de procesamiento

**pandas exclusivamente.** Justificación: el volumen diario total de todos los datasets combinados es inferior a 10 MB. PySpark requiere un contexto SparkSession, JVM, y overhead de serialización que no aportan nada a esta escala. La regla es: si los datos entran en RAM sin paginar, se usa pandas.

### Bronze (ingesta cruda)

- Descarga del response HTTP completo sin modificaciones.
- Campos de metadata agregados al wrapper del archivo: `ingested_at` (ISO 8601 UTC), `source_name`, `pipeline_run_id`.
- Si la descarga falla (timeout, HTTP 4xx/5xx): el archivo no se crea en S3. Airflow detecta la ausencia y reintenta según la política de la task.

### Bronze → Silver (normalización)

Operaciones por dataset:

**Bluelytics:**
- Aplanar JSON anidado: extraer `official.value_buy`, `official.value_sell`, `blue.value_buy`, `blue.value_sell`, `mep.value_buy`, `mep.value_sell`.
- Cast de todos los valores a `float64`.
- Columna `date` → `datetime64[ns, UTC]`.

**INDEC:**
- Parsear tabla HTML o Excel embebido.
- Extraer variación mensual % y variación interanual %.
- Normalizar nombre de mes a fecha del último día del mes (ej: "Enero 2025" → `2025-01-31`).

**BCRA:**
- Aplanar array de variables.
- Pivotar por `idVariable` a columnas nombradas.
- Retener únicamente: BADLAR privada, Leliq 28d, tipo de cambio de referencia, reservas internacionales.

**CoinGecko:**
- Extraer precio USD de cierre de cada moneda del response.
- Renombrar a columnas estandarizadas: `btc_usd`, `eth_usd`.

**FRED:**
- Extraer `date` y `value` para cada serie solicitada.
- Renombrar a `dxy_index` o `cpi_usa` según la serie.

**Reglas de limpieza comunes (todas las fuentes):**

- Eliminar duplicados por `(source, date)`. Si hay duplicados, retener el más reciente.
- Valores nulos: se dejan como `NaN` en Silver. No se interpola en esta capa.
- Validaciones de rango básicas (no bloqueantes): dólar blue > dólar oficial, inflación mensual > -5% y < 50%, BTC > 0. Las anomalías se loguean sin bloquear el pipeline.
- Todas las fechas en UTC.

### Silver → Gold (métricas derivadas)

- Join de todas las tablas Silver sobre la columna `date` (outer join para preservar distintas frecuencias de actualización).
- Datos mensuales (INDEC, FRED): forward-fill hasta el siguiente dato disponible. Columna `is_interpolated: bool` marca las filas rellenadas.
- Cálculo de métricas derivadas: ver sección 7.
- Output: tabla `combined_metrics` (wide por fecha) más tablas temáticas separadas `macro_ar_daily` y `global_daily`.

---

## 5. Airflow Design

### DAG principal: `macro_intelligence_dag`

| Parámetro | Valor |
|---|---|
| Schedule | `0 6 * * *` (06:00 UTC = 03:00 ART) |
| Rationale | APIs argentinas actualizan tipo de cambio a partir de las 10 ART; se corre con datos del día anterior consolidados |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `retries` | `2` por task |
| `retry_delay` | `5 minutos` |

### Task graph — DAG principal

```
extract_bluelytics ──┐
extract_bcra        ──┼──► validate_sources ──► load_bronze_all ──► build_silver ──► build_gold ──► llm_insight_gen ──► notify_success
extract_coingecko   ──┤
extract_fred        ──┘
```

### Tasks detalladas

| Task | Operador | Descripción |
|---|---|---|
| `extract_bluelytics` | PythonOperator | GET Bluelytics API; guarda JSON en `/tmp/`; push path vía XCom |
| `extract_bcra` | PythonOperator | GET BCRA API variables seleccionadas; guarda JSON en `/tmp/`; push XCom |
| `extract_coingecko` | PythonOperator | GET CoinGecko precios BTC+ETH; sleep entre llamadas; push XCom |
| `extract_fred` | PythonOperator | GET FRED series DXY + CPI USA; push XCom |
| `validate_sources` | PythonOperator | Verifica que todos los extracts tienen archivos en `/tmp/`; si alguno falla, marca esa fuente como `skipped` pero no bloquea el pipeline completo |
| `load_bronze_all` | PythonOperator | Lee paths de XCom; sube archivos a S3 Bronze con path particionado por fecha |
| `build_silver` | PythonOperator | Lee desde S3 Bronze; transforma con pandas; escribe S3 Silver (Parquet + Snappy) |
| `build_gold` | PythonOperator | Lee Silver; calcula métricas derivadas; escribe Gold (Parquet) y tablas temáticas |
| `llm_insight_gen` | PythonOperator | Lee snapshot Gold del día; serializa métricas; llama LLM API; escribe JSON de insights en `gold/llm_insights/` |
| `notify_success` | PythonOperator | Log estructurado de éxito con timestamp y run_id; opcional: email/Slack |

### DAG secundario: `macro_monthly_dag`

| Parámetro | Valor |
|---|---|
| Schedule | `0 8 1 * *` (día 1 de cada mes, 08:00 UTC) |
| Trigger | Complementario al DAG principal para datos mensuales |

```
extract_indec ──► load_bronze_indec ──► build_silver_indec ──► trigger_gold_rebuild
```

### Manejo de fallos

- `retries=2`, `retry_delay=timedelta(minutes=5)` en todas las tasks.
- `on_failure_callback`: log estructurado con campos `source`, `execution_date`, `error_type`, `error_message`.
- Si `build_gold` falla: el último Gold válido permanece en S3; Streamlit muestra los datos del día anterior con timestamp visible.
- Si `llm_insight_gen` falla: no bloquea el pipeline. El DAG se marca como éxito parcial. Streamlit muestra el panel de insights con timestamp del último éxito.
- Si `validate_sources` detecta que una fuente crítica (ej: Bluelytics) está ausente: el DAG continúa con las fuentes disponibles y loguea la ausencia.

---

## 6. Query Layer (Athena)

### Configuración base

- **Metastore:** AWS Glue Data Catalog. Una sola database: `macro_intelligence`.
- **Tablas:** externas, apuntan a prefijos S3 Gold en formato Parquet.
- **Partition projection:** habilitada en todas las tablas para evitar `MSCK REPAIR TABLE` manual.
- **Query results:** S3 bucket separado `s3://macro-intelligence-athena-results/`.
- **Workgroup:** workgroup dedicado con límite de datos escaneados por query (protección de costos).

### Tablas externas

| Tabla Athena | Fuente S3 Gold | Granularidad | Partition columns |
|---|---|---|---|
| `macro_ar_daily` | `gold/macro_ar_daily/` | Diaria | `year`, `month` |
| `global_daily` | `gold/global_daily/` | Diaria | `year`, `month` |
| `combined_metrics` | `gold/combined_metrics/` | Diaria (con forward-fill) | `year`, `month` |
| `llm_insights` | `gold/llm_insights/` | Semanal/mensual | `year`, `month`, `week` |

### Métricas consultables por Athena

- Evolución del spread dólar blue vs oficial por período arbitrario.
- Variación porcentual acumulada del BTC expresada en pesos al tipo de cambio blue.
- Correlación rolling entre inflación mensual AR y variación del tipo de cambio.
- CPI USA vs CPI Argentina en la misma escala temporal (índice relativo).
- Rolling average 30 días del dólar MEP.
- Brecha cambiaria histórica (oficial vs blue, expresada en %).
- DXY vs brecha cambiaria Argentina.
- Tasa Leliq real (Leliq mensualizada menos CPI mensual).
- Serie temporal completa de `btc_ars_blue` (BTC en pesos al tipo blue).

### Integración con Streamlit

Las queries se ejecutan vía `PyAthena` (wrapper de `boto3` para Athena). Los resultados se cargan directamente en pandas DataFrames para graficado. Las queries parametrizadas usan string formatting controlado por el servidor, no interpolación directa de input de usuario.

---

## 7. Analytics & Metrics (Gold layer)

Todas las métricas se calculan en la task `build_gold` del DAG principal.

### Variaciones porcentuales

| Métrica | Descripción |
|---|---|
| `dolar_blue_pct_1d` | Variación diaria dólar blue |
| `dolar_blue_pct_7d` | Variación últimos 7 días |
| `dolar_blue_pct_30d` | Variación últimos 30 días |
| `btc_usd_pct_7d` | Variación BTC/USD últimos 7 días |
| `btc_usd_pct_30d` | Variación BTC/USD últimos 30 días |
| `cpi_arg_mom` | Variación mensual inflación (from INDEC) |
| `cpi_arg_yoy` | Acumulado 12 meses calculado en Gold |
| `brecha_cambiaria_pct` | `(blue - oficial) / oficial * 100` |

### Rolling averages

| Métrica | Descripción |
|---|---|
| `dolar_blue_ma7` | Media móvil simple 7 días |
| `dolar_blue_ma30` | Media móvil simple 30 días |
| `dolar_mep_ma7` | Media móvil MEP 7 días |
| `btc_usd_ma7` | Media móvil BTC 7 días |
| `btc_usd_ma30` | Media móvil BTC 30 días |

### Métricas compuestas

| Métrica | Descripción |
|---|---|
| `btc_ars_blue` | BTC en pesos al tipo blue (`btc_usd * dolar_blue`) |
| `btc_ars_blue_ma7` | Rolling 7 días de `btc_ars_blue` |
| `leliq_real_rate` | Tasa Leliq mensualizada menos CPI mensual |
| `dxy_vs_brecha_corr_30d` | Correlación rolling 30 días entre DXY y brecha cambiaria |
| `cpi_ar_vs_cpi_usa_ratio` | Índice relativo de inflación AR / USA |

### Flags de alertas (booleanos)

Usados por la LLM layer como señales de eventos relevantes del día.

| Flag | Condición |
|---|---|
| `dolar_blue_spike` | Variación diaria > 3% |
| `brecha_over_100pct` | Brecha cambiaria > 100% |
| `btc_drop_7d` | Caída BTC > 10% en 7 días |
| `btc_rally_7d` | Suba BTC > 10% en 7 días |
| `cpi_above_10pct` | CPI mensual AR > 10% |

---

## 8. AI Intelligence Layer (LLM)

### Posición en la arquitectura

```
Gold layer (Athena / S3)
         │
         ▼
 LLM Intelligence Layer
    ├── Narrative engine
    ├── NL Query layer
    └── Insight generator
         │
         ▼
Streamlit Dashboard
```

Esta capa no interactúa con Bronze ni Silver. No tiene acceso a ingesta ni a APIs de datos externos.

### Input a la capa

Snapshot diario del Gold layer: últimas N filas de `combined_metrics` (default: 90 días) serializado como JSON estructurado + flags de alertas del día corriente. El tamaño total del payload se limita a 4000 tokens para mantener latencia baja y costos controlados.

### Módulo 1: Narrative engine

| Parámetro | Valor |
|---|---|
| Trigger | Semanal (llm_insight_gen task, modificar schedule a `0 6 * * 1` para lunes) |
| Input | Resumen semanal: min/max/cierre de cada variable de la semana + variaciones % |
| System prompt | Rol: analista macroeconómico Argentina. Formato fijo: secciones Dólar, Inflación, Mercados globales, Contexto. Máximo 300 palabras. |
| Output | JSON: `{ summary_text, week_label, generated_at }` |
| Persistencia | Escrito en `gold/llm_insights/` en S3. No se regenera en cada carga del dashboard. |

### Módulo 2: NL Query layer

| Parámetro | Valor |
|---|---|
| Trigger | On-demand desde Streamlit (request HTTP) |
| Input | Pregunta del usuario en texto libre + schema de tablas Athena disponibles |
| System prompt | Schema de tablas, instrucción de generar únicamente SQL Athena válido o respuesta directa si la pregunta se puede responder con datos en memoria. |
| Output | SQL string (ejecutado por Streamlit vía PyAthena) o respuesta directa en texto |
| Restricciones | Si el SQL generado referencia tablas fuera del schema definido: se descarta sin ejecutar. Sin agentes, sin tool use, sin loops. Stateless por request. |

### Módulo 3: Insight generator

| Parámetro | Valor |
|---|---|
| Trigger | Diario (dentro del DAG principal, task `llm_insight_gen`) |
| Input | Valores del día actual vs media 30 días de cada métrica + flags de alertas |
| System prompt | Instrucción: detectar las 3 variaciones más relevantes del día, producir bullet points tipo "financial brief". Máximo 5 items. |
| Output | JSON: `{ insights: [{ metric, observation, magnitude }] }` |
| Fallback | Si la llamada LLM falla: se usa el insight JSON del día anterior. |

### Configuración técnica

- **LLM provider:** configurable vía variable de entorno `LLM_PROVIDER`. Default: `claude` (`claude-sonnet-4-20250514`). Alternativa: `openai` (`gpt-4o-mini`).
- **API key:** variable de entorno `LLM_API_KEY`. Nunca hardcodeada.
- **Sin fine-tuning, sin ML clásico, sin vector databases.**
- **Sin historial de conversación** en NL Query (cada request es independiente).

---

## 9. Dashboard (Streamlit)

### Pantalla 1 — Macro Argentina

**Componentes:**
- Cards superiores: valor actual dólar blue, brecha cambiaria %, CPI último mes.
- Gráfico de líneas multi-serie: dólar oficial, MEP, blue en el tiempo. Selector de rango: 30d / 90d / 1y.
- Gráfico de área: brecha cambiaria % histórica.
- Gráfico de barras: inflación mensual CPI AR.
- Panel lateral derecho: insights del día generados por LLM (Módulo 3).

### Pantalla 2 — Mercados globales

**Componentes:**
- Gráfico de líneas: BTC/USD con MA7 y MA30 superpuestas.
- Gráfico de líneas: DXY (USD Index) y CPI USA.
- Card: BTC en pesos al tipo blue (precio actual y variación 24h).

### Pantalla 3 — Correlaciones

**Componentes:**
- Scatter plot: dólar blue vs BTC/USD (90 días).
- Gráfico doble eje: DXY vs brecha cambiaria.
- Gráfico doble eje: inflación AR vs tipo de cambio mensual.
- Tabla resumen: correlaciones rolling 30d entre todas las variables principales.

### Pantalla 4 — AI Analyst

**Componentes:**
- Resumen narrativo semanal (Módulo 1), renderizado con `st.markdown`.
- Input de texto libre: pregunta → NL Query → resultado tabulado o respuesta en texto (Módulo 2).
- Accordion: historial de insights de los últimos 7 días.

### Flujo de usuario

- Sidebar con navegación entre las 4 pantallas.
- Selector de rango de fechas global (afecta todas las vistas).
- Botón "Actualizar": muestra timestamp de la última ejecución exitosa del DAG.
- Sin autenticación en esta fase (ver Constraints).

---

## 10. IaC — Terraform

### Objetivo

Provisionar toda la infraestructura AWS del proyecto de forma reproducible, versionada y declarativa. El estado de Terraform se almacena en S3 con DynamoDB para locking. Los recursos se agrupan en módulos por responsabilidad.

### Estructura de directorios

```
infra/
├── main.tf                   # Entry point: providers, backend, llamadas a módulos
├── variables.tf              # Variables globales del proyecto
├── outputs.tf                # Outputs exportados (ARNs, bucket names, etc.)
├── terraform.tfvars          # Valores de variables (no versionar si tiene secrets)
├── terraform.tfvars.example  # Template versionable
│
├── modules/
│   ├── s3/
│   │   ├── main.tf           # Bucket data lake + bucket athena results
│   │   ├── variables.tf
│   │   └── outputs.tf
│   │
│   ├── glue/
│   │   ├── main.tf           # Glue database + tablas externas (DDL via Terraform)
│   │   ├── variables.tf
│   │   └── outputs.tf
│   │
│   ├── athena/
│   │   ├── main.tf           # Workgroup + configuración de output location
│   │   ├── variables.tf
│   │   └── outputs.tf
│   │
│   └── iam/
│       ├── main.tf           # Roles y policies para el pipeline
│       ├── variables.tf
│       └── outputs.tf
│
└── state/
    └── backend.tf            # Configuración del remote backend (S3 + DynamoDB)
```

### Backend de estado remoto

El estado de Terraform se almacena en S3 con locking vía DynamoDB. Se usa un bucket separado del data lake para el estado.

**`state/backend.tf` — configuración:**
```hcl
terraform {
  backend "s3" {
    bucket         = "macro-intelligence-tfstate"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "macro-intelligence-tfstate-lock"
    encrypt        = true
  }
}
```

El bucket de estado y la tabla DynamoDB se crean **manualmente una sola vez** antes del primer `terraform init` (bootstrapping). No se gestionan con Terraform para evitar el problema del huevo y la gallina.

### Módulo S3

**Recursos gestionados:**
- `aws_s3_bucket.data_lake`: bucket principal del data lake (`macro-intelligence-{env}`).
- `aws_s3_bucket.athena_results`: bucket para resultados de queries Athena.
- `aws_s3_bucket_versioning`: habilitado en data lake (permite recuperar versiones de Parquet sobreescritos).
- `aws_s3_bucket_lifecycle_configuration`: regla de ciclo de vida en Bronze para archivar a S3 Glacier después de 90 días (reducción de costos).
- `aws_s3_bucket_public_access_block`: todos los buckets con acceso público bloqueado.
- `aws_s3_bucket_server_side_encryption_configuration`: SSE-S3 (AES-256) en todos los buckets.

**Variables del módulo:**
- `env`: entorno (`dev` | `prod`).
- `region`: región AWS.
- `bronze_glacier_days`: días antes de archivar Bronze a Glacier (default: `90`).
- `athena_results_ttl_days`: TTL para limpiar resultados Athena (default: `7`).

**Outputs del módulo:**
- `data_lake_bucket_name`
- `data_lake_bucket_arn`
- `athena_results_bucket_name`
- `athena_results_bucket_arn`

### Módulo Glue

**Recursos gestionados:**
- `aws_glue_catalog_database.macro_intelligence`: database en el Glue Data Catalog.
- `aws_glue_catalog_table` × 4: una tabla externa por cada tabla Gold del pipeline.

**Configuración de tablas externas en Glue:**

Cada tabla Glue define:
- `table_type = "EXTERNAL_TABLE"`
- `parameters.classification = "parquet"`
- `parameters.projection.enabled = "true"` (partition projection)
- `storage_descriptor.location`: apunta al prefijo S3 Gold correspondiente.
- `storage_descriptor.input_format / output_format / serde_info`: configuración estándar Parquet con Snappy.

**Partition projection por tabla:**

| Tabla | Columnas de partición | Tipo de proyección |
|---|---|---|
| `macro_ar_daily` | `year` (int, 2024-2030), `month` (int, 1-12) | INTEGER |
| `global_daily` | `year`, `month` | INTEGER |
| `combined_metrics` | `year`, `month` | INTEGER |
| `llm_insights` | `year`, `month`, `week` (1-53) | INTEGER |

**Variables del módulo:**
- `data_lake_bucket_name`: nombre del bucket S3 (output del módulo S3).
- `env`: entorno.

**Outputs del módulo:**
- `glue_database_name`
- `table_names`: map de nombre lógico → nombre físico en Glue.

### Módulo Athena

**Recursos gestionados:**
- `aws_athena_workgroup.macro_intelligence`: workgroup con configuración de output location y límite de datos escaneados.

**Configuración del workgroup:**
- `result_configuration.output_location`: apunta al bucket de resultados Athena (output del módulo S3).
- `configuration.bytes_scanned_cutoff_per_query`: `10737418240` (10 GB). Protección contra queries accidentalmente costosas.
- `configuration.enforce_workgroup_configuration = true`.
- `configuration.publish_cloudwatch_metrics_enabled = true`.

**Variables del módulo:**
- `athena_results_bucket_name`
- `bytes_scanned_cutoff_per_query`: default `10737418240` (10 GB).

**Outputs del módulo:**
- `workgroup_name`
- `workgroup_arn`

### Módulo IAM

**Recursos gestionados:**

`aws_iam_role.pipeline_role`: rol asumido por el proceso Airflow/Python en la máquina local o EC2.

`aws_iam_policy.s3_data_lake`: policy con permisos mínimos sobre los buckets del data lake.

Permisos concedidos por política:

| Acción | Recurso | Justificación |
|---|---|---|
| `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` | `data_lake_bucket/*` | ETL: leer Bronze, escribir Silver/Gold |
| `s3:ListBucket` | `data_lake_bucket` | Listar particiones |
| `s3:PutObject`, `s3:GetObject` | `athena_results_bucket/*` | Leer/escribir resultados de queries |
| `glue:GetDatabase`, `glue:GetTable`, `glue:GetPartitions` | `macro_intelligence` database | Athena necesita acceder al catálogo |
| `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults` | workgroup `macro_intelligence` | Ejecutar queries desde Streamlit / LLM layer |

**Principio de mínimo privilegio:** no se concede `s3:*` ni `glue:*` completo. Los permisos de escritura en Glue no se incluyen porque las tablas se definen vía Terraform, no vía el pipeline en runtime.

`aws_iam_user.pipeline_user` (solo para dev local): usuario IAM con `aws_iam_user_policy_attachment` al rol anterior. Las credenciales se generan fuera de Terraform y se configuran como variables de entorno.

**Variables del módulo:**
- `data_lake_bucket_arn`
- `athena_results_bucket_arn`
- `env`

**Outputs del módulo:**
- `pipeline_role_arn`
- `pipeline_user_name` (si `env = "dev"`)

### `main.tf` — composición de módulos

```hcl
# Estructura conceptual del main.tf
# (no código ejecutable, representa las dependencias entre módulos)

provider "aws" {
  region = var.region
}

module "s3" {
  source      = "./modules/s3"
  env         = var.env
  region      = var.region
}

module "iam" {
  source                   = "./modules/iam"
  data_lake_bucket_arn     = module.s3.data_lake_bucket_arn
  athena_results_bucket_arn = module.s3.athena_results_bucket_arn
  env                      = var.env
}

module "glue" {
  source                = "./modules/glue"
  data_lake_bucket_name = module.s3.data_lake_bucket_name
  env                   = var.env
  depends_on            = [module.s3]
}

module "athena" {
  source                    = "./modules/athena"
  athena_results_bucket_name = module.s3.athena_results_bucket_name
}
```

### Variables globales (`variables.tf`)

| Variable | Tipo | Default | Descripción |
|---|---|---|---|
| `env` | string | `"dev"` | Entorno: `dev` o `prod` |
| `region` | string | `"us-east-1"` | Región AWS |
| `project` | string | `"macro-intelligence"` | Prefijo para naming de recursos |
| `bronze_glacier_days` | number | `90` | Días para archivado a Glacier |
| `athena_results_ttl_days` | number | `7` | TTL de resultados Athena |
| `bytes_scanned_cutoff_gb` | number | `10` | Límite de GB por query en Athena |

### Flujo de trabajo Terraform

```
# 1. Bootstrap manual (solo la primera vez)
#    Crear bucket tfstate y tabla DynamoDB manualmente via AWS Console o CLI

# 2. Inicialización
terraform init

# 3. Plan — revisión de cambios antes de aplicar
terraform plan -var-file="terraform.tfvars"

# 4. Apply
terraform apply -var-file="terraform.tfvars"

# 5. Para destruir el entorno dev
terraform destroy -var-file="terraform.tfvars" -target="module.s3"
```

### Recursos excluidos de Terraform

| Recurso | Razón |
|---|---|
| Airflow (local Docker) | No es infraestructura AWS |
| Streamlit (local Docker) | No es infraestructura AWS |
| EC2 (si se decide usar) | Se agrega en una iteración posterior |
| LLM API credentials | Secret externo, nunca en Terraform state |
| Bucket de tfstate y tabla DynamoDB | Bootstrap manual para evitar dependencia circular |

### Naming convention de recursos AWS

```
{project}-{resource_type}-{env}
```

Ejemplos:
- `macro-intelligence-data-lake-prod`
- `macro-intelligence-athena-results-prod`
- `macro-intelligence-pipeline-role-prod`
- `macro-intelligence-tfstate` (único, sin sufijo de entorno)

---

## 11. Deployment (Docker + AWS)

### Qué corre dónde

| Componente | Entorno | Notas |
|---|---|---|
| Apache Airflow | Docker local | `docker-compose up airflow-*` |
| Python ETL tasks | Docker local (Airflow workers) | Mismo contenedor que el scheduler |
| Streamlit dashboard | Docker local | Puerto 8501; alternativa: Streamlit Community Cloud (gratis para repos públicos) |
| AWS S3 | Cloud | Free tier: 5 GB almacenamiento, 20K GET, 2K PUT/mes |
| AWS Athena | Cloud | Pay-per-query: $5/TB escaneado; con Parquet + particiones → < $0.05/mes en dev |
| AWS Glue Data Catalog | Cloud | Free tier: 1M objetos de metadata/mes |
| LLM API | Cloud (externo) | ~$0.30–0.60 USD/mes con ~120K tokens/mes |

### Docker Compose (servicios locales)

```
services:
  postgres          # metadata DB de Airflow (puerto 5432)
  airflow-webserver # UI de Airflow (puerto 8080)
  airflow-scheduler # scheduler del DAG
  airflow-worker    # ejecuta las Python tasks
  streamlit         # dashboard (puerto 8501)
```

Todos los servicios comparten:
- Volumen para código ETL (`./dags`, `./src`).
- Variables de entorno AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).
- Variable `LLM_API_KEY`.

### Estrategia de costos mínimos

| Item | Costo estimado/mes |
|---|---|
| S3 storage (< 500 MB datos) | $0.00 (free tier) |
| S3 requests | $0.00 (free tier) |
| Athena (Parquet + particiones) | $0.01–0.05 |
| Glue Data Catalog | $0.00 (free tier) |
| LLM API (120K tokens) | $0.30–0.60 |
| EC2 (si se necesita para Airflow) | $0.00 (t2.micro free tier 750h/mes) |
| **Total estimado** | **< $1 USD/mes** |

**Técnicas de reducción de costo en Athena:**
- Parquet columnar reduce datos escaneados ~10x vs CSV.
- Partition projection elimina particiones irrelevantes en cada query.
- El workgroup Terraform tiene límite de 10 GB por query como protección.
- Las queries del dashboard usan siempre filtros por `year` y `month`.

---

## 12. Constraints & Trade-offs

### Limitaciones de diseño

**INDEC sin API oficial:** el scraping es el componente más frágil. Un cambio en el HTML del sitio rompe el extractor. Mitigación: test de schema en `validate_sources`; si el scraping retorna estructura inesperada, se loguea y se salta sin bloquear el pipeline.

**Airflow local:** el pipeline no corre si la máquina está apagada. Para un sistema de producción con disponibilidad garantizada, el siguiente paso es migrar a EC2 t2.micro o MWAA (este último tiene costo significativo, no recomendado en esta fase).

**Bluelytics sin SLA:** es un servicio no oficial con alta disponibilidad histórica pero sin garantías. Si se da de baja, se necesita reemplazar con scraping del BNA directamente.

**CoinGecko free tier:** rate limit de 30 rpm. La task de extracción debe implementar sleep entre llamadas. En caso de throttling agresivo, el retry con backoff resuelve el problema en la misma ejecución.

**NL Query sin guardrails de SQL:** el SQL generado por el LLM podría referenciar tablas inexistentes o generar queries costosas. Mitigación básica: whitelist de tablas permitidas y límite de bytes escaneados en el workgroup Athena. Para exposición pública, agregar sanitización explícita.

### Simplificaciones asumidas

- Sin autenticación en Streamlit. Aceptable para uso personal. Para entorno compartido: agregar `streamlit-authenticator`.
- Sin versionado de datos (no Delta Lake, no Iceberg). Rollback manual re-ejecutando el DAG del día afectado.
- Sin backfill automático. La carga histórica inicial se hace con un script separado por fuera del DAG regular.
- Glue tables definidas vía Terraform (DDL estático). Si el schema de Gold cambia, se actualiza el recurso Terraform y se hace `terraform apply`.

### Riesgos identificados

| Riesgo | Probabilidad | Impacto | Mitigación |
|---|---|---|---|
| Scraping INDEC rompe | Media | Bajo (dato mensual) | Alertas en DAG; fuente de backup: IPC data de fuentes secundarias |
| Bluelytics se discontinúa | Baja | Alto | Tener scraping BNA como fallback documentado |
| LLM API down | Media | Bajo (pipeline continúa) | Uso de insight del día anterior como fallback |
| Costo Athena inesperado | Baja | Medio | Límite por query en workgroup Terraform |
| Cambio en formato BCRA API | Media | Medio | Validación de schema en Silver task |

---

*Fin del documento — v1.0*