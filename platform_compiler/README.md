# Platform Compiler

A declarative SQL-DSL compiler for generating platform-specific code for **Apache Spark** (Batch/Streaming) and **Apache Flink** (Streaming), with orchestration support for **Airflow** and **Kubernetes**.

Based on specifications from chat-export-1778167073196.json.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  pipeline.dsl (SQL-DSL - Business Logic)                    │
│  - DEFINE TABLE/MODEL/GRAPH                                 │
│  - CREATE VIEW / INSERT SELECT                              │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  platform.yaml (Platform Configuration)                     │
│  - Target: Spark/Flink, Batch/Streaming                     │
│  - Connectors: Kafka, HDFS, S3                              │
│  - Resources: CPU, Memory, Parallelism                      │
│  - Orchestration: Airflow/Kubernetes                        │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Platform Compiler                                          │
│  - Parser: Extract DSL constructs                           │
│  - Config Loader: Validate & load config                    │
│  - Code Generator: Platform-specific output                 │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Generated Artifacts                                        │
│  - Spark: .sql + Airflow DAG (.py)                          │
│  - Flink: .sql + K8s Deployment (.yaml)                     │
└─────────────────────────────────────────────────────────────┘
```

## Installation

```bash
pip install pydantic pyyaml
```

## Quick Start

### 1. Create SQL-DSL Pipeline (`pipeline.dsl`)

```sql
-- Define data source
DEFINE TABLE raw_events (
    event_id STRING,
    user_id STRING,
    amount DOUBLE,
    event_time TIMESTAMP(3)
) WITH STREAM (
    time_attribute = event_time,
    watermark = '10 seconds'
);

-- Define ML model
DEFINE MODEL fraud_detector (
    path = 'hdfs:///models/fraud_v1.pkl',
    input_schema = STRUCT<user_id STRING, amount DOUBLE>,
    output_schema = FLOAT
);

-- Business logic
CREATE VIEW fraud_predictions AS
SELECT
    event_id,
    APPLY_MODEL(fraud_detector, STRUCT(user_id, amount)) AS fraud_score
FROM raw_events;

-- Windowed aggregation
CREATE TABLE stats_5min AS
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTES) AS window_start,
    COUNT(*) AS total_events
FROM fraud_predictions
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTES);
```

### 2. Create Platform Configuration (`platform.yaml`)

#### Spark Batch + Airflow

```yaml
meta:
  name: "fraud-detection-batch"
  version: "1.0.0"
  owner: "data-team"

target:
  platform: spark
  mode: batch

execution:
  parallelism: 50
  memory:
    driver: "8g"
    executor: "16g"

connectors:
  sources:
    - name: raw_events
      type: hdfs
      config:
        path: "hdfs:///data/events/"
        format: parquet

orchestration:
  type: airflow
  schedule_interval: "0 2 * * *"
```

#### Flink Streaming + Kubernetes

```yaml
meta:
  name: "fraud-detection-streaming"
  version: "1.0.0"

target:
  platform: flink
  mode: streaming

execution:
  parallelism: 32
  checkpointing:
    enabled: true
    interval: "60s"
    backend: rocksdb
    path: "s3://checkpoints/flink/"

connectors:
  sources:
    - name: raw_events
      type: kafka
      config:
        bootstrap_servers: ["kafka:9092"]
        topic: "events-prod"

orchestration:
  type: kubernetes
  namespace: "flink-jobs"
  image: "flink:1.17"
```

### 3. Compile

```python
from platform_compiler import compile_pipeline

outputs = compile_pipeline(
    dsl_path='pipeline.dsl',
    config_path='platform.yaml',
    output_dir='./output'
)

print("Generated files:", list(outputs.keys()))
```

## Project Structure

```
platform_compiler/
├── __init__.py              # Package initialization
├── compiler.py              # Main compiler orchestrator
├── core/
│   ├── parser.py            # SQL-DSL parser
│   └── config_loader.py     # YAML config loader
├── compilers/
│   ├── base.py              # Abstract base generator
│   ├── spark_generator.py   # Spark SQL + Airflow generator
│   └── flink_generator.py   # Flink SQL + K8s generator
├── models/
│   └── schemas.py           # Pydantic data models
├── examples/
│   ├── pipeline.dsl         # Example DSL file
│   ├── spark_batch_airflow.yaml
│   └── flink_streaming_k8s.yaml
└── output/                  # Generated artifacts
```

## Supported Platforms

| Platform | Mode | Orchestration | Output |
|----------|------|---------------|--------|
| Spark | Batch | Airflow | `.sql` + DAG `.py` |
| Spark | Streaming | Kubernetes | `.sql` |
| Flink | Streaming | Kubernetes | `.sql` + K8s `.yaml` |

## SQL-DSL Reference

### DEFINE TABLE

```sql
DEFINE TABLE table_name (
    column_name DATA_TYPE,
    ...
) WITH STREAM (
    time_attribute = timestamp_column,
    watermark = 'interval',
    allowed_lateness = 'interval'
);
```

### DEFINE MODEL

```sql
DEFINE MODEL model_name (
    path = 'storage_path',
    input_schema = schema_definition,
    output_schema = return_type
);
```

### Built-in Functions

| Function | Type | Description |
|----------|------|-------------|
| `APPLY_MODEL` | UDF | Apply ML model |
| `TUMBLE` | Window | Fixed windows |
| `HOP` | Window | Sliding windows |
| `SESSION` | Window | Session windows |
| `TUMBLE_START` | Scalar | Window start time |
| `TUMBLE_END` | Scalar | Window end time |

## License

MIT
