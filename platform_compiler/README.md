# Platform Compiler for Spark + Airflow и Flink

Платформенный компилятор для генерации кода пайплайнов обработки данных на основе SQL-DSL.

## 📋 Обзор

Компилятор преобразует декларативное описание пайплайна (SQL-DSL) в исполняемый код для:
- **Apache Spark** (Batch/Streaming) + **Airflow** для оркестрации
- **Apache Flink** (Streaming) + **Kubernetes** для оркестрации

## 🏗️ Архитектура

```
pipeline.dsl (SQL-DSL) ──┐
                         ├──> Parser ──> IR ──> Code Generator ──> Platform Code
platform.yaml (Config) ──┘
```

## 📁 Структура проекта

```
platform_compiler/
├── __init__.py              # Основной модуль
├── compiler.py              # Главный компилятор
├── core/
│   ├── parser.py            # SQL-DSL парсер
│   └── config_loader.py     # Загрузчик конфигурации YAML
├── compilers/
│   ├── base.py              # Базовый класс генератора
│   ├── spark_generator.py   # Генератор Spark SQL
│   └── flink_generator.py   # Генератор Flink SQL
├── models/
│   └── schemas.py           # Модели данных (Pydantic)
├── examples/
│   ├── pipeline.dsl         # Пример DSL описания
│   ├── spark_batch_airflow.yaml    # Конфигурация Spark+Airflow
│   ├── flink_streaming_k8s.yaml    # Конфигурация Flink+K8s
│   └── validate_spark_sql.py       # Валидатор Spark SQL
└── output/
    ├── spark_final/         # Сгенерированный код для Spark
    └── flink_final/         # Сгенерированный код для Flink
```

## 🚀 Быстрый старт

### 1. Компиляция пайплайна

```python
from platform_compiler import compile_pipeline

# Spark Batch + Airflow
compile_pipeline(
    dsl_path='examples/pipeline.dsl',
    config_path='examples/spark_batch_airflow.yaml',
    output_dir='output/spark_final'
)

# Flink Streaming + Kubernetes
compile_pipeline(
    dsl_path='examples/pipeline.dsl',
    config_path='examples/flink_streaming_k8s.yaml',
    output_dir='output/flink_final'
)
```

### 2. Валидация сгенерированного SQL

```bash
cd examples
python validate_spark_sql.py
```

## 📝 SQL-DSL Синтаксис

### Определение источников данных

```sql
DEFINE TABLE raw_events (
    event_id STRING,
    user_id STRING,
    transaction_amount DOUBLE,
    merchant_id STRING,
    event_time TIMESTAMP(3)
) WITH STREAM (
    time_attribute = event_time,
    watermark = '10 seconds'
);
```

### Определение ML моделей

```sql
DEFINE MODEL fraud_detector (
    path = 'hdfs:///models/fraud_detector_v2.pkl',
    input_schema = STRUCT<user_id STRING, amount DOUBLE, merchant_risk DOUBLE>,
    output_schema = FLOAT
);
```

### Трансформации

```sql
CREATE VIEW enriched_transactions AS
SELECT
    t.event_id,
    t.user_id,
    t.transaction_amount,
    m.risk_score AS merchant_risk
FROM raw_events t
LEFT JOIN merchant_risk m ON t.merchant_id = m.merchant_id;

CREATE VIEW fraud_predictions AS
SELECT
    e.event_id,
    APPLY_MODEL(fraud_detector, 
        STRUCT(e.user_id, e.transaction_amount, e.merchant_risk)
    ) AS fraud_probability
FROM enriched_transactions e;
```

### Вывод результатов

```sql
INSERT INTO fraud_alerts
SELECT event_id, user_id, transaction_amount, fraud_probability
FROM fraud_predictions
WHERE fraud_probability > 0.8;
```

## ⚙️ Конфигурация (platform.yaml)

### Spark Batch + Airflow

```yaml
meta:
  name: "fraud-detection-batch"
  version: "1.0.0"

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
        path: "hdfs:///data/raw_events/parquet/"
        format: parquet

orchestration:
  type: airflow
  schedule_interval: "0 2 * * *"
  retries: 2
```

### Flink Streaming + Kubernetes

```yaml
target:
  platform: flink
  mode: streaming

execution:
  parallelism: 32
  checkpointing:
    enabled: true
    interval: "60s"
    backend: rocksdb

connectors:
  sources:
    - name: raw_events
      type: kafka
      config:
        bootstrap_servers: ["kafka:9092"]
        topic: "transactions-prod"

orchestration:
  type: kubernetes
  namespace: "flink-jobs"
  image: "registry.company.com/flink-python:1.17"
```

## 📊 Сгенерированные артефакты

### Для Spark

- `fraud-detection-batch.sql` - Spark SQL скрипт
- `dag_fraud_detection_batch.py` - Airflow DAG (опционально)

### Для Flink

- `fraud-detection-streaming.sql` - Flink SQL скрипт
- `deployment_fraud_detection.yaml` - Kubernetes manifest (опционально)

## ✅ Валидация

Валидатор проверяет:
- ✅ Структуру SQL statements
- ✅ Соответствие таблиц источникам/приемникам
- ✅ Регистрацию UDF
- ✅ Корректность INSERT targets

Пример вывода валидатора:
```
Views defined: 6
INSERT targets: 2
UDFs used: 1
Errors: 0
Warnings: 0

✅ VALIDATION PASSED
```

## 🔧 Расширение

### Добавление новой платформы

1. Создайте класс-генератор в `compilers/`:
```python
from .base import BaseCodeGenerator

class NewPlatformGenerator(BaseCodeGenerator):
    def _get_platform_name(self) -> str:
        return "new_platform"
    
    def generate_source_ddl(self) -> str:
        # Реализация
        pass
```

2. Зарегистрируйте в `compiler.py`:
```python
if platform == 'new_platform':
    generator = NewPlatformGenerator(self.dsl, self.config)
```

## 📚 Примеры использования

Смотрите директорию `examples/`:
- `pipeline.dsl` - полное описание пайплайна fraud detection
- `spark_batch_airflow.yaml` - конфигурация для nightly batch пересчета
- `flink_streaming_k8s.yaml` - конфигурация для real-time обработки

## 📄 Лицензия

Internal use only.
