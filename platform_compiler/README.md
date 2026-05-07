# Platform Compiler - Shell Version

Компилятор SQL-DSL в платформенный код (Apache Spark / Apache Flink), переписанный на Bash.

## Возможности

- Парсинг SQL-DSL файлов (DEFINE TABLE, CREATE VIEW, INSERT INTO SELECT)
- Загрузка YAML конфигурации платформы
- Генерация кода для:
  - **Apache Spark** (Batch/Streaming) с Airflow DAG
  - **Apache Flink** (Streaming) с Kubernetes manifest
- Поддержка коннекторов: Kafka, HDFS, S3
- Цветной вывод логов

## Установка

Скрипт не требует дополнительных зависимостей кроме:
- `bash` (версия 4.0+)
- `grep` (с поддержкой `-P` для PCRE)
- `sed`, `awk`
- Стандартные утилиты Unix

## Использование

```bash
./compiler.sh -d <dsl_file> -c <config_file> -o <output_dir>
```

### Параметры

| Параметр | Описание |
|----------|----------|
| `-d, --dsl` | Путь к файлу SQL-DSL (pipeline.dsl) |
| `-c, --config` | Путь к YAML конфигурации платформы |
| `-o, --output` | Директория для выходных файлов |
| `-h, --help` | Показать справку |

### Примеры

```bash
# Компиляция для Spark
./compiler.sh -d examples/pipeline.dsl -c examples/spark_config.yaml -o output/spark

# Компиляция для Flink
./compiler.sh -d examples/pipeline.dsl -c examples/flink_config.yaml -o output/flink
```

## Формат DSL

```sql
-- Определение таблицы
DEFINE TABLE users (
  id STRING,
  name STRING,
  age INT,
  ts TIMESTAMP
) WITH STREAM (
  time_attribute = 'ts',
  watermark = '5 seconds'
);

-- Представление
CREATE VIEW active_users AS
SELECT id, name, age
FROM users
WHERE age > 18;

-- Вставка в таблицу
INSERT INTO output_table
SELECT id, name, age
FROM active_users;
```

## Формат конфигурации (YAML)

```yaml
meta:
  name: my_pipeline
  version: "1.0.0"
  owner: data-engineering

target:
  platform: spark  # или flink
  mode: batch      # или streaming

execution:
  parallelism: 4
  memory:
    driver: "2g"
    executor: "4g"

orchestration:
  type: airflow    # или kubernetes
  schedule_interval: "@daily"

connectors:
  sources:
    - name: users
      type: kafka
      config:
        bootstrap_servers: ["localhost:9092"]
        topic: users-topic
  sinks:
    - name: output_table
      type: hdfs
      config:
        path: /data/output
```

## Выходные файлы

В зависимости от платформы и типа оркестрации генерируются:

### Spark + Airflow
- `<pipeline_name>.sql` - Spark SQL скрипт
- `dag_<pipeline_name>.py` - Airflow DAG

### Flink + Kubernetes
- `<pipeline_name>.sql` - Flink SQL скрипт
- `deployment_<pipeline_name>.yaml` - Kubernetes manifest

## Структура проекта

```
platform_compiler/
├── compiler.sh          # Главный скрипт компилятора
├── README.md            # Документация
├── core/                # Вспомогательные скрипты (опционально)
├── compilers/           # Шаблоны генераторов (опционально)
├── models/              # Определения структур (опционально)
└── output/              # Выходные файлы
```

## Отличия от Python версии

Shell версия является упрощенной реализацией и имеет следующие ограничения:

1. **Парсинг YAML**: Используется простой grep/sed парсер вместо полноценного YAML парсера
2. **Валидация**: Минимальная проверка входных данных
3. **Сложные типы данных**: Упрощенная обработка ARRAY, MAP, STRUCT
4. **ML модели**: Не поддерживается генерация кода для ML моделей
5. **Графы**: Не поддерживается DEFINE GRAPH

Для сложных сценариев рекомендуется использовать Python версию.

## Лицензия

MIT License
