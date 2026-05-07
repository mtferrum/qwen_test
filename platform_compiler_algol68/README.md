# Platform Compiler - Algol 68 Implementation

Репозиторий содержит реализацию **Platform Compiler** на языке программирования **ALGOL 68**.

## Описание

Platform Compiler — это компилятор для генерации кода из SQL-DSL в платформенный код для:
- **Apache Spark** (Batch + Structured Streaming)
- **Apache Flink** (Streaming)

Эта версия написана на ALGOL 68 и демонстрирует перенос архитектуры оригинального Python-компилятора на классический императивный язык программирования.

## Структура проекта

```
platform_compiler_algol68/
├── README.md                          # Этот файл
├── src/
│   └── platform_compiler.a68          # Основная реализация компилятора
└── examples/
    ├── pipeline.dsl                   # Пример DSL файла
    └── platform.yaml                  # Пример конфигурации
```

## Требования

Для запуска программы требуется интерпретатор **ALGOL 68G**:

```bash
# Установка на Ubuntu/Debian
sudo apt-get install algol68g

# Установка на macOS (через Homebrew)
brew install algol68g

# Установка на Fedora/RHEL
sudo dnf install algol68g
```

## Запуск

### Тестовый запуск

```bash
cd /workspace/platform_compiler_algol68/src
a68g platform_compiler.a68
```

### Компиляция и выполнение

```bash
# Компиляция в исполняемый файл
a68c platform_compiler.a68 -o platform_compiler

# Запуск
./platform_compiler
```

## Архитектура

Реализация следует той же архитектуре, что и оригинальный Python-компилятор:

### Основные компоненты

1. **MODE DECLARATIONS** — Определение типов данных:
   - `STRING_DICT` — словарь ключ-значение
   - `STRING_LIST` — список строк
   - `TABLE_DEF` — определение таблицы
   - `STREAM_CFG` — конфигурация стриминга
   - `MODEL_DEF` — определение ML-модели
   - `GRAPH_DEF` — определение графа
   - `VIEW_DEF` — представление (VIEW)
   - `DSL_LAYER` — полное определение пайплайна
   - `PLATFORM_CFG` — конфигурация платформы
   - `EXEC_CFG` — конфигурация выполнения
   - `ORCH_CFG` — конфигурация оркестрации

2. **DSL_PARSER** — Парсер SQL-DSL:
   - `parse_file()` — разбор файла DSL
   - `parse_content()` — разбор содержимого из строки
   - `remove_comments()` — удаление комментариев
   - `parse_tables()` — парсинг DEFINE TABLE
   - `parse_models()` — парсинг DEFINE MODEL
   - `parse_views()` — парсинг CREATE VIEW
   - `parse_inserts()` — парсинг INSERT операций

3. **CONFIG_LOADER** — Загрузчик конфигурации:
   - `load_file()` — загрузка из YAML файла
   - `load_content()` — загрузка из строки
   - `validate_config()` — валидация конфигурации

4. **SPARK_GENERATOR** — Генератор кода для Spark:
   - `generate_full_script_spark()` — полный SQL скрипт
   - `generate_udf_registrations_spark()` — регистрация UDF
   - `generate_source_ddl_spark()` — DDL источников
   - `generate_sink_ddl_spark()` — DDL приёмников
   - `generate_transformations_spark()` — трансформации
   - `generate_airflow_dag()` — Airflow DAG

5. **FLINK_GENERATOR** — Генератор кода для Flink:
   - `generate_full_script_flink()` — полный SQL скрипт
   - `generate_udf_registrations_flink()` — регистрация UDF
   - `generate_source_ddl_flink()` — DDL источников
   - `generate_sink_ddl_flink()` — DDL приёмников
   - `generate_transformations_flink()` — трансформации
   - `generate_k8s_manifest()` — Kubernetes манифест

6. **PLATFORM_COMPILER** — Главный компилятор:
   - `compile()` — компиляция из файлов
   - `compile_from_strings()` — компиляция из строк

## Пример использования

```algol68
# Создание компилятора
PLATFORM_COMPILER compiler := create_compiler();

# Компиляция из строк
OUTPUT_DICT result := compile_from_strings(compiler,
    "-- Test DSL" + newline +
    "DEFINE TABLE events (id INT, timestamp TIMESTAMP);",
    "meta:" + newline +
    "  name: test_pipeline" + newline +
    "target:" + newline +
    "  platform: spark" + newline +
    "  mode: batch"
);

# Проверка результата
IF result /= NIL THEN
    print(("Generated file: ", FILENAME OF result, newline));
    print((CONTENT OF result, newline))
ELSE
    print(("Compilation failed", newline))
FI
```

## Отличия от Python-версии

1. **Типизация**: ALGOL 68 использует строгую статическую типизацию с явным объявлением MODE
2. **Структуры данных**: Словари и списки реализованы как связные структуры (REF STRUCT)
3. **Отсутствие ООП**: Вместо классов используются PROCEDURE и MODE
4. **Управление памятью**: Явное выделение памяти через HEAP
5. **Синтаксис**: Блочные конструкции BEGIN...END, IF...THEN...ELIF...ELSE...FI

## Поддерживаемые платформы

- ✅ Apache Spark (Batch режим)
- ✅ Apache Spark (Structured Streaming)
- ✅ Apache Flink (Streaming)
- ✅ Airflow DAG генерация
- ✅ Kubernetes манифесты для Flink

## Ограничения текущей реализации

1. **YAML парсинг**: В Algol 68 нет встроенной поддержки YAML, требуется внешняя библиотека или упрощённый парсер
2. **Регулярные выражения**: Отсутствуют в стандартной библиотеке, парсинг реализуется вручную
3. **Работа с файлами**: Упрощённая реализация для демонстрации

## Лицензия

Аналогично оригинальному проекту.

## Авторы

Перевод на ALGOL 68 выполнен для демонстрации возможностей языка и сохранения архитектуры оригинального Platform Compiler.
