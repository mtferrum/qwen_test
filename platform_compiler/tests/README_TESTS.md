# Руководство по тестированию Platform Compiler

## Обзор

Этот документ описывает систему тестирования для Platform Compiler - инструмента компиляции SQL-DSL в код для Apache Spark и Apache Flink.

## Структура тестов

```
platform_compiler/tests/
├── __init__.py              # Инициализация пакета тестов
└── test_compiler.py         # Основные тесты
```

## Запуск тестов

### Все тесты
```bash
cd /workspace
python -m pytest platform_compiler/tests/ -v
```

### Тесты с подробным выводом
```bash
python -m pytest platform_compiler/tests/ -v --tb=long
```

### Запуск конкретных тестов
```bash
# Тесты парсера
python -m pytest platform_compiler/tests/test_compiler.py::TestDSLParser -v

# Тесты загрузчика конфигурации
python -m pytest platform_compiler/tests/test_compiler.py::TestConfigLoader -v

# Тесты генератора Spark
python -m pytest platform_compiler/tests/test_compiler.py::TestSparkGenerator -v

# Тесты генератора Flink
python -m pytest platform_compiler/tests/test_compiler.py::TestFlinkGenerator -v

# Интеграционные тесты
python -m pytest platform_compiler/tests/test_compiler.py::TestIntegration -v
```

### Запуск с покрытием кода (требуется pytest-cov)
```bash
pip install pytest-cov
python -m pytest platform_compiler/tests/ --cov=platform_compiler --cov-report=html
```

## Категории тестов

### 1. Тесты парсера DSL (`TestDSLParser`)

Проверяют корректность разбора SQL-DSL файлов:

- `test_parse_simple_table` - Парсинг простых таблиц
- `test_parse_table_with_stream` - Таблицы с конфигурацией стриминга
- `test_parse_model` - Определения ML-моделей
- `test_parse_graph` - Определения графов
- `test_parse_view` - CREATE VIEW конструкции
- `test_parse_insert` - INSERT INTO операции
- `test_parse_comments_removed` - Удаление комментариев
- `test_parse_complex_types` - Сложные типы данных (ARRAY, MAP, STRUCT)
- `test_parse_full_dsl_file` - Полный файл из примеров

### 2. Тесты загрузчика конфигурации (`TestConfigLoader`)

Проверяют загрузку и валидацию YAML конфигурации:

- `test_load_spark_batch_config` - Загрузка Spark Batch конфигурации
- `test_load_flink_streaming_config` - Загрузка Flink Streaming конфигурации
- `test_validate_config` - Валидация конфигурации
- `test_validate_flink_requires_checkpointing` - Проверка требования checkpointing для Flink
- `test_load_from_string` - Загрузка из строки

### 3. Тесты генератора Spark (`TestSparkGenerator`)

Проверяют генерацию кода для Apache Spark:

- `test_generate_header` - Генерация заголовка скрипта
- `test_generate_source_ddl` - DDL для источников данных
- `test_generate_transformations` - Трансформации (VIEW, INSERT)
- `test_translate_window_functions` - Трансляция оконных функций
- `test_format_complex_types` - Форматирование типов данных

### 4. Тесты генератора Flink (`TestFlinkGenerator`)

Проверяют генерацию кода для Apache Flink:

- `test_generate_kafka_source_with_watermark` - Kafka источники с watermark
- `test_generate_k8s_manifest` - Kubernetes манифесты
- `test_translate_flink_functions` - Трансляция функций Flink

### 5. Тесты компилятора (`TestPlatformCompiler`)

Проверяют работу основного компилятора:

- `test_compile_from_strings_spark` - Компиляция из строк (Spark)
- `test_compile_from_strings_flink` - Компиляция из строк (Flink)
- `test_compile_example_files` - Компиляция файлов из примеров
- `test_unsupported_platform_error` - Обработка ошибок

### 6. Интеграционные тесты (`TestIntegration`)

Полный цикл компиляции:

- `test_full_spark_batch_pipeline` - Spark Batch пайплайн с Airflow DAG
- `test_full_flink_streaming_pipeline` - Flink Streaming пайплайн с K8s манифестом

## Примеры тестовых данных

Тесты используют файлы из директории `examples/`:

- `pipeline.dsl` - Пример SQL-DSL пайплайна
- `spark_batch_airflow.yaml` - Конфигурация Spark + Airflow
- `flink_streaming_k8s.yaml` - Конфигурация Flink + Kubernetes

## Добавление новых тестов

### Шаблон теста

```python
def test_my_feature():
    """Описание того, что проверяет тест."""
    # Arrange (подготовка)
    parser = DSLParser()
    dsl_content = "DEFINE TABLE test (id INT);"
    
    # Act (действие)
    result = parser.parse_content(dsl_content)
    
    # Assert (проверка)
    assert len(result.tables) == 1
    assert result.tables[0].name == "test"
```

### Лучшие практики

1. **Используйте фикстуры** для общих объектов
2. **Группируйте тесты** по классам (TestDSLParser, TestConfigLoader, etc.)
3. **Документируйте** каждый тест docstring
4. **Тестируйте граничные случаи** и ошибки
5. **Используйте параметризацию** для похожих тестов

## Устранение неполадок

### Тесты не запускаются

```bash
# Убедитесь что зависимости установлены
pip install pytest pyyaml pydantic

# Проверите путь к тестам
ls platform_compiler/tests/
```

### Ошибки импорта

```bash
# Запускайте тесты из корня проекта
cd /workspace
python -m pytest platform_compiler/tests/
```

### Ложные срабатывания

Если тест падает на реальных данных:
1. Проверьте актуальность тестовых файлов в `examples/`
2. Обновите ожидаемые значения в_assert_
3. Убедитесь что парсер обрабатывает новые конструкции

## CI/CD интеграция

### GitHub Actions

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.12
      - name: Install dependencies
        run: pip install pytest pyyaml pydantic
      - name: Run tests
        run: python -m pytest platform_compiler/tests/ -v
```

## Метрики качества

- **Покрытие кода**: Цель > 80%
- **Все тесты должны проходить**: 0 failed
- **Время выполнения**: < 5 секунд

## Контакты

По вопросам тестирования обращайтесь к:
- Документация: `README.md`
- Исходный код: `platform_compiler/`
