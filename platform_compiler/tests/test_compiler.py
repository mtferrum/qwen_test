"""
Тесты для Platform Compiler.

Проверяют корректность работы:
- Парсера SQL-DSL
- Загрузчика конфигурации
- Генераторов кода (Spark, Flink)
- Основного компилятора
"""

import pytest
from pathlib import Path

from platform_compiler.models.schemas import (
    DSLLayer, TableDefinition, StreamConfig, ModelDefinition,
    GraphDefinition, PlatformConfig, ExecutionConfig
)
from platform_compiler.core.parser import DSLParser
from platform_compiler.core.config_loader import ConfigLoader
from platform_compiler.compilers.spark_generator import SparkGenerator
from platform_compiler.compilers.flink_generator import FlinkGenerator
from platform_compiler.compiler import PlatformCompiler, compile_pipeline


# ==============================================================================
# Тесты парсера DSL (core/parser.py)
# ==============================================================================

class TestDSLParser:
    """Тесты для парсера SQL-DSL."""

    def test_parse_simple_table(self):
        """Проверка парсинга простой таблицы без стриминга."""
        parser = DSLParser()
        dsl_content = """
        DEFINE TABLE users (
            id INT,
            name STRING,
            email STRING
        );
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.name == "users"
        assert table.columns == {"id": "INT", "name": "STRING", "email": "STRING"}
        assert table.stream_config is None

    def test_parse_table_with_stream(self):
        """Проверка парсинга таблицы с конфигурацией стриминга."""
        parser = DSLParser()
        dsl_content = """
        DEFINE TABLE events (
            event_id STRING,
            user_id STRING,
            timestamp TIMESTAMP(3)
        ) WITH STREAM (
            time_attribute = timestamp,
            watermark = '5 seconds',
            allowed_lateness = '10 seconds'
        );
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.name == "events"
        assert table.stream_config is not None
        assert table.stream_config.time_attribute == "timestamp"
        assert table.stream_config.watermark == "5 seconds"
        assert table.stream_config.allowed_lateness == "10 seconds"

    def test_parse_model(self):
        """Проверка парсинга определения ML-модели."""
        parser = DSLParser()
        dsl_content = """
        DEFINE MODEL fraud_detector (
            path = 'hdfs:///models/fraud.pkl',
            input_schema = STRUCT<user_id STRING, amount DOUBLE>,
            output_schema = FLOAT
        );
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.models) == 1
        model = result.models[0]
        assert model.name == "fraud_detector"
        assert model.path == "hdfs:///models/fraud.pkl"
        assert "STRUCT" in model.input_schema
        assert model.output_schema == "FLOAT"

    def test_parse_graph(self):
        """Проверка парсинга определения графа."""
        parser = DSLParser()
        dsl_content = """
        DEFINE GRAPH social_network (
            vertices => users (user_id),
            edges => connections (src_id, dst_id)
        );
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.graphs) == 1
        graph = result.graphs[0]
        assert graph.name == "social_network"
        assert graph.vertices_table == "users"
        assert graph.vertices_id_col == "user_id"
        assert graph.edges_table == "connections"
        assert graph.edges_src_col == "src_id"
        assert graph.edges_dst_col == "dst_id"

    def test_parse_view(self):
        """Проверка парсинга CREATE VIEW."""
        parser = DSLParser()
        dsl_content = """
        CREATE VIEW active_users AS
        SELECT id, name FROM users WHERE status = 'active';
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.views) == 1
        assert "active_users" in result.views
        assert "SELECT id, name FROM users" in result.views["active_users"]

    def test_parse_insert(self):
        """Проверка парсинга INSERT INTO."""
        parser = DSLParser()
        dsl_content = """
        INSERT INTO output_table
        SELECT id, name FROM source_table WHERE id > 100;
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.inserts) == 1
        assert "INSERT INTO output_table SELECT" in result.inserts[0]

    def test_parse_comments_removed(self):
        """Проверка удаления комментариев."""
        parser = DSLParser()
        dsl_content = """
        -- Это комментарий
        DEFINE TABLE test (
            id INT  -- inline комментарий
        );
        /* Многострочный
           комментарий */
        """
        result = parser.parse_content(dsl_content)
        
        assert len(result.tables) == 1
        assert result.tables[0].name == "test"

    def test_parse_complex_types(self):
        """Проверка парсинга сложных типов данных."""
        parser = DSLParser()
        dsl_content = """
        DEFINE TABLE complex_table (
            id STRING,
            tags ARRAY<STRING>,
            metadata MAP<STRING, STRING>,
            nested STRUCT<a INT, b STRING>
        );
        """
        result = parser.parse_content(dsl_content)
        
        table = result.tables[0]
        assert table.columns["tags"] == "ARRAY<STRING>"
        assert table.columns["metadata"] == "MAP<STRING, STRING>"
        assert "STRUCT" in table.columns["nested"]

    def test_parse_full_dsl_file(self):
        """Проверка парсинга полного DSL файла из примеров."""
        parser = DSLParser()
        example_path = Path(__file__).parent.parent / "examples" / "pipeline.dsl"
        
        assert example_path.exists(), f"Файл {example_path} не найден"
        
        result = parser.parse_file(str(example_path))
        
        assert len(result.tables) >= 3
        assert len(result.models) >= 2
        assert len(result.views) >= 4
        assert len(result.inserts) >= 2


# ==============================================================================
# Тесты загрузчика конфигурации (core/config_loader.py)
# ==============================================================================

class TestConfigLoader:
    """Тесты для загрузчика YAML конфигурации."""

    def test_load_spark_batch_config(self):
        """Проверка загрузки конфигурации Spark Batch."""
        loader = ConfigLoader()
        config_path = Path(__file__).parent.parent / "examples" / "spark_batch_airflow.yaml"
        
        assert config_path.exists(), f"Файл {config_path} не найден"
        
        config = loader.load_file(str(config_path))
        
        assert config.meta["name"] == "fraud-detection-batch"
        assert config.target["platform"] == "spark"
        assert config.target["mode"] == "batch"
        assert config.execution.parallelism == 50
        assert loader.get_platform() == "spark"
        assert loader.get_mode() == "batch"

    def test_load_flink_streaming_config(self):
        """Проверка загрузки конфигурации Flink Streaming."""
        loader = ConfigLoader()
        config_path = Path(__file__).parent.parent / "examples" / "flink_streaming_k8s.yaml"
        
        assert config_path.exists(), f"Файл {config_path} не найден"
        
        config = loader.load_file(str(config_path))
        
        assert config.meta["name"] == "fraud-detection-streaming"
        assert config.target["platform"] == "flink"
        assert config.target["mode"] == "streaming"
        assert config.execution.checkpointing is not None
        assert config.execution.checkpointing.enabled is True
        assert config.execution.checkpointing.backend.value == "rocksdb"

    def test_validate_config(self):
        """Проверка валидации конфигурации."""
        loader = ConfigLoader()
        config_path = Path(__file__).parent.parent / "examples" / "spark_batch_airflow.yaml"
        
        config = loader.load_file(str(config_path))
        assert loader.validate() is True

    def test_validate_flink_requires_checkpointing(self):
        """Проверка что Flink streaming требует checkpointing."""
        loader = ConfigLoader()
        
        yaml_content = """
        meta:
          name: test
        target:
          platform: flink
          mode: streaming
        execution:
          parallelism: 1
          checkpointing:
            enabled: false
        connectors:
          sources: []
          sinks: []
        orchestration:
          type: native
        """
        
        config = loader.load_content(yaml_content)
        
        # Должна быть ошибка валидации
        with pytest.raises(ValueError, match="checkpointing"):
            loader.validate()

    def test_load_from_string(self):
        """Проверка загрузки конфигурации из строки."""
        loader = ConfigLoader()
        
        yaml_content = """
        meta:
          name: test-pipeline
          version: "1.0.0"
        target:
          platform: spark
          mode: batch
        execution:
          parallelism: 4
          memory:
            driver: "4g"
            executor: "8g"
        connectors:
          sources: []
          sinks: []
        orchestration:
          type: native
        """
        
        config = loader.load_content(yaml_content)
        
        assert config.meta["name"] == "test-pipeline"
        assert config.execution.memory.driver == "4g"


# ==============================================================================
# Тесты генератора Spark (compilers/spark_generator.py)
# ==============================================================================

class TestSparkGenerator:
    """Тесты для генератора кода Spark."""

    @pytest.fixture
    def sample_dsl(self):
        """Создание тестового DSL."""
        return DSLLayer(
            tables=[
                TableDefinition(
                    name="users",
                    columns={"id": "INT", "name": "STRING"},
                    stream_config=None
                )
            ],
            models=[],
            graphs=[],
            views={"active_users": "SELECT * FROM users WHERE active = true"},
            inserts=["INSERT INTO output SELECT * FROM users"]
        )

    @pytest.fixture
    def sample_config(self):
        """Создание тестовой конфигурации."""
        from platform_compiler.models.schemas import (
            PlatformConfig, ExecutionConfig, OrchestrationConfig, 
            OrchestrationType
        )
        
        return PlatformConfig(
            meta={"name": "test-pipeline", "version": "1.0.0"},
            target={"platform": "spark", "mode": "batch"},
            execution=ExecutionConfig(parallelism=4),
            connectors={"sources": [], "sinks": []},
            orchestration=OrchestrationConfig(type=OrchestrationType.NATIVE)
        )

    def test_generate_header(self, sample_dsl, sample_config):
        """Проверка генерации заголовка скрипта."""
        generator = SparkGenerator(sample_dsl, sample_config)
        header = generator._generate_header()
        
        assert "SPARK" in header
        assert "batch" in header

    def test_generate_source_ddl(self, sample_dsl, sample_config):
        """Проверка генерации DDL для источников."""
        generator = SparkGenerator(sample_dsl, sample_config)
        ddl = generator.generate_source_ddl()
        
        assert "SOURCE TABLES" in ddl
        assert "users" in ddl

    def test_generate_transformations(self, sample_dsl, sample_config):
        """Проверка генерации трансформаций."""
        generator = SparkGenerator(sample_dsl, sample_config)
        transformations = generator.generate_transformations()
        
        assert "CREATE OR REPLACE TEMPORARY VIEW active_users" in transformations
        assert "INSERT INTO output" in transformations

    def test_translate_window_functions(self, sample_dsl, sample_config):
        """Проверка трансляции оконных функций."""
        generator = SparkGenerator(sample_dsl, sample_config)
        
        query = "SELECT TUMBLE_START(time, INTERVAL '5' MINUTES) FROM events"
        translated = generator._translate_to_spark(query)
        
        assert "window.start" in translated

    def test_format_complex_types(self, sample_dsl, sample_config):
        """Проверка форматирования сложных типов."""
        generator = SparkGenerator(sample_dsl, sample_config)
        
        assert generator._format_type("ARRAY<INT>") == "ARRAY<INT>"
        assert generator._format_type("MAP<STRING, INT>") == "MAP<STRING, INT>"


# ==============================================================================
# Тесты генератора Flink (compilers/flink_generator.py)
# ==============================================================================

class TestFlinkGenerator:
    """Тесты для генератора кода Flink."""

    @pytest.fixture
    def sample_dsl_with_stream(self):
        """Создание тестового DSL со стримингом."""
        return DSLLayer(
            tables=[
                TableDefinition(
                    name="events",
                    columns={"id": "INT", "timestamp": "TIMESTAMP(3)"},
                    stream_config=StreamConfig(
                        time_attribute="timestamp",
                        watermark="5 seconds"
                    )
                )
            ],
            models=[],
            graphs=[],
            views={},
            inserts=[]
        )

    @pytest.fixture
    def sample_flink_config(self):
        """Создание тестовой конфигурации Flink."""
        from platform_compiler.models.schemas import (
            PlatformConfig, ExecutionConfig, CheckpointConfig,
            OrchestrationConfig, OrchestrationType
        )
        
        return PlatformConfig(
            meta={"name": "flink-test", "version": "1.0.0"},
            target={"platform": "flink", "mode": "streaming"},
            execution=ExecutionConfig(
                parallelism=4,
                checkpointing=CheckpointConfig(
                    enabled=True,
                    interval="60s",
                    backend="rocksdb"
                )
            ),
            connectors={"sources": [], "sinks": []},
            orchestration=OrchestrationConfig(type=OrchestrationType.KUBERNETES)
        )

    def test_generate_kafka_source_with_watermark(self, sample_dsl_with_stream, sample_flink_config):
        """Проверка генерации Kafka источника с watermark."""
        generator = FlinkGenerator(sample_dsl_with_stream, sample_flink_config)
        
        config = {
            "bootstrap_servers": ["localhost:9092"],
            "topic": "events-topic",
            "format": "json"
        }
        
        ddl = generator._generate_kafka_source(
            sample_dsl_with_stream.tables[0], 
            config
        )
        
        assert "WATERMARK FOR timestamp" in ddl
        assert "kafka" in ddl.lower()

    def test_generate_k8s_manifest(self, sample_dsl_with_stream, sample_flink_config):
        """Проверка генерации Kubernetes манифеста."""
        generator = FlinkGenerator(sample_dsl_with_stream, sample_flink_config)
        manifest = generator.generate_k8s_manifest()
        
        assert "FlinkDeployment" in manifest
        assert "flink-test" in manifest
        assert "rocksdb" in manifest

    def test_translate_flink_functions(self, sample_dsl_with_stream, sample_flink_config):
        """Проверка трансляции функций Flink."""
        generator = FlinkGenerator(sample_dsl_with_stream, sample_flink_config)
        
        query = "SELECT HOP_START(time, INTERVAL '1' MINUTE) FROM events"
        translated = generator._translate_to_flink(query)
        
        assert "HOP_START" in translated


# ==============================================================================
# Тесты основного компилятора (compiler.py)
# ==============================================================================

class TestPlatformCompiler:
    """Тесты для основного компилятора."""

    def test_compile_from_strings_spark(self):
        """Проверка компиляции из строк для Spark."""
        compiler = PlatformCompiler()
        
        dsl_content = """
        DEFINE TABLE input_data (
            id INT,
            value STRING
        );
        
        CREATE VIEW processed AS
        SELECT id, UPPER(value) as upper_value FROM input_data;
        
        INSERT INTO output_data
        SELECT * FROM processed;
        """
        
        config_content = """
        meta:
          name: test-spark-job
          version: "1.0.0"
        target:
          platform: spark
          mode: batch
        execution:
          parallelism: 2
        connectors:
          sources: []
          sinks: []
        orchestration:
          type: native
        """
        
        outputs = compiler.compile_from_strings(dsl_content, config_content)
        
        assert len(outputs) >= 1
        assert any(".sql" in key for key in outputs.keys())
        assert "processed" in outputs[list(outputs.keys())[0]]

    def test_compile_from_strings_flink(self):
        """Проверка компиляции из строк для Flink."""
        compiler = PlatformCompiler()
        
        dsl_content = """
        DEFINE TABLE stream_input (
            id INT,
            timestamp TIMESTAMP(3)
        ) WITH STREAM (
            time_attribute = timestamp,
            watermark = '5 seconds'
        );
        
        CREATE VIEW filtered AS
        SELECT * FROM stream_input WHERE id > 0;
        """
        
        config_content = """
        meta:
          name: test-flink-job
          version: "1.0.0"
        target:
          platform: flink
          mode: streaming
        execution:
          parallelism: 2
          checkpointing:
            enabled: true
            interval: "60s"
        connectors:
          sources: []
          sinks: []
        orchestration:
          type: kubernetes
          image: flink:1.17
        """
        
        outputs = compiler.compile_from_strings(dsl_content, config_content)
        
        assert len(outputs) >= 1
        sql_output = list(outputs.values())[0]
        assert "WATERMARK" in sql_output or "stream_input" in sql_output

    def test_compile_example_files(self):
        """Проверка компиляции файлов из примеров."""
        compiler = PlatformCompiler()
        
        dsl_path = Path(__file__).parent.parent / "examples" / "pipeline.dsl"
        config_path = Path(__file__).parent.parent / "examples" / "spark_batch_airflow.yaml"
        output_dir = Path(__file__).parent.parent / "output" / "test"
        
        assert dsl_path.exists()
        assert config_path.exists()
        
        outputs = compiler.compile(
            str(dsl_path),
            str(config_path),
            str(output_dir)
        )
        
        assert len(outputs) >= 1
        assert output_dir.exists()

    def test_unsupported_platform_error(self):
        """Проверка ошибки при неподдерживаемой платформе."""
        compiler = PlatformCompiler()
        
        dsl_content = "DEFINE TABLE test (id INT);"
        config_content = """
        meta:
          name: test
        target:
          platform: unsupported_platform
          mode: batch
        execution:
          parallelism: 1
        connectors:
          sources: []
          sinks: []
        orchestration:
          type: native
        """
        
        with pytest.raises(ValueError, match="Unsupported platform"):
            compiler.compile_from_strings(dsl_content, config_content)


# ==============================================================================
# Интеграционные тесты
# ==============================================================================

class TestIntegration:
    """Интеграционные тесты полного цикла компиляции."""

    def test_full_spark_batch_pipeline(self):
        """Полный цикл компиляции Spark Batch пайплайна."""
        compiler = PlatformCompiler()
        
        dsl_path = Path(__file__).parent.parent / "examples" / "pipeline.dsl"
        config_path = Path(__file__).parent.parent / "examples" / "spark_batch_airflow.yaml"
        output_dir = Path(__file__).parent.parent / "output" / "spark_test"
        
        outputs = compiler.compile(
            str(dsl_path),
            str(config_path),
            str(output_dir)
        )
        
        # Проверяем что сгенерирован SQL файл
        sql_files = [f for f in outputs.keys() if f.endswith('.sql')]
        assert len(sql_files) > 0
        
        # Проверяем что сгенерирован DAG для Airflow
        dag_files = [f for f in outputs.keys() if f.startswith('dag_') and f.endswith('.py')]
        assert len(dag_files) > 0
        
        # Проверяем содержимое SQL
        sql_content = outputs[sql_files[0]]
        assert "SOURCE TABLES" in sql_content
        assert "SINK TABLES" in sql_content
        assert "TRANSFORMATIONS" in sql_content

    def test_full_flink_streaming_pipeline(self):
        """Полный цикл компиляции Flink Streaming пайплайна."""
        compiler = PlatformCompiler()
        
        dsl_path = Path(__file__).parent.parent / "examples" / "pipeline.dsl"
        config_path = Path(__file__).parent.parent / "examples" / "flink_streaming_k8s.yaml"
        output_dir = Path(__file__).parent.parent / "output" / "flink_test"
        
        outputs = compiler.compile(
            str(dsl_path),
            str(config_path),
            str(output_dir)
        )
        
        # Проверяем что сгенерирован SQL файл
        sql_files = [f for f in outputs.keys() if f.endswith('.sql')]
        assert len(sql_files) > 0
        
        # Проверяем что сгенерирован K8s манифест
        k8s_files = [f for f in outputs.keys() if f.startswith('deployment_') and f.endswith('.yaml')]
        assert len(k8s_files) > 0
        
        # Проверяем содержимое K8s манифеста
        k8s_content = outputs[k8s_files[0]]
        assert "FlinkDeployment" in k8s_content
        assert "flink-jobs" in k8s_content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
