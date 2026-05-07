"""
Flink SQL Code Generator

Generates Flink SQL code for Streaming mode.
Supports Kubernetes deployment via Flink Operator.
"""

from typing import Dict, Any, List

from .base import BaseCodeGenerator
from ..models.schemas import DSLLayer, PlatformConfig


class FlinkGenerator(BaseCodeGenerator):
    """Code generator for Apache Flink (Streaming)."""

    def _get_platform_name(self) -> str:
        return "flink"

    def generate_source_ddl(self) -> str:
        """Generate Flink source table DDL."""
        lines = ["-- ==========================================",
                 "-- SOURCE TABLES",
                 "-- =========================================="]
        
        sources = self.config.connectors.get('sources', [])
        
        for table in self.dsl.tables:
            # Find matching source connector
            connector = None
            for src in sources:
                if src.get('name') == table.name:
                    connector = src
                    break
            
            if connector:
                conn_type = connector.get('type', 'kafka')
                conn_config = connector.get('config', {})
                
                if conn_type == 'kafka':
                    ddl = self._generate_kafka_source(table, conn_config)
                elif conn_type in ['hdfs', 's3']:
                    ddl = self._generate_file_source(table, conn_config)
                else:
                    ddl = f"-- Unsupported source type: {conn_type}"
                
                lines.append(ddl)
            else:
                lines.append(self._create_flink_table(table))
        
        return '\n\n'.join(lines)

    def generate_sink_ddl(self) -> str:
        """Generate Flink sink table DDL."""
        lines = ["-- ==========================================",
                 "-- SINK TABLES",
                 "-- =========================================="]
        
        sinks = self.config.connectors.get('sinks', [])
        
        for sink in sinks:
            sink_name = sink.get('name', '')
            conn_type = sink.get('type', 'kafka')
            conn_config = sink.get('config', {})
            semantics = sink.get('semantics', 'at_least_once')
            
            if conn_type == 'kafka':
                ddl = self._generate_kafka_sink(sink_name, conn_config, semantics)
            elif conn_type in ['hdfs', 's3']:
                ddl = self._generate_file_sink(sink_name, conn_config)
            else:
                ddl = f"-- Unsupported sink type: {conn_type}"
            
            lines.append(ddl)
        
        return '\n\n'.join(lines)

    def generate_transformations(self) -> str:
        """Generate transformation logic (views and queries)."""
        lines = ["-- ==========================================",
                 "-- TRANSFORMATIONS",
                 "-- =========================================="]
        
        # Generate views
        for view_name, query in self.dsl.views.items():
            translated_query = self._translate_to_flink(query)
            lines.append(f"CREATE VIEW {view_name} AS\n{translated_query};")
        
        # Generate insert statements
        for insert in self.dsl.inserts:
            translated = self._translate_to_flink(insert)
            lines.append(translated + ";")
        
        return '\n\n'.join(lines)

    def generate_udf_registrations(self) -> str:
        """Generate UDF registration code for Flink."""
        lines = ["-- ==========================================",
                 "-- UDF/UDTF REGISTRATIONS",
                 "-- =========================================="]
        
        for model in self.dsl.models:
            lines.append(f"-- Model: {model.name}")
            lines.append(f"-- Path: {model.path}")
            lines.append(f"-- Input: {model.input_schema}, Output: {model.output_schema}")
            
            # Flink Python UDF registration
            lines.append(f"""
-- Create Python UDF for {model.name}
-- CREATE TEMPORARY FUNCTION {model.name} 
-- AS 'udfs.{model.name}_udf' 
-- LANGUAGE PYTHON;
""")
        
        return '\n\n'.join(lines)

    def generate_k8s_manifest(self) -> str:
        """Generate Kubernetes manifest for Flink deployment."""
        meta = self.config.meta
        k8s = self.config.orchestration.kubernetes
        exec_cfg = self.config.execution
        
        job_name = meta.get('name', 'flink-pipeline').replace('-', '-')
        namespace = k8s.namespace if k8s else 'default'
        image = k8s.image if k8s else 'flink:1.17'
        
        parallelism = exec_cfg.parallelism
        jobmanager_memory = exec_cfg.memory.jobmanager if exec_cfg.memory else '2048m'
        taskmanager_memory = exec_cfg.memory.taskmanager if exec_cfg.memory else '4096m'
        
        checkpoint_path = ''
        if exec_cfg.checkpointing and exec_cfg.checkpointing.path:
            checkpoint_path = exec_cfg.checkpointing.path
        
        manifest = f'''apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: {job_name}
  namespace: {namespace}
spec:
  image: {image}
  flinkVersion: v1_17
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "{parallelism}"
    state.backend: rocksdb
    state.checkpoints.dir: "{checkpoint_path or 'file:///tmp/flink-checkpoints'}"
    execution.checkpointing.interval: "{exec_cfg.checkpointing.interval if exec_cfg.checkpointing else '60000'}ms"
  serviceAccount: {k8s.service_account if k8s and k8s.service_account else 'flink'}
  jobManager:
    resource:
      memory: {jobmanager_memory}
      cpu: {exec_cfg.cpu.cores if exec_cfg.cpu else 1}
  taskManager:
    resource:
      memory: {taskmanager_memory}
      cpu: {exec_cfg.cpu.cores if exec_cfg.cpu else 2}
  job:
    jarURI: local:///opt/flink/usrlib/{job_name}.jar
    parallelism: {parallelism}
    upgradeMode: stateless
'''
        return manifest

    def _generate_kafka_source(self, table, config: Dict[str, Any]) -> str:
        """Generate Kafka source DDL for Flink SQL."""
        columns = ',\n  '.join(f'{k} {self._format_type(v)}' for k, v in table.columns.items())
        servers = ','.join(config.get('bootstrap_servers', ['localhost:9092']))
        topic = config.get('topic', table.name)
        format_type = config.get('format', 'json').upper()
        startup_mode = config.get('scan_startup_mode', 'latest-offset')
        
        watermark_clause = ''
        if table.stream_config and table.stream_config.time_attribute:
            wm_interval = table.stream_config.watermark or '5 seconds'
            time_attr = table.stream_config.time_attribute
            watermark_clause = f',\n  WATERMARK FOR {time_attr} AS {time_attr} - INTERVAL \'{wm_interval}\''
        
        return f'''-- Kafka Source: {table.name}
CREATE TABLE {table.name} (
  {columns}{watermark_clause}
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = '{servers}',
  'topic' = '{topic}',
  'scan.startup.mode' = '{startup_mode}',
  'format' = '{format_type.lower()}'
);'''

    def _generate_file_source(self, table, config: Dict[str, Any]) -> str:
        """Generate file-based source DDL for Flink."""
        columns = ',\n  '.join(f'{k} {self._format_type(v)}' for k, v in table.columns.items())
        path = config.get('path', f'/data/{table.name}')
        format_type = config.get('format', 'parquet').upper()
        
        return f'''-- File Source: {table.name}
CREATE TABLE {table.name} (
  {columns}
) WITH (
  'connector' = 'filesystem',
  'path' = '{path}',
  'format' = '{format_type.lower()}'
);'''

    def _generate_kafka_sink(self, name: str, config: Dict[str, Any], semantics: str) -> str:
        """Generate Kafka sink DDL for Flink SQL."""
        # For sinks, we typically use INSERT INTO, so define as a regular table
        # The actual sink is defined by the INSERT statement
        servers = ','.join(config.get('bootstrap_servers', ['localhost:9092']))
        topic = config.get('topic', name)
        
        semantic_value = 'exactly-once' if semantics == 'exactly_once' else 'at-least-once'
        
        return f'''-- Kafka Sink: {name}
-- Use INSERT INTO to write to this sink
-- Configuration:
--   bootstrap.servers: {servers}
--   topic: {topic}
--   semantic: {semantic_value}'''

    def _generate_file_sink(self, name: str, config: Dict[str, Any]) -> str:
        """Generate file-based sink DDL for Flink."""
        path = config.get('path', f'/data/{name}')
        format_type = config.get('format', 'parquet').upper()
        partition_by = config.get('partition_by', [])
        
        partition_clause = ''
        if partition_by:
            partition_clause = f",\n  'partition' = '{','.join(partition_by)}'"
        
        return f'''-- File Sink: {name}
-- Use INSERT INTO to write to this sink
-- Configuration:
--   path: {path}
--   format: {format_type.lower()}{partition_clause}'''

    def _create_flink_table(self, table) -> str:
        """Create Flink table from schema definition."""
        columns = ',\n  '.join(f'{k} {self._format_type(v)}' for k, v in table.columns.items())
        
        watermark_clause = ''
        if table.stream_config and table.stream_config.time_attribute:
            wm_interval = table.stream_config.watermark or '5 seconds'
            time_attr = table.stream_config.time_attribute
            watermark_clause = f',\n  WATERMARK FOR {time_attr} AS {time_attr} - INTERVAL \'{wm_interval}\''
        
        return f'''-- Table: {table.name}
CREATE TABLE {table.name} (
  {columns}{watermark_clause}
);'''

    def _translate_to_flink(self, query: str) -> str:
        """Translate DSL-specific syntax to Flink SQL."""
        # Replace DSL window functions with Flink equivalents
        translations = [
            ('TUMBLE_START(', 'TUMBLE_START('),
            ('TUMBLE_END(', 'TUMBLE_END('),
            ('TUMBLE(', 'TUMBLE('),
            ('HOP(', 'HOP('),
            ('SESSION(', 'SESSION('),
            ('CROSS JOIN LATERAL TABLE(', 'CROSS JOIN LATERAL TABLE('),
            ('APPLY_MODEL(', 'apply_model('),
            ('ENCODE_TEXT(', 'encode_text('),
            ('LLM_GENERATE(', 'llm_generate('),
            ('VECTOR_SEARCH(', 'vector_search('),
        ]
        
        result = query
        for dsl_fn, flink_fn in translations:
            result = result.replace(dsl_fn, flink_fn)
        
        return result
