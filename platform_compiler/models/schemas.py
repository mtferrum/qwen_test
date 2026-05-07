"""
Core models for the Platform Compiler.

Based on the SQL-DSL and Platform Configuration specifications from chat-export-1778167073196.json
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ============================================================================
# SQL-DSL Models (Business Logic Layer)
# ============================================================================

class DataType(str, Enum):
    """Supported SQL data types."""
    STRING = "STRING"
    INT = "INT"
    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"


class StreamConfig(BaseModel):
    """Configuration for streaming tables."""
    time_attribute: str
    watermark: Optional[str] = None
    allowed_lateness: Optional[str] = None


class TableDefinition(BaseModel):
    """DEFINE TABLE construct."""
    name: str
    columns: Dict[str, str]  # column_name -> data_type
    stream_config: Optional[StreamConfig] = None


class ModelDefinition(BaseModel):
    """DEFINE MODEL construct."""
    name: str
    path: str
    input_schema: str
    output_schema: str


class GraphDefinition(BaseModel):
    """DEFINE GRAPH construct."""
    name: str
    vertices_table: str
    vertices_id_col: str
    edges_table: str
    edges_src_col: str
    edges_dst_col: str


class DSLLayer(BaseModel):
    """Complete SQL-DSL pipeline definition."""
    tables: List[TableDefinition] = []
    models: List[ModelDefinition] = []
    graphs: List[GraphDefinition] = []
    views: Dict[str, str] = {}  # view_name -> sql_query
    inserts: List[str] = []


# ============================================================================
# Platform Configuration Models (Infrastructure Layer)
# ============================================================================

class PlatformType(str, Enum):
    """Target execution platform."""
    SPARK = "spark"
    FLINK = "flink"
    YQL = "yql"


class ExecutionMode(str, Enum):
    """Processing mode."""
    BATCH = "batch"
    STREAMING = "streaming"


class CheckpointBackend(str, Enum):
    """Checkpoint storage backend."""
    ROCKSDB = "rocksdb"
    FS = "fs"


class CheckpointConfig(BaseModel):
    """Checkpointing configuration."""
    enabled: bool = False
    interval: Optional[str] = None
    backend: Optional[CheckpointBackend] = None
    path: Optional[str] = None
    num_retained: Optional[int] = None


class MemoryConfig(BaseModel):
    """Memory configuration."""
    driver: Optional[str] = None
    executor: Optional[str] = None
    taskmanager: Optional[str] = None
    jobmanager: Optional[str] = None
    per_slot: Optional[str] = None


class CPUConfig(BaseModel):
    """CPU configuration."""
    cores: int = 2


class ExecutionConfig(BaseModel):
    """Execution engine configuration."""
    parallelism: int = 1
    checkpointing: Optional[CheckpointConfig] = None
    memory: Optional[MemoryConfig] = None
    cpu: Optional[CPUConfig] = None


class ConnectorType(str, Enum):
    """Data connector types."""
    KAFKA = "kafka"
    HDFS = "hdfs"
    S3 = "s3"
    YT_TABLE = "yt_table"


class KafkaConfig(BaseModel):
    """Kafka connector configuration."""
    bootstrap_servers: List[str]
    topic: str
    format: str = "json"
    properties: Optional[Dict[str, Any]] = None
    transactional_id: Optional[str] = None
    scan_startup_mode: Optional[str] = None
    sink_semantic: Optional[str] = None


class FileSystemConfig(BaseModel):
    """File system connector configuration."""
    path: str
    format: str = "parquet"
    partition_by: Optional[List[str]] = None
    overwrite: Optional[bool] = None
    append: Optional[bool] = None


class YTTableConfig(BaseModel):
    """Yandex Tsaurus table configuration."""
    cluster: str
    table_path: str
    schema_inference: Optional[bool] = None


class SourceConnector(BaseModel):
    """Source data connector."""
    name: str
    type: ConnectorType
    config: Dict[str, Any]


class SinkSemantics(str, Enum):
    """Sink delivery semantics."""
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    AT_MOST_ONCE = "at_most_once"


class SinkConnector(BaseModel):
    """Sink data connector."""
    name: str
    type: ConnectorType
    semantics: SinkSemantics = SinkSemantics.AT_LEAST_ONCE
    config: Dict[str, Any]


class ModelRuntime(str, Enum):
    """Model runtime environment."""
    PYTHON = "python"
    JAVA = "java"


class CachePolicy(str, Enum):
    """Model caching strategy."""
    LAZY_LOAD = "lazy_load"
    EAGER_LOAD = "eager_load"


class ModelConfig(BaseModel):
    """ML model configuration."""
    name: str
    runtime: ModelRuntime = ModelRuntime.PYTHON
    requirements: List[str] = []
    cache_policy: CachePolicy = CachePolicy.LAZY_LOAD
    storage_path: str


class OrchestrationType(str, Enum):
    """Orchestration platform."""
    AIRFLOW = "airflow"
    KUBERNETES = "kubernetes"
    NATIVE = "native"


class ResourceRequirements(BaseModel):
    """K8s resource requirements."""
    cpu: str
    memory: str


class ResourceConfig(BaseModel):
    """Resource configuration for orchestration."""
    requests: Optional[ResourceRequirements] = None
    limits: Optional[ResourceRequirements] = None


class AirflowConfig(BaseModel):
    """Airflow-specific orchestration config."""
    schedule_interval: str = "@daily"
    start_date: str = "2024-01-01"
    catchup: bool = False
    retries: int = 2
    retry_delay: str = "5m"
    operator_config: Optional[Dict[str, Any]] = None


class KubernetesConfig(BaseModel):
    """Kubernetes-specific orchestration config."""
    namespace: str = "default"
    service_account: Optional[str] = None
    image: str
    crd_type: Optional[str] = None  # e.g., "FlinkDeployment"
    resources: Optional[ResourceConfig] = None


class OrchestrationConfig(BaseModel):
    """Orchestration configuration."""
    type: OrchestrationType
    image: Optional[str] = None
    airflow: Optional[AirflowConfig] = None
    kubernetes: Optional[KubernetesConfig] = None
    env_vars: Optional[Dict[str, str]] = None


class PlatformConfig(BaseModel):
    """Complete platform configuration."""
    meta: Dict[str, str]  # name, version, owner, description
    target: Dict[str, str]  # platform, mode
    execution: ExecutionConfig
    connectors: Dict[str, List[Dict[str, Any]]]  # sources, sinks
    models: List[ModelConfig] = []
    orchestration: OrchestrationConfig
