"""
Модели данных для Platform Compiler.

Определяет структуры данных для:
- SQL-DSL (бизнес-логика пайплайна)
- Конфигурации платформы (инфраструктура)
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ==============================================================================
# Модели SQL-DSL (уровень бизнес-логики)
# ==============================================================================

class StreamConfig(BaseModel):
    """
    Конфигурация для стриминговых таблиц.
    
    Attributes:
        time_attribute: Имя колонки времени для оконных операций
        watermark: Допустимая задержка данных (например, '5 seconds')
        allowed_lateness: Дополнительная задержка для поздних данных
    """
    time_attribute: str
    watermark: Optional[str] = None
    allowed_lateness: Optional[str] = None


class TableDefinition(BaseModel):
    """
    Определение таблицы (DEFINE TABLE).
    
    Attributes:
        name: Имя таблицы
        columns: Словарь {имя_колонки: тип_данных}
        stream_config: Настройки стриминга (опционально)
    """
    name: str
    columns: Dict[str, str]
    stream_config: Optional[StreamConfig] = None


class ModelDefinition(BaseModel):
    """
    Определение ML-модели (DEFINE MODEL).
    
    Attributes:
        name: Имя модели
        path: Путь к файлу модели
        input_schema: Схема входных данных
        output_schema: Схема выходных данных
    """
    name: str
    path: str
    input_schema: str
    output_schema: str


class GraphDefinition(BaseModel):
    """
    Определение графа (DEFINE GRAPH).
    
    Attributes:
        name: Имя графа
        vertices_table: Таблица вершин
        vertices_id_col: Колонка ID вершины
        edges_table: Таблица рёбер
        edges_src_col: Колонка ID источника
        edges_dst_col: Колонка ID назначения
    """
    name: str
    vertices_table: str
    vertices_id_col: str
    edges_table: str
    edges_src_col: str
    edges_dst_col: str


class DSLLayer(BaseModel):
    """
    Полное определение пайплайна на SQL-DSL.
    
    Attributes:
        tables: Список определённых таблиц
        models: Список ML-моделей
        graphs: Список графов
        views: Словарь {имя_представления: SQL-запрос}
        inserts: Список INSERT-операций
    """
    tables: List[TableDefinition] = []
    models: List[ModelDefinition] = []
    graphs: List[GraphDefinition] = []
    views: Dict[str, str] = {}
    inserts: List[str] = []



# ==============================================================================
# Модели конфигурации платформы (уровень инфраструктуры)
# ==============================================================================

class CheckpointBackend(str, Enum):
    """Бэкенд для хранения чекпоинтов."""
    ROCKSDB = "rocksdb"
    FS = "fs"


class CheckpointConfig(BaseModel):
    """
    Конфигурация чекпоинтов.
    
    Attributes:
        enabled: Включено ли чекпоинтирование
        interval: Интервал между чекпоинтами
        backend: Бэкенд хранения
        path: Путь к хранилищу чекпоинтов
        num_retained: Количество сохраняемых чекпоинтов
    """
    enabled: bool = False
    interval: Optional[str] = None
    backend: Optional[CheckpointBackend] = None
    path: Optional[str] = None
    num_retained: Optional[int] = None


class MemoryConfig(BaseModel):
    """
    Конфигурация памяти.
    
    Attributes:
        driver: Память драйвера (Spark)
        executor: Память исполнителя (Spark)
        taskmanager: Память TaskManager (Flink)
        jobmanager: Память JobManager (Flink)
        per_slot: Память на слот
    """
    driver: Optional[str] = None
    executor: Optional[str] = None
    taskmanager: Optional[str] = None
    jobmanager: Optional[str] = None
    per_slot: Optional[str] = None


class CPUConfig(BaseModel):
    """
    Конфигурация CPU.
    
    Attributes:
        cores: Количество ядер
    """
    cores: int = 2


class ExecutionConfig(BaseModel):
    """
    Конфигурация движка выполнения.
    
    Attributes:
        parallelism: Уровень параллелизма
        checkpointing: Настройки чекпоинтов
        memory: Настройки памяти
        cpu: Настройки CPU
    """
    parallelism: int = 1
    checkpointing: Optional[CheckpointConfig] = None
    memory: Optional[MemoryConfig] = None
    cpu: Optional[CPUConfig] = None


class ConnectorType(str, Enum):
    """Типы коннекторов данных."""
    KAFKA = "kafka"
    HDFS = "hdfs"
    S3 = "s3"
    YT_TABLE = "yt_table"


class KafkaConfig(BaseModel):
    """
    Конфигурация Kafka коннектора.
    
    Attributes:
        bootstrap_servers: Серверы Kafka
        topic: Имя топика
        format: Формат данных
        properties: Дополнительные свойства
        transactional_id: ID транзакции
        scan_startup_mode: Режим запуска сканирования
        sink_semantic: Семантика доставки
    """
    bootstrap_servers: List[str]
    topic: str
    format: str = "json"
    properties: Optional[Dict[str, Any]] = None
    transactional_id: Optional[str] = None
    scan_startup_mode: Optional[str] = None
    sink_semantic: Optional[str] = None


class FileSystemConfig(BaseModel):
    """
    Конфигурация файлового коннектора.
    
    Attributes:
        path: Путь к данным
        format: Формат файлов
        partition_by: Колонки для партиционирования
        overwrite: Перезаписывать ли данные
        append: Добавлять ли данные
    """
    path: str
    format: str = "parquet"
    partition_by: Optional[List[str]] = None
    overwrite: Optional[bool] = None
    append: Optional[bool] = None


class YTTableConfig(BaseModel):
    """
    Конфигурация таблицы Yandex Tsaurus.
    
    Attributes:
        cluster: Имя кластера
        table_path: Путь к таблице
        schema_inference: Автоопределение схемы
    """
    cluster: str
    table_path: str
    schema_inference: Optional[bool] = None


class SourceConnector(BaseModel):
    """
    Коннектор источника данных.
    
    Attributes:
        name: Имя коннектора
        type: Тип коннектора
        config: Конфигурация
    """
    name: str
    type: ConnectorType
    config: Dict[str, Any]


class SinkSemantics(str, Enum):
    """Семантика доставки в sink."""
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    AT_MOST_ONCE = "at_most_once"


class SinkConnector(BaseModel):
    """
    Коннектор приёмника данных.
    
    Attributes:
        name: Имя коннектора
        type: Тип коннектора
        semantics: Семантика доставки
        config: Конфигурация
    """
    name: str
    type: ConnectorType
    semantics: SinkSemantics = SinkSemantics.AT_LEAST_ONCE
    config: Dict[str, Any]


class ModelRuntime(str, Enum):
    """Среда выполнения моделей."""
    PYTHON = "python"
    JAVA = "java"


class CachePolicy(str, Enum):
    """Стратегия кеширования моделей."""
    LAZY_LOAD = "lazy_load"
    EAGER_LOAD = "eager_load"


class ModelConfig(BaseModel):
    """
    Конфигурация ML-модели.
    
    Attributes:
        name: Имя модели
        runtime: Среда выполнения
        requirements: Зависимости
        cache_policy: Стратегия кеширования
        storage_path: Путь к хранилищу
    """
    name: str
    runtime: ModelRuntime = ModelRuntime.PYTHON
    requirements: List[str] = []
    cache_policy: CachePolicy = CachePolicy.LAZY_LOAD
    storage_path: str


class OrchestrationType(str, Enum):
    """Платформы оркестрации."""
    AIRFLOW = "airflow"
    KUBERNETES = "kubernetes"
    NATIVE = "native"


class ResourceRequirements(BaseModel):
    """
    Требования к ресурсам в Kubernetes.
    
    Attributes:
        cpu: Требуемый CPU
        memory: Требуемая память
    """
    cpu: str
    memory: str


class ResourceConfig(BaseModel):
    """
    Конфигурация ресурсов для оркестрации.
    
    Attributes:
        requests: Запрашиваемые ресурсы
        limits: Предельные значения ресурсов
    """
    requests: Optional[ResourceRequirements] = None
    limits: Optional[ResourceRequirements] = None


class AirflowConfig(BaseModel):
    """
    Конфигурация Airflow.
    
    Attributes:
        schedule_interval: Расписание выполнения
        start_date: Дата начала
        catchup: Выполнять ли пропущенные запуски
        retries: Количество попыток
        retry_delay: Задержка между попытками
        operator_config: Настройки оператора
    """
    schedule_interval: str = "@daily"
    start_date: str = "2024-01-01"
    catchup: bool = False
    retries: int = 2
    retry_delay: str = "5m"
    operator_config: Optional[Dict[str, Any]] = None


class KubernetesConfig(BaseModel):
    """
    Конфигурация Kubernetes.
    
    Attributes:
        namespace: Имя namespace
        service_account: Service account
        image: Docker образ
        crd_type: Тип CRD (например, FlinkDeployment)
        resources: Требования к ресурсам
    """
    namespace: str = "default"
    service_account: Optional[str] = None
    image: str
    crd_type: Optional[str] = None
    resources: Optional[ResourceConfig] = None


class OrchestrationConfig(BaseModel):
    """
    Конфигурация оркестрации.
    
    Attributes:
        type: Тип оркестратора
        image: Docker образ
        airflow: Настройки Airflow
        kubernetes: Настройки Kubernetes
        env_vars: Переменные окружения
    """
    type: OrchestrationType
    image: Optional[str] = None
    airflow: Optional[AirflowConfig] = None
    kubernetes: Optional[KubernetesConfig] = None
    env_vars: Optional[Dict[str, str]] = None


class PlatformConfig(BaseModel):
    """
    Полная конфигурация платформы.
    
    Attributes:
        meta: Метаданные (name, version, owner, description)
        target: Целевая платформа и режим
        execution: Настройки выполнения
        connectors: Коннекторы (sources, sinks)
        models: ML-модели
        orchestration: Настройки оркестрации
    """
    meta: Dict[str, str]
    target: Dict[str, str]
    execution: ExecutionConfig
    connectors: Dict[str, List[Dict[str, Any]]]
    models: List[ModelConfig] = []
    orchestration: OrchestrationConfig
