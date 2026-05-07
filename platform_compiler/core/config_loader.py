"""
Загрузчик конфигурации платформы.

Загружает и проверяет YAML-файлы конфигурации platform.yaml.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional

from ..models.schemas import (
    PlatformConfig, ExecutionConfig, CheckpointConfig, MemoryConfig,
    CPUConfig, OrchestrationConfig, AirflowConfig, KubernetesConfig,
    ResourceConfig, ResourceRequirements, ModelConfig, CachePolicy,
    ModelRuntime
)


class ConfigLoader:
    """
    Загрузчик для YAML-файлов конфигурации платформы.
    
    Пример использования:
        loader = ConfigLoader()
        config = loader.load_file('platform.yaml')
        loader.validate()
    """

    def __init__(self):
        """Инициализация загрузчика."""
        self.config: Optional[PlatformConfig] = None

    def load_file(self, file_path: str) -> PlatformConfig:
        """
        Загрузка конфигурации из YAML файла.
        
        Args:
            file_path: Путь к файлу конфигурации
            
        Returns:
            Объект PlatformConfig
        """
        content = Path(file_path).read_text()
        return self.load_content(content)

    def load_content(self, content: str) -> PlatformConfig:
        """
        Загрузка конфигурации из YAML строки.
        
        Args:
            content: Строка с YAML содержимым
            
        Returns:
            Объект PlatformConfig
        """
        data = yaml.safe_load(content)
        self.config = self._parse_config(data)
        return self.config

    def _parse_config(self, data: Dict[str, Any]) -> PlatformConfig:
        """
        Разбор сырого словаря в модель PlatformConfig.
        
        Args:
            data: Словарь с данными конфигурации
            
        Returns:
            Объект PlatformConfig
        """
        # Парсим конфигурацию выполнения
        exec_data = data.get('execution', {})
        checkpoint_data = exec_data.get('checkpointing', {})
        memory_data = exec_data.get('memory', {})
        cpu_data = exec_data.get('cpu', {})
        
        execution = ExecutionConfig(
            parallelism=exec_data.get('parallelism', 1),
            checkpointing=CheckpointConfig(
                enabled=checkpoint_data.get('enabled', False),
                interval=checkpoint_data.get('interval'),
                backend=checkpoint_data.get('backend'),
                path=checkpoint_data.get('path'),
                num_retained=checkpoint_data.get('num_retained')
            ) if checkpoint_data else None,
            memory=MemoryConfig(
                driver=memory_data.get('driver'),
                executor=memory_data.get('executor'),
                taskmanager=memory_data.get('taskmanager'),
                jobmanager=memory_data.get('jobmanager'),
                per_slot=memory_data.get('per_slot')
            ) if memory_data else None,
            cpu=CPUConfig(cores=cpu_data.get('cores', 2)) if cpu_data else None
        )
        
        # Парсим конфигурацию оркестрации
        orch_data = data.get('orchestration', {})
        orch_type = orch_data.get('type', 'native').lower()
        
        airflow_cfg = None
        k8s_cfg = None
        
        if orch_type == 'airflow':
            airflow_cfg = AirflowConfig(
                schedule_interval=orch_data.get('schedule_interval', '@daily'),
                start_date=orch_data.get('start_date', '2024-01-01'),
                catchup=orch_data.get('catchup', False),
                retries=orch_data.get('retries', 2),
                retry_delay=orch_data.get('retry_delay', '5m'),
                operator_config=orch_data.get('operator_config')
            )
        elif orch_type == 'kubernetes':
            resources_data = orch_data.get('resources', {})
            requests_data = resources_data.get('requests', {})
            limits_data = resources_data.get('limits', {})
            
            k8s_cfg = KubernetesConfig(
                namespace=orch_data.get('namespace', 'default'),
                service_account=orch_data.get('service_account'),
                image=orch_data.get('image', ''),
                crd_type=orch_data.get('crd_type'),
                resources=ResourceConfig(
                    requests=ResourceRequirements(
                        cpu=requests_data.get('cpu', '1'),
                        memory=requests_data.get('memory', '1Gi')
                    ) if requests_data else None,
                    limits=ResourceRequirements(
                        cpu=limits_data.get('cpu', '2'),
                        memory=limits_data.get('memory', '2Gi')
                    ) if limits_data else None
                ) if resources_data else None
            )
        
        orchestration = OrchestrationConfig(
            type=orch_type,
            image=orch_data.get('image'),
            airflow=airflow_cfg,
            kubernetes=k8s_cfg,
            env_vars=orch_data.get('env_vars')
        )
        
        # Парсим модели
        models = []
        for m in data.get('models', []):
            models.append(ModelConfig(
                name=m.get('name', ''),
                runtime=ModelRuntime(m.get('runtime', 'python')),
                requirements=m.get('requirements', []),
                cache_policy=CachePolicy(m.get('cache_policy', 'lazy_load')),
                storage_path=m.get('storage_path', '')
            ))
        
        # Собираем финальную конфигурацию
        return PlatformConfig(
            meta=data.get('meta', {}),
            target=data.get('target', {}),
            execution=execution,
            connectors=data.get('connectors', {'sources': [], 'sinks': []}),
            models=models,
            orchestration=orchestration
        )

    def get_platform(self) -> str:
        """
        Получение имени целевой платформы.
        
        Returns:
            Имя платформы (spark, flink, yql)
            
        Raises:
            ValueError: Если конфигурация не загружена
        """
        if not self.config:
            raise ValueError("Configuration not loaded")
        return self.config.target.get('platform', 'spark')

    def get_mode(self) -> str:
        """
        Получение режима выполнения.
        
        Returns:
            Режим (batch, streaming)
            
        Raises:
            ValueError: Если конфигурация не загружена
        """
        if not self.config:
            raise ValueError("Configuration not loaded")
        return self.config.target.get('mode', 'batch')

    def validate(self) -> bool:
        """
        Проверка согласованности конфигурации.
        
        Returns:
            True если конфигурация валидна
            
        Raises:
            ValueError: Если найдены ошибки валидации
        """
        if not self.config:
            return False
        
        target = self.config.target
        platform = target.get('platform', '').lower()
        mode = target.get('mode', '').lower()
        
        # Проверяем поддерживаемые комбинации платформа/режим
        if platform not in ['spark', 'flink', 'yql']:
            raise ValueError(f"Unsupported platform: {platform}")
        
        if mode not in ['batch', 'streaming']:
            raise ValueError(f"Unsupported mode: {mode}")
        
        # Flink streaming требует включенного чекпоинтирования для exactly-once
        if platform == 'flink' and mode == 'streaming':
            if self.config.execution.checkpointing:
                if not self.config.execution.checkpointing.enabled:
                    raise ValueError(
                        "Flink streaming requires checkpointing to be enabled"
                    )
        
        return True
