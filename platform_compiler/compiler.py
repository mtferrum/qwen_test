"""
Главный интерфейс компилятора.

Оркестрирует процесс компиляции:
1. Парсинг SQL-DSL файла
2. Загрузка конфигурации платформы
3. Выбор генератора кода
4. Генерация платформо-специфичного кода
"""

from pathlib import Path
from typing import Any, Dict, Optional

from .core import DSLParser, ConfigLoader
from .compilers import SparkGenerator, FlinkGenerator
from .models.schemas import DSLLayer, PlatformConfig


class PlatformCompiler:
    """
    Главный компилятор для генерации кода из DSL в платформенный код.
    
    Пример использования:
        compiler = PlatformCompiler()
        outputs = compiler.compile(
            dsl_path='pipeline.dsl',
            config_path='platform.yaml',
            output_dir='output/'
        )
    """

    def __init__(self):
        """Инициализация компилятора."""
        self.parser = DSLParser()
        self.config_loader = ConfigLoader()
        self.dsl: Optional[DSLLayer] = None
        self.config: Optional[PlatformConfig] = None

    def compile(self, dsl_path: str, config_path: str, output_dir: str) -> Dict[str, str]:
        """
        Компиляция DSL и конфигурации в платформенные артефакты.
        
        Args:
            dsl_path: Путь к файлу pipeline.dsl
            config_path: Путь к файлу platform.yaml
            output_dir: Директория для записи сгенерированных файлов
            
        Returns:
            Словарь {имя_файла: содержимое}
        """
        # Шаг 1: Парсим DSL
        print(f"Parsing DSL: {dsl_path}")
        self.dsl = self.parser.parse_file(dsl_path)
        
        # Шаг 2: Загружаем конфигурацию
        print(f"Loading configuration: {config_path}")
        self.config = self.config_loader.load_file(config_path)
        
        # Валидация конфигурации
        self.config_loader.validate()
        
        # Шаг 3: Выбираем генератор по платформе
        platform = self.config.target.get('platform', 'spark').lower()
        print(f"Target platform: {platform}")
        
        if platform == 'spark':
            generator = SparkGenerator(self.dsl, self.config)
        elif platform == 'flink':
            generator = FlinkGenerator(self.dsl, self.config)
        else:
            raise ValueError(f"Unsupported platform: {platform}")
        
        # Шаг 4: Генерируем код
        print("Generating code...")
        outputs = {}
        
        # Генерируем основной SQL скрипт
        sql_content = generator.generate_full_script()
        sql_filename = f"{self.config.meta.get('name', 'pipeline')}.sql"
        outputs[sql_filename] = sql_content
        
        # Генерируем оркестрационные артефакты
        if platform == 'spark' and self.config.orchestration.type.value == 'airflow':
            dag_content = generator.generate_airflow_dag()
            dag_filename = f"dag_{self.config.meta.get('name', 'pipeline')}.py"
            outputs[dag_filename] = dag_content
        
        elif platform == 'flink' and self.config.orchestration.type.value == 'kubernetes':
            k8s_content = generator.generate_k8s_manifest()
            k8s_filename = f"deployment_{self.config.meta.get('name', 'pipeline')}.yaml"
            outputs[k8s_filename] = k8s_content
        
        # Записываем выходные файлы
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for filename, content in outputs.items():
            filepath = output_path / filename
            filepath.write_text(content)
            print(f"  Written: {filepath}")
        
        return outputs

    def compile_from_strings(self, dsl_content: str, config_content: str) -> Dict[str, str]:
        """
        Компиляция из строк (удобно для тестирования/API).
        
        Args:
            dsl_content: Строка с содержимым SQL-DSL
            config_content: Строка с YAML конфигурацией
            
        Returns:
            Словарь {имя_файла: содержимое}
        """
        # Парсим DSL
        self.dsl = self.parser.parse_content(dsl_content)
        
        # Загружаем конфигурацию
        self.config = self.config_loader.load_content(config_content)
        
        # Валидация
        self.config_loader.validate()
        
        # Выбираем генератор
        platform = self.config.target.get('platform', 'spark').lower()
        
        if platform == 'spark':
            generator = SparkGenerator(self.dsl, self.config)
        elif platform == 'flink':
            generator = FlinkGenerator(self.dsl, self.config)
        else:
            raise ValueError(f"Unsupported platform: {platform}")
        
        # Генерируем код
        outputs = {}
        
        sql_content = generator.generate_full_script()
        sql_filename = f"{self.config.meta.get('name', 'pipeline')}.sql"
        outputs[sql_filename] = sql_content
        
        # Добавляем оркестрационные артефакты
        if platform == 'spark' and self.config.orchestration.type.value == 'airflow':
            outputs[f"dag_{self.config.meta.get('name', 'pipeline')}.py"] = \
                generator.generate_airflow_dag()
        
        elif platform == 'flink' and self.config.orchestration.type.value == 'kubernetes':
            outputs[f"deployment_{self.config.meta.get('name', 'pipeline')}.yaml"] = \
                generator.generate_k8s_manifest()
        
        return outputs


def compile_pipeline(dsl_path: str, config_path: str, output_dir: str) -> Dict[str, str]:
    """
    Удобная функция для компиляции пайплайна.
    
    Args:
        dsl_path: Путь к DSL файлу
        config_path: Путь к конфигурации
        output_dir: Директория для вывода
        
    Returns:
        Словарь {имя_файла: содержимое}
    """
    compiler = PlatformCompiler()
    return compiler.compile(dsl_path, config_path, output_dir)
