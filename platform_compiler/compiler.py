"""
Main Compiler Interface

Orchestrates the compilation process:
1. Parse SQL-DSL file
2. Load platform configuration
3. Select appropriate code generator
4. Generate platform-specific code
"""

from pathlib import Path
from typing import Optional, Dict, Any

from .core import DSLParser, ConfigLoader
from .compilers import SparkGenerator, FlinkGenerator
from .models.schemas import DSLLayer, PlatformConfig


class PlatformCompiler:
    """Main compiler orchestrating DSL to platform code generation."""

    def __init__(self):
        self.parser = DSLParser()
        self.config_loader = ConfigLoader()
        self.dsl: Optional[DSLLayer] = None
        self.config: Optional[PlatformConfig] = None

    def compile(self, dsl_path: str, config_path: str, output_dir: str) -> Dict[str, str]:
        """
        Compile DSL and config into platform-specific artifacts.
        
        Args:
            dsl_path: Path to pipeline.dsl file
            config_path: Path to platform.yaml file
            output_dir: Directory to write generated files
            
        Returns:
            Dictionary mapping output filenames to their content
        """
        # Step 1: Parse DSL
        print(f"Parsing DSL: {dsl_path}")
        self.dsl = self.parser.parse_file(dsl_path)
        
        # Step 2: Load configuration
        print(f"Loading configuration: {config_path}")
        self.config = self.config_loader.load_file(config_path)
        
        # Validate configuration
        self.config_loader.validate()
        
        # Step 3: Select generator based on platform
        platform = self.config.target.get('platform', 'spark').lower()
        print(f"Target platform: {platform}")
        
        if platform == 'spark':
            generator = SparkGenerator(self.dsl, self.config)
        elif platform == 'flink':
            generator = FlinkGenerator(self.dsl, self.config)
        else:
            raise ValueError(f"Unsupported platform: {platform}")
        
        # Step 4: Generate code
        print("Generating code...")
        outputs = {}
        
        # Generate main SQL script
        sql_content = generator.generate_full_script()
        sql_filename = f"{self.config.meta.get('name', 'pipeline')}.sql"
        outputs[sql_filename] = sql_content
        
        # Generate orchestration artifacts
        if platform == 'spark' and self.config.orchestration.type == 'AIRFLOW':
            dag_content = generator.generate_airflow_dag()
            dag_filename = f"dag_{self.config.meta.get('name', 'pipeline')}.py"
            outputs[dag_filename] = dag_content
        
        elif platform == 'flink' and self.config.orchestration.type == 'KUBERNETES':
            k8s_content = generator.generate_k8s_manifest()
            k8s_filename = f"deployment_{self.config.meta.get('name', 'pipeline')}.yaml"
            outputs[k8s_filename] = k8s_content
        
        # Write outputs
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for filename, content in outputs.items():
            filepath = output_path / filename
            filepath.write_text(content)
            print(f"  Written: {filepath}")
        
        return outputs

    def compile_from_strings(self, dsl_content: str, config_content: str) -> Dict[str, str]:
        """
        Compile from string contents (useful for testing/API).
        
        Args:
            dsl_content: SQL-DSL content string
            config_content: Platform YAML content string
            
        Returns:
            Dictionary mapping output filenames to their content
        """
        # Parse DSL
        self.dsl = self.parser.parse_content(dsl_content)
        
        # Load configuration
        self.config = self.config_loader.load_content(config_content)
        
        # Validate
        self.config_loader.validate()
        
        # Select generator
        platform = self.config.target.get('platform', 'spark').lower()
        
        if platform == 'spark':
            generator = SparkGenerator(self.dsl, self.config)
        elif platform == 'flink':
            generator = FlinkGenerator(self.dsl, self.config)
        else:
            raise ValueError(f"Unsupported platform: {platform}")
        
        # Generate code
        outputs = {}
        
        sql_content = generator.generate_full_script()
        sql_filename = f"{self.config.meta.get('name', 'pipeline')}.sql"
        outputs[sql_filename] = sql_content
        
        # Add orchestration artifacts
        if platform == 'spark' and self.config.orchestration.airflow:
            outputs[f"dag_{self.config.meta.get('name', 'pipeline')}.py"] = \
                generator.generate_airflow_dag()
        
        elif platform == 'flink' and self.config.orchestration.kubernetes:
            outputs[f"deployment_{self.config.meta.get('name', 'pipeline')}.yaml"] = \
                generator.generate_k8s_manifest()
        
        return outputs


def compile_pipeline(dsl_path: str, config_path: str, output_dir: str) -> Dict[str, str]:
    """Convenience function for compiling a pipeline."""
    compiler = PlatformCompiler()
    return compiler.compile(dsl_path, config_path, output_dir)
