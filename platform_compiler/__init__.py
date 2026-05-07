"""
Platform Compiler for Spark + Airflow and Flink

A declarative SQL-DSL compiler that generates platform-specific code
for Apache Spark (Batch/Streaming) and Apache Flink (Streaming).

Architecture:
    pipeline.dsl (SQL-DSL) -> Parser -> IR -> Code Generator -> Platform Code
    platform.yaml (Config) -> Config Loader -> Runtime Parameters
"""

from .compiler import PlatformCompiler, compile_pipeline

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

__all__ = ["PlatformCompiler", "compile_pipeline"]
