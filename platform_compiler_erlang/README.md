# Platform Compiler Erlang

Erlang implementation of the Platform Compiler - a tool for generating platform-specific code from SQL-DSL.

## Structure

```
platform_compiler_erlang/
├── include/
│   └── schemas.hrl          # Type definitions and data structures
├── src/
│   ├── dsl_parser.erl       # SQL-DSL parser
│   ├── config_loader.erl    # YAML configuration loader
│   ├── base_generator.erl   # Base code generator
│   ├── spark_generator.erl  # Spark SQL code generator
│   ├── flink_generator.erl  # Flink SQL code generator
│   └── platform_compiler.erl # Main compiler orchestration
├── examples/
│   ├── pipeline.dsl         # Example DSL file
│   └── spark_batch_airflow.yaml  # Example configuration
└── test/
    └── ...                  # Test files
```

## Building

```bash
cd platform_compiler_erlang
rebar3 compile
```

## Usage

### From Erlang shell

```erlang
1> c(platform_compiler).
2> {ok, Outputs} = platform_compiler:compile(
       "examples/pipeline.dsl",
       "examples/spark_batch_airflow.yaml",
       "output/").
```

### Compile from strings

```erlang
1> DslContent = <<"DEFINE TABLE events (...);">>,
2> ConfigContent = <<"meta:\n  name: test\n...">>,
3> {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent).
```

## Supported Platforms

- **Spark** - Batch and Structured Streaming modes
  - Airflow DAG generation
  - Kafka and file sources/sinks
  
- **Flink** - Streaming mode
  - Kubernetes deployment manifests
  - Kafka and file sources/sinks

## Features

- SQL-DSL parsing (DEFINE TABLE, DEFINE MODEL, CREATE VIEW, INSERT)
- YAML configuration loading
- Platform-specific code generation
- Watermark and stream configuration support
- ML model registration
- Window function translations

## Requirements

- Erlang/OTP 24+
- rebar3
