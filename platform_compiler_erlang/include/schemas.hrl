%%% @doc
%%% Models and data structures for Platform Compiler.
%%% Defines structures for:
%%% - SQL-DSL (pipeline business logic)
%%% - Platform configuration (infrastructure)
%%% @end

%% This is a header file, not a module - removed -module(schemas).

-export_type([
    stream_config/0,
    table_definition/0,
    model_definition/0,
    graph_definition/0,
    dsl_layer/0,
    checkpoint_config/0,
    memory_config/0,
    cpu_config/0,
    execution_config/0,
    connector_config/0,
    source_connector/0,
    sink_connector/0,
    model_config/0,
    orchestration_config/0,
    platform_config/0
]).

%% Stream configuration for streaming tables
-type stream_config() :: #{
    time_attribute => string(),
    watermark => string() | undefined,
    allowed_lateness => string() | undefined
}.

%% Table definition (DEFINE TABLE)
-type table_definition() :: #{
    name => string(),
    columns => #{string() => string()},  %% #{column_name => type}
    stream_config => stream_config() | undefined
}.

%% ML Model definition (DEFINE MODEL)
-type model_definition() :: #{
    name => string(),
    path => string(),
    input_schema => string(),
    output_schema => string()
}.

%% Graph definition (DEFINE GRAPH)
-type graph_definition() :: #{
    name => string(),
    vertices_table => string(),
    vertices_id_col => string(),
    edges_table => string(),
    edges_src_col => string(),
    edges_dst_col => string()
}.

%% Complete DSL layer definition
-type dsl_layer() :: #{
    tables => [table_definition()],
    models => [model_definition()],
    graphs => [graph_definition()],
    views => #{string() => string()},  %% #{view_name => sql_query}
    inserts => [string()]
}.

%% Checkpoint configuration
-type checkpoint_config() :: #{
    enabled => boolean(),
    interval => string() | undefined,
    backend => rocksdb | fs | undefined,
    path => string() | undefined,
    num_retained => integer() | undefined
}.

%% Memory configuration
-type memory_config() :: #{
    driver => string() | undefined,
    executor => string() | undefined,
    taskmanager => string() | undefined,
    jobmanager => string() | undefined,
    per_slot => string() | undefined
}.

%% CPU configuration
-type cpu_config() :: #{
    cores => integer()
}.

%% Execution engine configuration
-type execution_config() :: #{
    parallelism => integer(),
    checkpointing => checkpoint_config() | undefined,
    memory => memory_config() | undefined,
    cpu => cpu_config() | undefined
}.

%% Connector configuration
-type connector_config() :: #{
    name => string(),
    type => kafka | hdfs | s3 | yt_table,
    config => map()
}.

%% Source connector
-type source_connector() :: #{
    name => string(),
    type => kafka | hdfs | s3 | yt_table,
    config => map()
}.

%% Sink connector
-type sink_connector() :: #{
    name => string(),
    type => kafka | hdfs | s3 | yt_table,
    semantics => at_least_once | exactly_once | at_most_once,
    config => map()
}.

%% Model runtime configuration
-type model_config() :: #{
    name => string(),
    runtime => python | java,
    requirements => [string()],
    cache_policy => lazy_load | eager_load,
    storage_path => string()
}.

%% Airflow orchestration configuration
-type airflow_config() :: #{
    schedule_interval => string(),
    start_date => string(),
    catchup => boolean(),
    retries => integer(),
    retry_delay => string(),
    operator_config => map() | undefined
}.

%% Kubernetes orchestration configuration
-type kubernetes_config() :: #{
    namespace => string(),
    service_account => string() | undefined,
    image => string(),
    crd_type => string() | undefined,
    resources => map() | undefined
}.

%% Orchestration configuration
-type orchestration_config() :: #{
    type => airflow | kubernetes | native,
    image => string() | undefined,
    airflow => airflow_config() | undefined,
    kubernetes => kubernetes_config() | undefined,
    env_vars => #{string() => string()} | undefined
}.

%% Complete platform configuration
-type platform_config() :: #{
    meta => #{string() => string()},
    target => #{string() => string()},
    execution => execution_config(),
    connectors => #{sources => [source_connector()], sinks => [sink_connector()]},
    models => [model_config()],
    orchestration => orchestration_config()
}.

%% Default values (exported for use in modules)
-export([default_stream_config/0, default_table/0, default_dsl/0,
         default_checkpoint_config/0, default_execution_config/0]).

default_stream_config() ->
    #{
        time_attribute => "",
        watermark => undefined,
        allowed_lateness => undefined
    }.

default_table() ->
    #{
        name => "",
        columns => #{},
        stream_config => undefined
    }.

default_dsl() ->
    #{
        tables => [],
        models => [],
        graphs => [],
        views => #{},
        inserts => []
    }.

default_checkpoint_config() ->
    #{
        enabled => false,
        interval => undefined,
        backend => undefined,
        path => undefined,
        num_retained => undefined
    }.

default_execution_config() ->
    #{
        parallelism => 1,
        checkpointing => undefined,
        memory => undefined,
        cpu => undefined
    }.
