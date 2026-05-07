%%% @doc
%%% Flink SQL Code Generator.
%%% Generates Flink SQL code for Streaming mode.
%%% Supports Kubernetes deployment via Flink Operator.
%%% @end

-module(flink_generator).

-export([generate_source_ddl/1, generate_sink_ddl/1, generate_transformations/1,
         generate_udf_registrations/1, generate_k8s_manifest/1]).

-include("schemas.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% @doc Generate Flink source table DDL.
-spec generate_source_ddl(map()) -> string().
generate_source_ddl(State) ->
    DSL = maps:get(dsl, State),
    Config = maps:get(config, State),
    Sources = maps:get(sources, maps:get(connectors, Config, #{}), []),
    
    Header = "-- ==========================================\n-- SOURCE TABLES\n-- ==========================================",
    TablesDDL = [generate_source_table(T, Sources, State) || T <- maps:get(tables, DSL, [])],
    string:join([Header | TablesDDL], "\n\n").

%% @doc Generate Flink sink table DDL.
-spec generate_sink_ddl(map()) -> string().
generate_sink_ddl(State) ->
    Config = maps:get(config, State),
    Sinks = maps:get(sinks, maps:get(connectors, Config, #{}), []),
    
    Header = "-- ==========================================\n-- SINK TABLES\n-- ==========================================",
    SinksDDL = [generate_sink_table(S, State) || S <- Sinks],
    string:join([Header | SinksDDL], "\n\n").

%% @doc Generate transformation logic (views and queries).
-spec generate_transformations(map()) -> string().
generate_transformations(State) ->
    DSL = maps:get(dsl, State),
    
    Header = "-- ==========================================\n-- TRANSFORMATIONS\n-- ==========================================",
    ViewsDDL = [generate_view(Name, Query, State) || {Name, Query} <- maps:to_list(maps:get(views, DSL, #{}))],
    InsertsDDL = [translate_to_flink(I) ++ ";" || I <- maps:get(inserts, DSL, [])],
    string:join([Header | ViewsDDL ++ InsertsDDL], "\n\n").

%% @doc Generate UDF registration code for Flink.
-spec generate_udf_registrations(map()) -> string().
generate_udf_registrations(State) ->
    DSL = maps:get(dsl, State),
    
    Header = "-- ==========================================\n-- UDF/UDTF REGISTRATIONS\n-- ==========================================",
    ModelsDDL = [generate_model_registration(M) || M <- maps:get(models, DSL, [])],
    string:join([Header | ModelsDDL], "\n\n").

%% @doc Generate Kubernetes manifest for Flink deployment.
-spec generate_k8s_manifest(map()) -> string().
generate_k8s_manifest(State) ->
    Config = maps:get(config, State),
    Meta = maps:get(meta, Config, #{}),
    K8s = maps:get(kubernetes, maps:get(orchestration, Config, #{}), undefined),
    Exec = maps:get(execution, Config, #{}),
    Memory = maps:get(memory, Exec, #{}),
    Cpu = maps:get(cpu, Exec, #{}),
    
    JobName = re:replace(maps:get(<<"name">>, Meta, <<"flink-pipeline">>), "-", "-", [global, {return, list}]),
    Namespace = case K8s of
        undefined -> "default";
        _ -> binary_to_list(maps:get(<<"namespace">>, K8s, <<"default">>))
    end,
    Image = case K8s of
        undefined -> "flink:1.17";
        _ -> binary_to_list(maps:get(<<"image">>, K8s, <<"flink:1.17">>))
    end,
    
    Parallelism = maps:get(parallelism, Exec, 1),
    JmMemory = case Memory of
        #{} -> binary_to_list(maps:get(<<"jobmanager">>, Memory, <<"2048m">>));
        _ -> "2048m"
    end,
    TmMemory = case Memory of
        #{} -> binary_to_list(maps:get(<<"taskmanager">>, Memory, <<"4096m">>));
        _ -> "4096m"
    end,
    CpuCores = case Cpu of
        #{} -> integer_to_list(maps:get(cores, Cpu, 2));
        _ -> "2"
    end,
    
    CheckpointInterval = case maps:get(checkpointing, Exec, undefined) of
        #{interval := Interval} -> binary_to_list(Interval);
        _ -> "60000"
    end,
    
    CheckpointPath = case maps:get(checkpointing, Exec, undefined) of
        #{path := Path} -> binary_to_list(Path);
        _ -> "file:///tmp/flink-checkpoints"
    end,
    
    io_lib:format(
"apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: ~s
  namespace: ~s
spec:
  image: ~s
  flinkVersion: v1_17
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: \"~B\"
    state.backend: rocksdb
    state.checkpoints.dir: \"~s\"
    execution.checkpointing.interval: \"~sms\"
  serviceAccount: ~s
  jobManager:
    resource:
      memory: ~s
      cpu: ~s
  taskManager:
    resource:
      memory: ~s
      cpu: ~s
  job:
    jarURI: local:///opt/flink/usrlib/~s.jar
    parallelism: ~B
    upgradeMode: stateless",
        [JobName, Namespace, Image, Parallelism, CheckpointPath, CheckpointInterval,
         case K8s of undefined -> "flink"; _ -> binary_to_list(maps:get(<<"service_account">>, K8s, <<"flink">>)) end,
         JmMemory, CpuCores, TmMemory, CpuCores, JobName, Parallelism]).

%%====================================================================
%% Internal functions
%%====================================================================

-spec generate_source_table(map(), [map()], map()) -> string().
generate_source_table(Table, Sources, State) ->
    Name = maps:get(name, Table),
    Connector = find_connector(Name, Sources),
    
    case Connector of
        undefined ->
            create_flink_table(Table);
        _ ->
            ConnType = binary_to_list(maps:get(<<"type">>, Connector, <<"kafka">>)),
            ConnConfig = maps:get(<<"config">>, Connector, #{}),
            case ConnType of
                "kafka" -> generate_kafka_source(Table, ConnConfig, State);
                "hdfs" -> generate_file_source(Table, ConnConfig, State);
                "s3" -> generate_file_source(Table, ConnConfig, State);
                _ -> "-- Unsupported source type: " ++ ConnType
            end
    end.

-spec generate_sink_table(map(), map()) -> string().
generate_sink_table(Sink, State) ->
    Name = binary_to_list(maps:get(<<"name">>, Sink, <<"">>)),
    ConnType = binary_to_list(maps:get(<<"type">>, Sink, <<"kafka">>)),
    ConnConfig = maps:get(<<"config">>, Sink, #{}),
    Semantics = binary_to_atom(binary_to_list(maps:get(<<"semantics">>, Sink, <<"at_least_once">>)), utf8),
    
    case ConnType of
        "kafka" -> generate_kafka_sink(Name, ConnConfig, Semantics);
        "hdfs" -> generate_file_sink(Name, ConnConfig);
        "s3" -> generate_file_sink(Name, ConnConfig);
        _ -> "-- Unsupported sink type: " ++ ConnType
    end.

-spec generate_kafka_source(map(), map(), map()) -> string().
generate_kafka_source(Table, Config, _State) ->
    Name = maps:get(name, Table),
    Columns = string:join([K ++ " " ++ base_generator:format_type(V) || {K, V} <- maps:to_list(maps:get(columns, Table, #{}))], ",\n  "),
    Servers = string:join(binary_to_list(maps:get(<<"bootstrap_servers">>, Config, [<<"localhost:9092">>])), ","),
    Topic = binary_to_list(maps:get(<<"topic">>, Config, list_to_binary(Name))),
    Format = string:to_upper(binary_to_list(maps:get(<<"format">>, Config, <<"json">>))),
    StartupMode = binary_to_list(maps:get(<<"scan_startup_mode">>, Config, <<"latest-offset">>)),
    
    WatermarkClause = case maps:get(stream_config, Table, undefined) of
        undefined -> "";
        SC ->
            TimeAttr = maps:get(time_attribute, SC, ""),
            WmInterval = maps:get(watermark, SC, "5 seconds"),
            ",\n  WATERMARK FOR " ++ TimeAttr ++ " AS " ++ TimeAttr ++ " - INTERVAL '" ++ WmInterval ++ "'"
    end,
    
    io_lib:format(
"-- Kafka Source: ~s
CREATE TABLE ~s (
  ~s~s
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = '~s',
  'topic' = '~s',
  'scan.startup.mode' = '~s',
  'format' = '~s'
);", [Name, Name, Columns, WatermarkClause, Servers, Topic, StartupMode, string:to_lower(Format)]).

-spec generate_file_source(map(), map(), map()) -> string().
generate_file_source(Table, Config, _State) ->
    Name = maps:get(name, Table),
    Columns = string:join([K ++ " " ++ base_generator:format_type(V) || {K, V} <- maps:to_list(maps:get(columns, Table, #{}))], ",\n  "),
    DefaultPath = list_to_binary("/data/" ++ atom_to_list(Name)),
    Path = binary_to_list(maps:get(<<"path">>, Config, DefaultPath)),
    Format = string:to_upper(binary_to_list(maps:get(<<"format">>, Config, <<"parquet">>))),
    
    io_lib:format(
"-- File Source: ~s
CREATE TABLE ~s (
  ~s
) WITH (
  'connector' = 'filesystem',
  'path' = '~s',
  'format' = '~s'
);", [Name, Name, Columns, Path, string:to_lower(Format)]).

-spec generate_kafka_sink(string(), map(), atom()) -> string().
generate_kafka_sink(Name, Config, Semantics) ->
    Servers = string:join(binary_to_list(maps:get(<<"bootstrap_servers">>, Config, [<<"localhost:9092">>])), ","),
    Topic = binary_to_list(maps:get(<<"topic">>, Config, list_to_binary(Name))),
    
    SemanticValue = case Semantics of
        exactly_once -> "exactly-once";
        _ -> "at-least-once"
    end,
    
    io_lib:format(
"-- Kafka Sink: ~s
-- Use INSERT INTO to write to this sink
-- Configuration:
--   bootstrap.servers: ~s
--   topic: ~s
--   semantic: ~s", [Name, Servers, Topic, SemanticValue]).

-spec generate_file_sink(string(), map()) -> string().
generate_file_sink(Name, Config) ->
    DefaultPath = list_to_binary("/data/" ++ Name),
    Path = binary_to_list(maps:get(<<"path">>, Config, DefaultPath)),
    Format = string:to_upper(binary_to_list(maps:get(<<"format">>, Config, <<"parquet">>))),
    PartitionBy = maps:get(<<"partition_by">>, Config, []),
    
    PartitionClause = case PartitionBy of
        [] -> "";
        _ -> ",\n  'partition' = '" ++ string:join([binary_to_list(P) || P <- PartitionBy], ",") ++ "'"
    end,
    
    io_lib:format(
"-- File Sink: ~s
-- Use INSERT INTO to write to this sink
-- Configuration:
--   path: ~s
--   format: ~s~s", [Name, Path, string:to_lower(Format), PartitionClause]).

-spec create_flink_table(map()) -> string().
create_flink_table(Table) ->
    Name = maps:get(name, Table),
    Columns = string:join([K ++ " " ++ base_generator:format_type(V) || {K, V} <- maps:to_list(maps:get(columns, Table, #{}))], ",\n  "),
    
    WatermarkClause = case maps:get(stream_config, Table, undefined) of
        undefined -> "";
        SC ->
            TimeAttr = maps:get(time_attribute, SC, ""),
            WmInterval = maps:get(watermark, SC, "5 seconds"),
            ",\n  WATERMARK FOR " ++ TimeAttr ++ " AS " ++ TimeAttr ++ " - INTERVAL '" ++ WmInterval ++ "'"
    end,
    
    io_lib:format(
"-- Table: ~s
CREATE TABLE ~s (
  ~s~s
);", [Name, Name, Columns, WatermarkClause]).

-spec generate_model_registration(map()) -> string().
generate_model_registration(Model) ->
    Name = maps:get(name, Model),
    Path = maps:get(path, Model),
    InputSchema = maps:get(input_schema, Model),
    OutputSchema = maps:get(output_schema, Model),
    
    io_lib:format(
"-- Model: ~s
-- Path: ~s
-- Input: ~s, Output: ~s

-- Create Python UDF for ~s
-- CREATE TEMPORARY FUNCTION ~s 
-- AS 'udfs.~s_udf' 
-- LANGUAGE PYTHON;", [Name, Path, InputSchema, OutputSchema, Name, Name, Name]).

-spec generate_view(string(), string(), map()) -> string().
generate_view(Name, Query, _State) ->
    TranslatedQuery = translate_to_flink(Query),
    io_lib:format("CREATE VIEW ~s AS\n~s;", [Name, TranslatedQuery]).

-spec translate_to_flink(string()) -> string().
translate_to_flink(Query) ->
    Translations = [
        {"TUMBLE_START(", "TUMBLE_START("},
        {"TUMBLE_END(", "TUMBLE_END("},
        {"TUMBLE(", "TUMBLE("},
        {"HOP(", "HOP("},
        {"SESSION(", "SESSION("},
        {"CROSS JOIN LATERAL TABLE(", "CROSS JOIN LATERAL TABLE("},
        {"APPLY_MODEL(", "apply_model("},
        {"ENCODE_TEXT(", "encode_text("},
        {"LLM_GENERATE(", "llm_generate("},
        {"VECTOR_SEARCH(", "vector_search("}
    ],
    lists:foldl(fun({From, To}, Acc) -> re:replace(Acc, From, To, [global, {return, list}]) end, Query, Translations).

-spec find_connector(string(), [map()]) -> map() | undefined.
find_connector(_Name, []) ->
    undefined;
find_connector(Name, [C | Rest]) ->
    BinName = maps:get(<<"name">>, C, <<>>),
    case BinName =:= list_to_binary(Name) of
        true -> C;
        false -> find_connector(Name, Rest)
    end.
