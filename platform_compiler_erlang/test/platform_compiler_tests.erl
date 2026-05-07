%%% @doc
%%% Тесты для модуля platform_compiler.
%%% Проверяют компиляцию DSL в код для Spark и Flink.
%%% @end

-module(platform_compiler_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/schemas.hrl").

%%====================================================================
%% Группы тестов
%%====================================================================

all_tests_() -> [
    fun compile_spark_batch/0,
    fun compile_flink_streaming/0,
    fun spark_sql_generation/0,
    fun spark_airflow_dag/0,
    fun flink_sql_generation/0,
    fun flink_k8s_manifest/0
].

%%====================================================================
%% Тесты компиляции
%%====================================================================

%% @doc Тест: компиляция для Spark batch режима
compile_spark_batch() ->
    DslContent = "
        DEFINE TABLE events (event_id BIGINT, user_id STRING, event_time TIMESTAMP);
        
        CREATE VIEW user_events AS 
        SELECT user_id, COUNT(*) as cnt 
        FROM events 
        GROUP BY user_id;
        
        INSERT INTO analytics SELECT user_id, cnt FROM user_events;
    ",
    
    ConfigContent = "
meta:
  name: test-pipeline
  version: 1.0.0
  owner: test-team
  
target:
  platform: spark
  mode: batch
  
execution:
  parallelism: 4
  memory:
    driver: \"2g\"
    executor: \"4g\"
    
orchestration:
  type: airflow
  schedule_interval: \"@hourly\"
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    %% Проверка наличия SQL файла
    ?assert(maps:is_key(<<"test-pipeline.sql">>, Outputs) orelse maps:is_key("test-pipeline.sql", Outputs)),
    
    %% Проверка наличия Airflow DAG
    ?assert(maps:is_key(<<"dag_test_pipeline.py">>, Outputs) orelse maps:is_key("dag_test_pipeline.py", Outputs)).

%% @doc Тест: компиляция для Flink streaming режима
compile_flink_streaming() ->
    DslContent = "
        DEFINE TABLE clicks (
            click_id BIGINT, 
            user_id STRING, 
            event_time TIMESTAMP
        ) WITH STREAM (
            time_attribute = 'event_time',
            watermark = '5 seconds'
        );
        
        CREATE VIEW user_clicks AS 
        SELECT user_id, COUNT(*) as cnt 
        FROM clicks 
        GROUP BY user_id;
        
        INSERT INTO output SELECT user_id, cnt FROM user_clicks;
    ",
    
    ConfigContent = "
meta:
  name: flink-streaming-pipeline
  version: 1.0.0
  
target:
  platform: flink
  mode: streaming
  
execution:
  parallelism: 2
  checkpointing:
    enabled: true
    interval: \"60s\"
    backend: rocksdb
    path: \"hdfs:///checkpoints\"
    
orchestration:
  type: kubernetes
  namespace: data-pipelines
  image: flink:1.17
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    %% Проверка наличия SQL файла
    ?assert(maps:is_key(<<"flink-streaming-pipeline.sql">>, Outputs) orelse maps:is_key("flink-streaming-pipeline.sql", Outputs)),
    
    %% Проверка наличия K8s манифеста
    ?assert(maps:is_key(<<"deployment_flink-streaming-pipeline.yaml">>, Outputs) orelse maps:is_key("deployment_flink-streaming-pipeline.yaml", Outputs)).

%%====================================================================
%% Тесты генерации Spark кода
%%====================================================================

%% @doc Тест: генерация Spark SQL
spark_sql_generation() ->
    DslContent = "
        DEFINE TABLE source_data (id INT, value STRING);
        CREATE VIEW transformed AS SELECT id, UPPER(value) as upper_val FROM source_data;
        INSERT INTO result SELECT * FROM transformed;
    ",
    
    ConfigContent = "
meta:
  name: spark-test
target:
  platform: spark
  mode: batch
execution:
  parallelism: 2
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    SqlContent = get_output_content(Outputs, "spark-test.sql"),
    ?assert(string:str(SqlContent, "CREATE OR REPLACE TEMPORARY VIEW") > 0),
    ?assert(string:str(SqlContent, "transformed") > 0).

%% @doc Тест: генерация Airflow DAG
spark_airflow_dag() ->
    DslContent = "DEFINE TABLE dummy (id INT);",
    
    ConfigContent = "
meta:
  name: airflow-test
  owner: test-owner
  description: Test DAG
target:
  platform: spark
  mode: batch
execution:
  parallelism: 4
  memory:
    driver: \"2g\"
    executor: \"4g\"
orchestration:
  type: airflow
  schedule_interval: \"@daily\"
  start_date: \"2024-01-01\"
  retries: 3
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    DagContent = get_output_content(Outputs, "dag_airflow_test.py"),
    ?assert(string:str(DagContent, "from airflow import DAG") > 0),
    ?assert(string:str(DagContent, "SparkSubmitOperator") > 0),
    ?assert(string:str(DagContent, "airflow-test") > 0).

%%====================================================================
%% Тесты генерации Flink кода
%%====================================================================

%% @doc Тест: генерация Flink SQL
flink_sql_generation() ->
    DslContent = "
        DEFINE TABLE stream_input (
            id BIGINT,
            event_time TIMESTAMP
        ) WITH STREAM (
            time_attribute = 'event_time',
            watermark = '10 seconds'
        );
        CREATE VIEW processed AS SELECT id FROM stream_input;
        INSERT INTO stream_output SELECT * FROM processed;
    ",
    
    ConfigContent = "
meta:
  name: flink-test
target:
  platform: flink
  mode: streaming
execution:
  parallelism: 2
  checkpointing:
    enabled: true
    interval: \"30s\"
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    SqlContent = get_output_content(Outputs, "flink-test.sql"),
    ?assert(string:str(SqlContent, "CREATE TABLE") > 0),
    ?assert(string:str(SqlContent, "WATERMARK FOR") > 0),
    ?assert(string:str(SqlContent, "event_time") > 0).

%% @doc Тест: генерация K8s манифеста
flink_k8s_manifest() ->
    DslContent = "DEFINE TABLE dummy (id INT);",
    
    ConfigContent = "
meta:
  name: k8s-flink-job
target:
  platform: flink
  mode: streaming
execution:
  parallelism: 4
  cpu:
    cores: 2
  memory:
    jobmanager: \"2048m\"
    taskmanager: \"4096m\"
  checkpointing:
    enabled: true
    interval: \"60s\"
    path: \"s3://bucket/checkpoints\"
orchestration:
  type: kubernetes
  namespace: production
  service_account: flink-operator
  image: flink:1.17
",
    
    {ok, Outputs} = platform_compiler:compile_from_strings(DslContent, ConfigContent),
    
    K8sContent = get_output_content(Outputs, "deployment_k8s-flink-job.yaml"),
    ?assert(string:str(K8sContent, "apiVersion: flink.apache.org/v1beta1") > 0),
    ?assert(string:str(K8sContent, "kind: FlinkDeployment") > 0),
    ?assert(string:str(K8sContent, "namespace: production") > 0),
    ?assert(string:str(K8sContent, "parallelism: 4") > 0).

%%====================================================================
%% Вспомогательные функции
%%====================================================================

%% @doc Получить контент из outputs по имени файла
get_output_content(Outputs, FileName) ->
    case maps:get(FileName, Outputs, undefined) of
        undefined ->
            %% Попытка с binary ключом
            maps:get(list_to_binary(FileName), Outputs);
        Content ->
            Content
    end.
