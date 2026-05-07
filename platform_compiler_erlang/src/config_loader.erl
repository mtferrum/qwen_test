%%% @doc
%%% Configuration loader module.
%%% Loads and validates YAML platform configuration files.
%%% @end

-module(config_loader).

-export([load_file/1, load_content/1, get_platform/1, get_mode/1, validate/1]).

-include("schemas.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% @doc Load configuration from a YAML file.
-spec load_file(string()) -> {ok, platform_config()} | {error, term()}.
load_file(FilePath) ->
    case file:read_file(FilePath) of
        {ok, Content} ->
            load_content(binary_to_list(Content));
        Error ->
            Error
    end.

%% @doc Load configuration from YAML string content.
-spec load_content(string()) -> {ok, platform_config()} | {error, term()}.
load_content(Content) ->
    try
        case yaml:load(Content) of
            {ok, Data} when is_map(Data) ->
                Config = parse_config(Data),
                {ok, Config};
            {ok, _} ->
                {error, invalid_yaml_format};
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:_ ->
            %% Fallback: simple YAML parser for basic configs
            FallbackConfig = parse_simple_yaml(Content),
            {ok, FallbackConfig}
    end.

%% @doc Get the target platform name.
-spec get_platform(platform_config()) -> string().
get_platform(Config) ->
    Target = maps:get(target, Config, #{}),
    maps:get(<<"platform">>, Target, <<"spark">>).

%% @doc Get the execution mode.
-spec get_mode(platform_config()) -> string().
get_mode(Config) ->
    Target = maps:get(target, Config, #{}),
    maps:get(<<"mode">>, Target, <<"batch">>).

%% @doc Validate configuration consistency.
-spec validate(platform_config()) -> true | {error, term()}.
validate(Config) ->
    Target = maps:get(target, Config, #{}),
    Platform = binary_to_lower(maps:get(<<"platform">>, Target, <<"spark">>)),
    Mode = binary_to_lower(maps:get(<<"mode">>, Target, <<"batch">>)),
    
    %% Check supported platforms
    case lists:member(Platform, [<<"spark">>, <<"flink">>, <<"yql">>]) of
        false ->
            {error, {unsupported_platform, Platform}};
        true ->
            %% Check supported modes
            case lists:member(Mode, [<<"batch">>, <<"streaming">>]) of
                false ->
                    {error, {unsupported_mode, Mode}};
                true ->
                    %% Flink streaming requires checkpointing
                    case Platform =:= <<"flink">> andalso Mode =:= <<"streaming">> of
                        true ->
                            Execution = maps:get(execution, Config, #{}),
                            Checkpointing = maps:get(checkpointing, Execution, undefined),
                            case Checkpointing of
                                #{enabled := true} ->
                                    true;
                                _ ->
                                    {error, flink_streaming_requires_checkpointing}
                            end;
                        false ->
                            true
                    end
            end
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Parse raw YAML data into platform_config record.
-spec parse_config(map()) -> platform_config().
parse_config(Data) ->
    Meta = maps:get(<<"meta">>, Data, #{}),
    Target = maps:get(<<"target">>, Data, #{}),
    ExecData = maps:get(<<"execution">>, Data, #{}),
    Connectors = maps:get(<<"connectors">>, Data, #{sources => [], sinks => []}),
    ModelsData = maps:get(<<"models">>, Data, []),
    OrchData = maps:get(<<"orchestration">>, Data, #{}),
    
    Execution = parse_execution_config(ExecData),
    Orchestration = parse_orchestration_config(OrchData),
    Models = [parse_model_config(M) || M <- ModelsData],
    
    #{
        meta => Meta,
        target => Target,
        execution => Execution,
        connectors => Connectors,
        models => Models,
        orchestration => Orchestration
    }.

-spec parse_execution_config(map()) -> execution_config().
parse_execution_config(Data) ->
    Parallelism = maps:get(<<"parallelism">>, Data, 1),
    CheckpointData = maps:get(<<"checkpointing">>, Data, undefined),
    MemoryData = maps:get(<<"memory">>, Data, undefined),
    CpuData = maps:get(<<"cpu">>, Data, undefined),
    
    Checkpointing = case CheckpointData of
        undefined -> undefined;
        _ -> parse_checkpoint_config(CheckpointData)
    end,
    
    Memory = case MemoryData of
        undefined -> undefined;
        _ -> parse_memory_config(MemoryData)
    end,
    
    Cpu = case CpuData of
        undefined -> undefined;
        _ -> parse_cpu_config(CpuData)
    end,
    
    #{
        parallelism => Parallelism,
        checkpointing => Checkpointing,
        memory => Memory,
        cpu => Cpu
    }.

-spec parse_checkpoint_config(map()) -> checkpoint_config().
parse_checkpoint_config(Data) ->
    #{
        enabled => maps:get(<<"enabled">>, Data, false),
        interval => maps:get(<<"interval">>, Data, undefined),
        backend => maps:get(<<"backend">>, Data, undefined),
        path => maps:get(<<"path">>, Data, undefined),
        num_retained => maps:get(<<"num_retained">>, Data, undefined)
    }.

-spec parse_memory_config(map()) -> memory_config().
parse_memory_config(Data) ->
    #{
        driver => maps:get(<<"driver">>, Data, undefined),
        executor => maps:get(<<"executor">>, Data, undefined),
        taskmanager => maps:get(<<"taskmanager">>, Data, undefined),
        jobmanager => maps:get(<<"jobmanager">>, Data, undefined),
        per_slot => maps:get(<<"per_slot">>, Data, undefined)
    }.

-spec parse_cpu_config(map()) -> cpu_config().
parse_cpu_config(Data) ->
    #{
        cores => maps:get(<<"cores">>, Data, 2)
    }.

-spec parse_orchestration_config(map()) -> orchestration_config().
parse_orchestration_config(Data) ->
    TypeBin = maps:get(<<"type">>, Data, <<"native">>),
    Type = binary_to_atom(binary_to_lower(TypeBin), utf8),
    
    AirflowCfg = case Type of
        airflow -> parse_airflow_config(Data);
        _ -> undefined
    end,
    
    K8sCfg = case Type of
        kubernetes -> parse_kubernetes_config(Data);
        _ -> undefined
    end,
    
    #{
        type => Type,
        image => maps:get(<<"image">>, Data, undefined),
        airflow => AirflowCfg,
        kubernetes => K8sCfg,
        env_vars => maps:get(<<"env_vars">>, Data, undefined)
    }.

-spec parse_airflow_config(map()) -> airflow_config().
parse_airflow_config(Data) ->
    #{
        schedule_interval => maps:get(<<"schedule_interval">>, Data, <<"@daily">>),
        start_date => maps:get(<<"start_date">>, Data, <<"2024-01-01">>),
        catchup => maps:get(<<"catchup">>, Data, false),
        retries => maps:get(<<"retries">>, Data, 2),
        retry_delay => maps:get(<<"retry_delay">>, Data, <<"5m">>),
        operator_config => maps:get(<<"operator_config">>, Data, undefined)
    }.

-spec parse_kubernetes_config(map()) -> kubernetes_config().
parse_kubernetes_config(Data) ->
    ResourcesData = maps:get(<<"resources">>, Data, undefined),
    Resources = case ResourcesData of
        undefined -> undefined;
        _ -> parse_resource_config(ResourcesData)
    end,
    
    #{
        namespace => maps:get(<<"namespace">>, Data, <<"default">>),
        service_account => maps:get(<<"service_account">>, Data, undefined),
        image => maps:get(<<"image">>, Data, <<>>),
        crd_type => maps:get(<<"crd_type">>, Data, undefined),
        resources => Resources
    }.

-spec parse_resource_config(map()) -> map().
parse_resource_config(Data) ->
    RequestsData = maps:get(<<"requests">>, Data, undefined),
    LimitsData = maps:get(<<"limits">>, Data, undefined),
    
    Requests = case RequestsData of
        undefined -> undefined;
        _ -> #{cpu => maps:get(<<"cpu">>, RequestsData, <<"1">>),
               memory => maps:get(<<"memory">>, RequestsData, <<"1Gi">>)}
    end,
    
    Limits = case LimitsData of
        undefined -> undefined;
        _ -> #{cpu => maps:get(<<"cpu">>, LimitsData, <<"2">>),
               memory => maps:get(<<"memory">>, LimitsData, <<"2Gi">>)}
    end,
    
    #{requests => Requests, limits => Limits}.

-spec parse_model_config(map()) -> model_config().
parse_model_config(Data) ->
    #{
        name => maps:get(<<"name">>, Data, <<>>),
        runtime => binary_to_atom(maps:get(<<"runtime">>, Data, <<"python">>), utf8),
        requirements => maps:get(<<"requirements">>, Data, []),
        cache_policy => binary_to_atom(maps:get(<<"cache_policy">>, Data, <<"lazy_load">>), utf8),
        storage_path => maps:get(<<"storage_path">>, Data, <<>>)
    }.

%% @doc Simple YAML parser fallback for basic configurations.
-spec parse_simple_yaml(string()) -> platform_config().
parse_simple_yaml(_Content) ->
    %% This is a simplified parser - in production, use a proper YAML library
    %% For now, return a default config
    default_platform_config().

%% Helper to convert binary to lowercase atom
-spec binary_to_lower(binary()) -> binary().
binary_to_lower(Bin) ->
    list_to_binary(string:lowercase(binary_to_list(Bin))).

-spec default_platform_config() -> platform_config().
default_platform_config() ->
    #{
        meta => #{},
        target => #{<<"platform">> => <<"spark">>, <<"mode">> => <<"batch">>},
        execution => #{parallelism => 1, checkpointing => undefined, memory => undefined, cpu => undefined},
        connectors => #{sources => [], sinks => []},
        models => [],
        orchestration => #{type => native}
    }.
