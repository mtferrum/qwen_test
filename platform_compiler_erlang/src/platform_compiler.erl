%%% @doc
%%% Platform Compiler - Main module.
%%% Orchestrates the compilation process:
%%% 1. Parse SQL-DSL file
%%% 2. Load platform configuration
%%% 3. Select code generator
%%% 4. Generate platform-specific code
%%% @end

-module(platform_compiler).

-export([compile/3, compile_from_strings/2]).

-include("schemas.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% @doc Compile DSL and configuration into platform artifacts.
-spec compile(string(), string(), string()) -> {ok, #{string() => string()}} | {error, term()}.
compile(DslPath, ConfigPath, OutputDir) ->
    io:format("Parsing DSL: ~s~n", [DslPath]),
    case dsl_parser:parse_file(DslPath) of
        undefined ->
            {error, failed_to_parse_dsl};
        DSL ->
            io:format("Loading configuration: ~s~n", [ConfigPath]),
            case config_loader:load_file(ConfigPath) of
                {ok, Config} ->
                    %% Validate configuration
                    case config_loader:validate(Config) of
                        true ->
                            generate_code(DSL, Config, OutputDir);
                        {error, Reason} ->
                            {error, {validation_failed, Reason}}
                    end;
                {error, Reason} ->
                    {error, {failed_to_load_config, Reason}}
            end
    end.

%% @doc Compile from strings (convenient for testing/API).
-spec compile_from_strings(string(), string()) -> {ok, #{string() => string()}} | {error, term()}.
compile_from_strings(DslContent, ConfigContent) ->
    %% Parse DSL
    DSL = dsl_parser:parse_content(DslContent),
    
    %% Load configuration
    case config_loader:load_content(ConfigContent) of
        {ok, Config} ->
            %% Validate
            case config_loader:validate(Config) of
                true ->
                    {ok, generate_outputs(DSL, Config)};
                {error, Reason} ->
                    {error, {validation_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {failed_to_load_config, Reason}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

-spec generate_code(dsl_layer(), platform_config(), string()) -> {ok, #{string() => string()}} | {error, term()}.
generate_code(DSL, Config, OutputDir) ->
    Outputs = generate_outputs(DSL, Config),
    
    %% Write output files
    OutputPath = filename:dirname(OutputDir),
    ok = filelib:ensure_dir(OutputPath ++ "/"),
    
    Results = maps:fold(
        fun(FileName, Content, _Acc) ->
            FilePath = OutputDir ++ "/" ++ FileName,
            case file:write_file(FilePath, Content) of
                ok ->
                    io:format("  Written: ~s~n", [FilePath]);
                {error, WriteErr} ->
                    io:format("  Error writing ~s: ~p~n", [FilePath, WriteErr])
            end,
            ok
        end,
        nil,
        Outputs
    ),
    
    {ok, Outputs}.

-spec generate_outputs(dsl_layer(), platform_config()) -> #{string() => string()}.
generate_outputs(DSL, Config) ->
    Target = maps:get(target, Config, #{}),
    PlatformBin = maps:get(<<"platform">>, Target, <<"spark">>),
    Platform = binary_to_atom(binary_to_lower(PlatformBin), utf8),
    
    io:format("Target platform: ~s~n", [atom_to_list(Platform)]),
    io:format("Generating code...~n"),
    
    %% Initialize generator state
    State = base_generator:init(DSL, Config),
    
    %% Select generator based on platform
    {GeneratorModule, Outputs0} = case Platform of
        spark ->
            {spark_generator, generate_spark_outputs(State, DSL, Config)};
        flink ->
            {flink_generator, generate_flink_outputs(State, DSL, Config)};
        _ ->
            erlang:error({unsupported_platform, Platform})
    end,
    
    %% Add orchestration artifacts
    Outputs1 = case Platform of
        spark ->
            Orch = maps:get(orchestration, Config, #{}),
            case maps:get(type, Orch, native) of
                airflow ->
                    DagContent = GeneratorModule:generate_airflow_dag(State),
                    Meta = maps:get(meta, Config, #{}),
                    PipelineName = binary_to_list(maps:get(<<"name">>, Meta, <<"pipeline">>)),
                    DagFileName = "dag_" ++ re:replace(PipelineName, "-", "_", [global, {return, list}]) ++ ".py",
                    Outputs0#{DagFileName => DagContent};
                _ ->
                    Outputs0
            end;
        flink ->
            Orch = maps:get(orchestration, Config, #{}),
            case maps:get(type, Orch, native) of
                kubernetes ->
                    K8sContent = GeneratorModule:generate_k8s_manifest(State),
                    Meta = maps:get(meta, Config, #{}),
                    PipelineName = binary_to_list(maps:get(<<"name">>, Meta, <<"pipeline">>)),
                    K8sFileName = "deployment_" ++ PipelineName ++ ".yaml",
                    Outputs0#{K8sFileName => K8sContent};
                _ ->
                    Outputs0
            end;
        _ ->
            Outputs0
    end,
    
    Outputs1.

-spec generate_spark_outputs(map(), dsl_layer(), platform_config()) -> #{string() => string()}.
generate_spark_outputs(State, _DSL, Config) ->
    SqlContent = base_generator:generate_full_script(State, spark_generator),
    Meta = maps:get(meta, Config, #{}),
    SqlFileName = binary_to_list(maps:get(<<"name">>, Meta, <<"pipeline">>)) ++ ".sql",
    #{SqlFileName => SqlContent}.

-spec generate_flink_outputs(map(), dsl_layer(), platform_config()) -> #{string() => string()}.
generate_flink_outputs(State, _DSL, Config) ->
    SqlContent = base_generator:generate_full_script(State, flink_generator),
    Meta = maps:get(meta, Config, #{}),
    SqlFileName = binary_to_list(maps:get(<<"name">>, Meta, <<"pipeline">>)) ++ ".sql",
    #{SqlFileName => SqlContent}.

-spec binary_to_lower(binary()) -> binary().
binary_to_lower(Bin) ->
    list_to_binary(string:lowercase(binary_to_list(Bin))).
