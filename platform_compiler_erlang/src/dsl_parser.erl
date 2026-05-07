%%% @doc
%%% DSL Parser module.
%%% Parses SQL-DSL files and extracts:
%%% - DEFINE TABLE - table definitions
%%% - DEFINE MODEL - ML model definitions
%%% - DEFINE GRAPH - graph definitions
%%% - CREATE VIEW - views
%%% - INSERT/CREATE TABLE AS SELECT - insert operations
%%% @end

-module(dsl_parser).

-export([parse_file/1, parse_content/1]).

-include("schemas.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% @doc Parse a DSL file from disk.
-spec parse_file(string()) -> dsl_layer().
parse_file(FilePath) ->
    {ok, Content} = file:read_file(FilePath),
    parse_content(binary_to_list(Content)).

%% @doc Parse DSL content from a string.
-spec parse_content(string()) -> dsl_layer().
parse_content(Content) ->
    %% Remove comments
    CleanContent = remove_comments(Content),
    
    %% Parse each construct
    Tables = parse_tables(CleanContent),
    Models = parse_models(CleanContent),
    Graphs = parse_graphs(CleanContent),
    Views = parse_views(CleanContent),
    Inserts = parse_inserts(CleanContent),
    
    #{
        tables => Tables,
        models => Models,
        graphs => Graphs,
        views => Views,
        inserts => Inserts
    }.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Remove SQL comments (-- and /* */).
-spec remove_comments(string()) -> string().
remove_comments(Content) ->
    %% Remove single-line comments
    NoSingleLine = re:replace(Content, "--.*$", "", [{return, list}, multiline]),
    %% Remove multi-line comments
    re:replace(NoSingleLine, "/\\*.*?\\*/", "", [{return, list}, dotall]).

%% @doc Parse DEFINE TABLE statements.
-spec parse_tables(string()) -> [table_definition()].
parse_tables(Content) ->
    Pattern = "(?i)DEFINE\\s+TABLE\\s+(\\w+)\\s*\\((.*?)\\)\\s*(?:WITH\\s+STREAM\\s*\\((.*?)\\))?\\s*;",
    case re:run(Content, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            [parse_table_match(M) || M <- Matches];
        nomatch ->
            []
    end.

-spec parse_table_match([string()]) -> table_definition().
parse_table_match([Name, ColumnsStr, StreamStr]) ->
    Columns = parse_columns(ColumnsStr),
    StreamConfig = parse_stream_config(StreamStr),
    #{
        name => list_to_binary(Name),
        columns => Columns,
        stream_config => StreamConfig
    };
parse_table_match([Name, ColumnsStr]) ->
    Columns = parse_columns(ColumnsStr),
    #{
        name => list_to_binary(Name),
        columns => Columns,
        stream_config => undefined
    }.

%% @doc Parse column definitions.
-spec parse_columns(string()) -> #{binary() => binary()}.
parse_columns(ColumnsStr) ->
    Parts = re:split(ColumnsStr, ",(?![^<]*>)", [{return, list}]),
    parse_columns_parts(Parts, #{}).

-spec parse_columns_parts([string()], #{binary() => binary()}) -> #{binary() => binary()}.
parse_columns_parts([], Acc) ->
    Acc;
parse_columns_parts(["" | Rest], Acc) ->
    parse_columns_parts(Rest, Acc);
parse_columns_parts([Part | Rest], Acc) ->
    Trimmed = string:trim(Part),
    case re:run(Trimmed, "(\\w+)\\s+(.+)", [{capture, all_but_first, list}]) of
        {match, [ColName, ColType]} ->
            parse_columns_parts(Rest, maps:put(list_to_binary(string:trim(ColName)), list_to_binary(string:to_upper(string:trim(ColType))), Acc));
        nomatch ->
            parse_columns_parts(Rest, Acc)
    end.

%% @doc Parse WITH STREAM configuration.
-spec parse_stream_config(string()) -> stream_config().
parse_stream_config(StreamStr) when StreamStr =:= "" orelse StreamStr =:= undefined ->
    undefined;
parse_stream_config(StreamStr) ->
    Config = parse_stream_pairs(StreamStr),
    #{
        time_attribute => maps:get("time_attribute", Config, ""),
        watermark => maps:get("watermark", Config, undefined),
        allowed_lateness => maps:get("allowed_lateness", Config, undefined)
    }.

-spec parse_stream_pairs(string()) -> #{string() => string()}.
parse_stream_pairs(Str) ->
    Pattern = "(\\w+)\\s*=\\s*(?:'([^']*)'|(\\w+))",
    case re:run(Str, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            lists:foldl(fun([K, V1, V2], Acc) ->
                V = case V1 of
                    "" -> V2;
                    _ -> V1
                end,
                maps:put(K, V, Acc)
            end, #{}, Matches);
        nomatch ->
            #{}
    end.

%% @doc Parse DEFINE MODEL statements.
-spec parse_models(string()) -> [model_definition()].
parse_models(Content) ->
    Pattern = "(?i)DEFINE\\s+MODEL\\s+(\\w+)\\s*\\((.*?)\\)\\s*;",
    case re:run(Content, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            [parse_model_match(M) || M <- Matches];
        nomatch ->
            []
    end.

-spec parse_model_match([string()]) -> model_definition().
parse_model_match([Name, ParamsStr]) ->
    Params = parse_model_params(ParamsStr),
    #{
        name => list_to_binary(Name),
        path => maps:get(<<"path">>, Params, <<>>),
        input_schema => maps:get(<<"input_schema">>, Params, <<>>),
        output_schema => maps:get(<<"output_schema">>, Params, <<>>)
    }.

-spec parse_model_params(string()) -> #{binary() => binary()}.
parse_model_params(Str) ->
    Pattern = "(\\w+)\\s*=\\s*'([^']*)'",
    case re:run(Str, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            lists:foldl(fun([K, V], Acc) ->
                maps:put(list_to_binary(K), list_to_binary(V), Acc)
            end, #{}, Matches);
        nomatch ->
            #{}
    end.

%% @doc Parse DEFINE GRAPH statements.
-spec parse_graphs(string()) -> [graph_definition()].
parse_graphs(Content) ->
    Pattern = "(?i)DEFINE\\s+GRAPH\\s+(\\w+)\\s*\\((.*?)\\)\\s*;",
    case re:run(Content, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            [parse_graph_match(M) || M <- Matches];
        nomatch ->
            []
    end.

-spec parse_graph_match([string()]) -> graph_definition().
parse_graph_match([Name, Body]) ->
    VMatch = re:run(Body, "(?i)vertices\\s*=>\\s*(\\w+)\\s*\\(([^)]+)\\)", [{capture, all_but_first, list}]),
    EMatch = re:run(Body, "(?i)edges\\s*=>\\s*(\\w+)\\s*\\(([^)]+)\\)", [{capture, all_but_first, list}]),
    case {VMatch, EMatch} of
        {{match, [VTable, VCols]}, {match, [ETable, ECols]}} ->
            VColList = [string:trim(C) || C <- re:split(VCols, ",", [{return, list}])],
            EColList = [string:trim(C) || C <- re:split(ECols, ",", [{return, list}])],
            #{
                name => list_to_binary(Name),
                vertices_table => list_to_binary(VTable),
                vertices_id_col => case VColList of [H | _] -> list_to_binary(H); [] -> <<>> end,
                edges_table => list_to_binary(ETable),
                edges_src_col => case EColList of [H | _] -> list_to_binary(H); [] -> <<>> end,
                edges_dst_col => case EColList of [_, H | _] -> list_to_binary(H); _ -> <<>> end
            };
        _ ->
            #{
                name => list_to_binary(Name),
                vertices_table => <<>>,
                vertices_id_col => <<>>,
                edges_table => <<>>,
                edges_src_col => <<>>,
                edges_dst_col => <<>>
            }
    end.

%% @doc Parse CREATE VIEW statements.
-spec parse_views(string()) -> #{binary() => binary()}.
parse_views(Content) ->
    ViewPattern = "(?i)CREATE\\s+VIEW\\s+(\\w+)\\s+AS\\s+(.*?)(?=CREATE\\s+VIEW|CREATE\\s+TABLE|INSERT|$)",
    TableAsPattern = "(?i)CREATE\\s+TABLE\\s+(\\w+)\\s+AS\\s+(.*?)(?=CREATE\\s+VIEW|CREATE\\s+TABLE|INSERT|$)",
    
    Views1 = parse_view_matches(ViewPattern, Content, #{}),
    parse_view_matches(TableAsPattern, Content, Views1).

-spec parse_view_matches(string(), string(), #{binary() => binary()}) -> #{binary() => binary()}.
parse_view_matches(Pattern, Content, Acc) ->
    case re:run(Content, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            lists:foldl(fun([Name, Query], A) ->
                CleanQuery = string:trim(re:replace(Query, ";\\s*$", "", [{return, list}])),
                maps:put(list_to_binary(Name), list_to_binary(CleanQuery), A)
            end, Acc, Matches);
        nomatch ->
            Acc
    end.

%% @doc Parse INSERT statements.
-spec parse_inserts(string()) -> [string()].
parse_inserts(Content) ->
    Pattern = "(?i)INSERT\\s+INTO\\s+(\\w+)\\s+SELECT\\s+(.*?)(?=CREATE|INSERT|$)",
    case re:run(Content, Pattern, [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            [begin
                 Table = T,
                 SelectQ = string:trim(re:replace(S, ";\\s*$", "", [{return, list}])),
                 "INSERT INTO " ++ Table ++ " SELECT " ++ SelectQ
             end || [T, S] <- Matches];
        nomatch ->
            []
    end.
