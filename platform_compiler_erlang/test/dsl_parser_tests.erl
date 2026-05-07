%%% @doc
%%% Тесты для модуля dsl_parser.
%%% Проверяют парсинг всех конструкций SQL-DSL.
%%% @end

-module(dsl_parser_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/schemas.hrl").

%%====================================================================
%% Группы тестов
%%====================================================================

all_tests_() -> [
    fun parse_empty_content_test/0,
    fun parse_complete_dsl_test/0,
    fun parse_simple_table_test/0,
    fun parse_streaming_table_test/0,
    fun parse_multiple_tables_test/0,
    fun parse_model_definition_test/0,
    fun parse_graph_definition_test/0,
    fun parse_simple_view_test/0,
    fun parse_create_table_as_test/0,
    fun parse_insert_statement_test/0
].

%%====================================================================
%% Тесты парсинга
%%====================================================================

%% @doc Тест: пустой контент
parse_empty_content_test() ->
    Result = dsl_parser:parse_content(""),
    Expected = #{tables => [], models => [], graphs => [], views => #{}, inserts => []},
    ?assertEqual(Expected, Result).

%% @doc Тест: полный DSL со всеми конструкциями
parse_complete_dsl_test() ->
    DslContent = "
        DEFINE TABLE users (id INT, name STRING, created_at TIMESTAMP);
        
        DEFINE MODEL user_classifier (path='/models/classifier.pkl', input_schema='features', output_schema='prediction');
        
        DEFINE GRAPH social_network (vertices => users(id), edges => friendships(user_id, friend_id));
        
        CREATE VIEW active_users AS SELECT * FROM users WHERE created_at > '2024-01-01';
        
        INSERT INTO analytics SELECT user_id, COUNT(*) FROM events GROUP BY user_id;
    ",
    Result = dsl_parser:parse_content(DslContent),
    
    %% Проверка таблиц
    ?assertEqual(1, length(maps:get(tables, Result))),
    FirstTable = hd(maps:get(tables, Result)),
    ?assertEqual(<<"users">>, maps:get(name, FirstTable)),
    
    %% Проверка моделей
    ?assertEqual(1, length(maps:get(models, Result))),
    
    %% Проверка графов
    ?assertEqual(1, length(maps:get(graphs, Result))),
    
    %% Проверка представлений
    ?assertEqual(1, maps:size(maps:get(views, Result))),
    
    %% Проверка вставок
    ?assertEqual(1, length(maps:get(inserts, Result))).

%% @doc Тест: простая таблица без стриминга
parse_simple_table_test() ->
    Content = "DEFINE TABLE orders (order_id BIGINT, amount DOUBLE, status STRING);",
    Result = dsl_parser:parse_content(Content),
    Tables = maps:get(tables, Result),
    
    ?assertEqual(1, length(Tables)),
    Table = hd(Tables),
    ?assertEqual(<<"orders">>, maps:get(name, Table)),
    
    Columns = maps:get(columns, Table),
    ?assertEqual(3, maps:size(Columns)),
    ?assertEqual(<<"BIGINT">>, maps:get(<<"order_id">>, Columns)),
    ?assertEqual(<<"DOUBLE">>, maps:get(<<"amount">>, Columns)),
    ?assertEqual(<<"STRING">>, maps:get(<<"status">>, Columns)),
    
    ?assertEqual(undefined, maps:get(stream_config, Table)).

%% @doc Тест: таблица с конфигурацией стриминга
parse_streaming_table_test() ->
    Content = "
        DEFINE TABLE clicks (
            click_id BIGINT, 
            user_id STRING, 
            event_time TIMESTAMP
        ) WITH STREAM (
            time_attribute = 'event_time',
            watermark = '5 seconds'
        );
    ",
    Result = dsl_parser:parse_content(Content),
    Tables = maps:get(tables, Result),
    
    ?assertEqual(1, length(Tables)),
    Table = hd(Tables),
    ?assertEqual(<<"clicks">>, maps:get(name, Table)),
    
    StreamConfig = maps:get(stream_config, Table),
    ?assertNotEqual(undefined, StreamConfig),
    ?assertEqual(<<"event_time">>, maps:get(time_attribute, StreamConfig)),
    ?assertEqual(<<"5 seconds">>, maps:get(watermark, StreamConfig)).

%% @doc Тест: множественные таблицы
parse_multiple_tables_test() ->
    Content = "
        DEFINE TABLE table1 (id INT, value STRING);
        DEFINE TABLE table2 (id INT, data BIGINT);
        DEFINE TABLE table3 (name STRING, count INT);
    ",
    Result = dsl_parser:parse_content(Content),
    Tables = maps:get(tables, Result),
    
    ?assertEqual(3, length(Tables)),
    
    TableNames = [maps:get(name, T) || T <- Tables],
    ?assert(lists:member(<<"table1">>, TableNames)),
    ?assert(lists:member(<<"table2">>, TableNames)),
    ?assert(lists:member(<<"table3">>, TableNames)).

%% @doc Тест: определение ML модели
parse_model_definition_test() ->
    Content = "
        DEFINE MODEL sentiment_analyzer (
            path = '/ml/sentiment.bin',
            input_schema = 'text',
            output_schema = 'sentiment_score'
        );
    ",
    Result = dsl_parser:parse_content(Content),
    Models = maps:get(models, Result),
    
    ?assertEqual(1, length(Models)),
    Model = hd(Models),
    ?assertEqual(<<"sentiment_analyzer">>, maps:get(name, Model)),
    ?assertEqual(<<"/ml/sentiment.bin">>, maps:get(path, Model)),
    ?assertEqual(<<"text">>, maps:get(input_schema, Model)),
    ?assertEqual(<<"sentiment_score">>, maps:get(output_schema, Model)).

%% @doc Тест: определение графа
parse_graph_definition_test() ->
    Content = "
        DEFINE GRAPH knowledge_graph (
            vertices => entities(entity_id),
            edges => relationships(source_id, target_id)
        );
    ",
    Result = dsl_parser:parse_content(Content),
    Graphs = maps:get(graphs, Result),
    
    ?assertEqual(1, length(Graphs)),
    Graph = hd(Graphs),
    ?assertEqual(<<"knowledge_graph">>, maps:get(name, Graph)),
    ?assertEqual(<<"entities">>, maps:get(vertices_table, Graph)),
    ?assertEqual(<<"entity_id">>, maps:get(vertices_id_col, Graph)),
    ?assertEqual(<<"relationships">>, maps:get(edges_table, Graph)),
    ?assertEqual(<<"source_id">>, maps:get(edges_src_col, Graph)),
    ?assertEqual(<<"target_id">>, maps:get(edges_dst_col, Graph)).

%% @doc Тест: простое представление (VIEW)
parse_simple_view_test() ->
    Content = "
        CREATE VIEW premium_users AS 
        SELECT id, name, subscription_type 
        FROM users 
        WHERE subscription_type = 'premium';
    ",
    Result = dsl_parser:parse_content(Content),
    Views = maps:get(views, Result),
    
    ?assertEqual(1, maps:size(Views)),
    ?assert(maps:is_key(<<"premium_users">>, Views)),
    
    Query = maps:get(<<"premium_users">>, Views),
    ?assert(string:str(Query, "SELECT") > 0),
    ?assert(string:str(Query, "premium_users") > 0).

%% @doc Тест: CREATE TABLE AS SELECT
parse_create_table_as_test() ->
    Content = "
        CREATE TABLE aggregated_data AS 
        SELECT category, SUM(amount) as total 
        FROM transactions 
        GROUP BY category;
    ",
    Result = dsl_parser:parse_content(Content),
    Views = maps:get(views, Result),
    
    ?assertEqual(1, maps:size(Views)),
    ?assert(maps:is_key(<<"aggregated_data">>, Views)).

%% @doc Тест: INSERT statement
parse_insert_statement_test() ->
    Content = "
        INSERT INTO output_table 
        SELECT user_id, event_type, COUNT(*) as cnt
        FROM events
        GROUP BY user_id, event_type;
    ",
    Result = dsl_parser:parse_content(Content),
    Inserts = maps:get(inserts, Result),
    
    ?assertEqual(1, length(Inserts)),
    InsertStmt = hd(Inserts),
    ?assert(string:str(InsertStmt, "INSERT INTO output_table") > 0),
    ?assert(string:str(InsertStmt, "SELECT") > 0).
