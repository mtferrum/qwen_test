-- Пример SQL-DSL пайплайна для Platform Compiler
-- Этот файл описывает бизнес-логику обработки данных

-- Определение таблиц с поддержкой стриминга
DEFINE TABLE raw_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP,
    payload STRUCT<action: STRING, value: DOUBLE>
) WITH STREAM (
    time_attribute = 'timestamp',
    watermark = '5 seconds',
    allowed_lateness = '10 seconds'
);

DEFINE TABLE user_profiles (
    user_id STRING,
    name STRING,
    age INT,
    country STRING
);

-- Определение ML-модели
DEFINE MODEL fraud_detector (
    path = 'hdfs:///models/fraud_detector_v2.pkl',
    input_schema = 'STRUCT<amount: DOUBLE, merchant: STRING, hour: INT>',
    output_schema = 'STRUCT<score: DOUBLE, is_fraud: BOOLEAN>'
);

-- Определение графа для анализа связей
DEFINE GRAPH user_network (
    vertices => users(user_id),
    edges => transactions(from_user, to_user)
);

-- Представления для трансформации данных
CREATE VIEW enriched_events AS
SELECT 
    e.event_id,
    e.user_id,
    e.event_type,
    e.timestamp,
    u.name AS user_name,
    u.country,
    APPLY_MODEL(fraud_detector, e.payload) AS fraud_score
FROM raw_events e
JOIN user_profiles u ON e.user_id = u.user_id
WHERE e.event_type IN ('purchase', 'transfer');

CREATE VIEW hourly_stats AS
SELECT 
    TUMBLE_START(timestamp, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(timestamp, INTERVAL '1' HOUR) AS window_end,
    event_type,
    COUNT(*) AS event_count,
    AVG(payload.value) AS avg_value
FROM enriched_events
GROUP BY TUMBLE(timestamp, INTERVAL '1' HOUR), event_type;

-- Вставка результатов в целевые таблицы
INSERT INTO analytics.events_hourly
SELECT 
    window_start,
    window_end,
    event_type,
    event_count,
    avg_value
FROM hourly_stats;

INSERT INTO ml.fraud_predictions
SELECT 
    event_id,
    user_id,
    timestamp,
    fraud_score.score,
    fraud_score.is_fraud
FROM enriched_events
WHERE fraud_score.is_fraud = TRUE;
