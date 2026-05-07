-- ==========================================
-- EXAMPLE SQL-DSL PIPELINE
-- Fraud Detection with ML Inference and RAG
-- ==========================================

-- 1. DEFINE DATA SOURCES

-- Raw events from Kafka (streaming source)
DEFINE TABLE raw_events (
    event_id STRING,
    user_id STRING,
    transaction_amount DOUBLE,
    merchant_id STRING,
    event_time TIMESTAMP(3)
) WITH STREAM (
    time_attribute = event_time,
    watermark = '10 seconds',
    allowed_lateness = '30 seconds'
);

-- Historical transactions for feature engineering
DEFINE TABLE historical_transactions (
    transaction_id STRING,
    user_id STRING,
    amount DOUBLE,
    category STRING,
    transaction_date DATE
);

-- Merchant risk scores (static lookup table)
DEFINE TABLE merchant_risk (
    merchant_id STRING,
    risk_score DOUBLE,
    category STRING
);

-- 2. DEFINE ML MODELS

-- Fraud detection model
DEFINE MODEL fraud_detector (
    path = 'hdfs:///models/fraud_detector_v2.pkl',
    input_schema = STRUCT<user_id STRING, amount DOUBLE, merchant_risk DOUBLE>,
    output_schema = FLOAT
);

-- User embedding model for similarity detection
DEFINE MODEL user_embedder (
    path = 'hdfs:///models/user_encoder.onnx',
    input_schema = transaction_history ARRAY<DOUBLE>,
    output_schema = ARRAY<FLOAT>
);

-- 3. BUSINESS LOGIC TRANSFORMATIONS

-- Step 1: Enrich transactions with merchant risk
CREATE VIEW enriched_transactions AS
SELECT
    t.event_id,
    t.user_id,
    t.transaction_amount,
    t.merchant_id,
    t.event_time,
    m.risk_score AS merchant_risk,
    m.category AS merchant_category
FROM raw_events t
LEFT JOIN merchant_risk m ON t.merchant_id = m.merchant_id;

-- Step 2: Apply fraud detection model
CREATE VIEW fraud_predictions AS
SELECT
    e.event_id,
    e.user_id,
    e.transaction_amount,
    e.event_time,
    APPLY_MODEL(fraud_detector, 
        STRUCT(e.user_id, e.transaction_amount, e.merchant_risk)
    ) AS fraud_probability
FROM enriched_transactions e;

-- Step 3: Flag high-risk transactions
CREATE VIEW flagged_transactions AS
SELECT
    event_id,
    user_id,
    transaction_amount,
    fraud_probability,
    event_time,
    CASE 
        WHEN fraud_probability > 0.8 THEN 'HIGH_RISK'
        WHEN fraud_probability > 0.5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_level
FROM fraud_predictions;

-- Step 4: Windowed aggregation for monitoring
CREATE TABLE fraud_stats_5min AS
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTES) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTES) AS window_end,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN fraud_probability > 0.5 THEN 1 ELSE 0 END) AS suspicious_count,
    AVG(fraud_probability) AS avg_fraud_score
FROM fraud_predictions
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTES);

-- 4. OUTPUT SINKS

-- Insert flagged transactions to alert stream
INSERT INTO fraud_alerts
SELECT
    event_id,
    user_id,
    transaction_amount,
    fraud_probability,
    risk_level,
    event_time
FROM flagged_transactions
WHERE risk_level IN ('HIGH_RISK', 'MEDIUM_RISK');

-- Insert aggregated stats to monitoring table
INSERT INTO monitoring_output
SELECT * FROM fraud_stats_5min;
