-- Пример SQL-DSL пайплайна
-- Определение таблиц и трансформаций

-- Таблица пользователей с стримингом
DEFINE TABLE users (
  id STRING,
  name STRING,
  email STRING,
  age INT,
  created_at TIMESTAMP
) WITH STREAM (
  time_attribute = 'created_at',
  watermark = '10 seconds'
);

-- Таблица событий кликов
DEFINE TABLE clicks (
  user_id STRING,
  page_url STRING,
  event_time TIMESTAMP,
  session_id STRING
) WITH STREAM (
  time_attribute = 'event_time',
  watermark = '5 seconds'
);

-- Представление активных пользователей
CREATE VIEW active_users AS
SELECT id, name, email, age
FROM users
WHERE age >= 18;

-- Представление кликов по страницам
CREATE VIEW page_stats AS
SELECT 
  page_url,
  COUNT(*) as click_count,
  COUNT(DISTINCT user_id) as unique_users
FROM clicks
GROUP BY page_url;

-- Вставка в выходную таблицу
INSERT INTO user_activity
SELECT 
  u.id,
  u.name,
  COUNT(c.user_id) as total_clicks
FROM active_users u
LEFT JOIN clicks c ON u.id = c.user_id
GROUP BY u.id, u.name;
