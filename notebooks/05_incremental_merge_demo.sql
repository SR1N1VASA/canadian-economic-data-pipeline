-- Databricks notebook source
-- DBTITLE 1,Create a safe demo copy
CREATE OR REPLACE TABLE boc_silver_merge_demo
USING DELTA
AS
SELECT *
FROM boc_silver_exchange_rates;

-- COMMAND ----------

-- DBTITLE 1,Create fake incoming incremental data
CREATE OR REPLACE TEMP VIEW incoming_silver AS

WITH latest_record AS (
    SELECT
        observation_date,
        series,
        value
    FROM boc_silver_merge_demo
    ORDER BY observation_date DESC
    LIMIT 1
)

SELECT
    observation_date,
    series,
    value + 0.01 AS value
FROM latest_record

UNION ALL

SELECT
    DATE_ADD(observation_date, 1) AS observation_date,
    series,
    value + 0.02 AS value
FROM latest_record;

-- COMMAND ----------

-- DBTITLE 1,Perform the merge
MERGE INTO boc_silver_merge_demo AS target

USING incoming_silver AS source

ON target.observation_date = source.observation_date
AND target.series = source.series

WHEN MATCHED THEN
    UPDATE SET
        target.value = source.value

WHEN NOT MATCHED THEN
    INSERT (
        observation_date,
        series,
        value
    )
    VALUES (
        source.observation_date,
        source.series,
        source.value
    );

-- COMMAND ----------

-- DBTITLE 1,Verify the result
SELECT *
FROM boc_silver_merge_demo
ORDER BY observation_date DESC
LIMIT 10;