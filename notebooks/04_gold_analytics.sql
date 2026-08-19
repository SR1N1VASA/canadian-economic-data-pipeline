-- Databricks notebook source
CREATE OR REPLACE TABLE boc_gold_monthly_exchange_rates
USING DELTA
AS

SELECT
    YEAR(observation_date) AS year,
    MONTH(observation_date) AS month,
    series,
    COUNT(*) AS observation_count,
    AVG(value) AS average_rate,
    MIN(value) AS minimum_rate,
    MAX(value) AS maximum_rate
FROM boc_silver_exchange_rates
GROUP BY
    YEAR(observation_date),
    MONTH(observation_date),
    series;

-- COMMAND ----------

SELECT *
FROM boc_gold_monthly_exchange_rates
ORDER BY year, month;