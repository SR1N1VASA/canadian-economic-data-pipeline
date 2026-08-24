-- Databricks notebook source
USE SCHEMA IDENTIFIER(:schema_name);

CREATE OR REPLACE TABLE IDENTIFIER(:gold_table_name)
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

FROM IDENTIFIER(:silver_table_name)

GROUP BY
    YEAR(observation_date),
    MONTH(observation_date),
    series;

-- COMMAND ----------

SELECT *
FROM IDENTIFIER(:gold_table_name)
ORDER BY year, month;