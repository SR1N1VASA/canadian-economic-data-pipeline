# Canadian Economic Data Pipeline

**Status:** In Progress

## Overview

A data engineering project that ingests public USD/CAD exchange-rate data from the Bank of Canada Valet API and processes it through a Databricks Medallion Architecture.

The pipeline includes incremental ingestion, Delta Lake transformations, data-quality validation, and workflow orchestration using Databricks Lakeflow Jobs.

## Pipeline

`Bank of Canada API → Raw Volume → Bronze → Silver → Data Quality → Gold`

The workflow is orchestrated as:

`API Ingestion → Bronze Ingestion → Silver Transformation → Data Quality → Gold Analytics`

## Current Implementation

### Data Ingestion
- Retrieves `FXUSDCAD` data from the Bank of Canada Valet API
- Runs the API ingestion directly in Databricks using Python
- Stores raw JSON responses in a Unity Catalog Volume
- Uses the latest Silver observation date as a watermark for incremental API requests
- Supports runtime parameters for series, schema, volume, and table names

### Bronze
- Reads raw JSON files using PySpark
- Stores raw data in a Delta table
- Tracks source files using Databricks file metadata
- Processes and appends only new raw files

### Silver
- Converts nested API observations into structured records
- Stores `observation_date`, `series`, and `value`
- Processes only new Bronze files
- Tracks processed source files
- Uses Delta `MERGE` to update existing records or insert new records
- Uses `(observation_date, series)` as the business key

### Data Quality
Checks the Silver dataset for:
- Null dates
- Null values
- Duplicate business keys
- Invalid exchange-rate values
- Expected table schema

A failed data-quality check stops the pipeline before Gold processing.

### Gold
Creates monthly exchange-rate summaries using Spark SQL:
- Observation count
- Average rate
- Minimum rate
- Maximum rate

## Orchestration

The pipeline is orchestrated using a Databricks Lakeflow Job with dependent notebook tasks.

The Job includes:
- Job-level parameters
- Task dependencies
- API retry configuration
- Data Quality as a validation gate

The workflow is also defined as code using a Databricks Declarative Automation Bundle.

```text
databricks.yml
resources/
└── canadian_economic_pipeline.job.yml
```

The development bundle has been successfully deployed and the bundle-managed Lakeflow Job has completed successfully.

## Deployment

### Bundle Deployment

![Databricks Bundle Deployment](docs/images/bundle%20deployment.png)

### Lakeflow Job Run

![Databricks Lakeflow Job Run](docs/images/lakeflow%20job%20run.png)

## Technologies

- Python
- Databricks
- PySpark
- Apache Spark
- Spark SQL
- Delta Lake
- Unity Catalog
- Databricks Lakeflow Jobs
- Databricks Declarative Automation Bundles
- Bank of Canada Valet API
- Git / GitHub

## Repository Structure

```text
canadian-economic-data-pipeline/
├── databricks.yml
├── resources/
│   └── canadian_economic_pipeline.job.yml
├── notebooks/
│   ├── 00_api_ingestion.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_data_quality.py
│   ├── 04_gold_analytics.sql
│   └── 05_incremental_merge_demo.sql
├── docs/
│   └── images/
├── src/
│   └── ingest_boc.py
└── README.md
```

## Next Steps

- Scheduled pipeline execution
- Improved logging and monitoring
- Development and production deployment targets
- Additional Bank of Canada data series
- Power BI dashboard
- MLflow experimentation