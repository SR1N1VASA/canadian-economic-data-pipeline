# Canadian Economic Data Pipeline

Status: In Progress

## Objective
Build a data pipeline that retrieves public economic time-series data from the
Bank of Canada, validates and transforms the data, and publishes structured
datasets for analysis.

## Current Progress
- Created the initial Python ingestion component
- Retrieved public USD/CAD observations from the Bank of Canada Valet API
- Added request error handling and configurable date parameters
- Preserved the raw response with source and ingestion metadata

## Planned Work
- Databricks and PySpark transformations
- Data-quality checks
- Analytical data model
- Incremental loading
- Databricks workflow orchestration
- MLflow experimentation
- Power BI dashboard
