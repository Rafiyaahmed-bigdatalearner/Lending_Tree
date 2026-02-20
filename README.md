# Loans ETL Pipeline

## Overview
Enterprise-grade ETL pipeline for loan data using PySpark. Reads CSV → cleans/transforms → writes partitioned Parquet & CSV. Configurable via config.yaml.

## ETL Flow
CSV Input → Clean & Transform → Partitioned Parquet & CSV → Quarantine Invalid Rows

## Project Structure
project_root/
├── config/
│   └── config.yaml
├── src/
│   └── loans_etl.py
├── requirements.txt
├── .gitignore
└── README.md

## Key Features
- Schema enforcement
- Partitioned output
- Audit columns
- Configurable paths
- Data quality handling

## How to Run
1. Install requirements
2. Update config/config.yaml
3. Run `python src/loans_etl.py`

## Future Improvements
- Incremental loads
- SCD Type 2
- Delta Lake integration
- Airflow scheduling
