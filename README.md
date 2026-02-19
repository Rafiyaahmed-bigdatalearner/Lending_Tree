## Overview
This project builds a production-style PySpark ETL pipeline that:
- Ingests raw CSV loan data
- Cleans and transforms columns
- Standardizes loan purpose
- Converts term to years
- Writes output to Parquet and CSV
## Technologies Used
- PySpark
- Hive
- YARN
- Python argparse
- Logging
- 
## How to Run

spark-submit loans_etl.py \
--input <input_path> \
--parquet_output <parquet_path> \
--csv_output <csv_path>
