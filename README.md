Lending_Tree ETL Pipeline

A scalable ETL pipeline built with PySpark to process and clean loan data for analytics, reporting, and machine learning.

Project Overview

The Lending_Tree ETL pipeline ingests raw loan data, performs data cleaning and transformation, handles invalid records, and outputs processed data in both Parquet and CSV formats.

It is designed for large-scale distributed processing using Spark on YARN or a local Spark setup.

Key Goals

Ensure data quality and consistency

Enable analytics and ML applications

Maintain a clean and professional data pipeline workflow

Tech Stack

Python 3.x

PySpark (ETL & distributed data processing)

YARN / Spark Cluster

CSV & Parquet formats

Logging & Error Handling

Folder Structure
Lending_Tree/
│
├── bad_records/        # Invalid or quarantined loan data
├── config/             # Configuration files (config.yaml)
├── data/               # Raw input datasets
├── output/             # Processed output datasets
├── src/                # ETL scripts (loans_etl.py)
├── tests/              # Unit tests for ETL functions
├── docs/               # Documentation / diagrams
│
├── .gitignore
├── README.md
└── requirements.txt
How to Run
git clone https://github.com/Rafiyaahmed-bigdatalearner/Lending_Tree.git

cd Lending_Tree

pip install -r requirements.txt

python src/loans_etl.py \
  --input data/loans_data.csv \
  --parquet_output output/parquet \
  --csv_output output/csv
Sample Output
loan_id	member_id	loan_amount	funded_amount	interest_rate	loan_status	loan_purpose	loan_term_years
L001	M001	10000.00	10000.00	12.5	Fully Paid	credit_card	3
L002	M002	5000.00	5000.00	9.9	Current	debt_consolidation	2
ETL Flow Diagram

Future Improvements

Add unit tests for cleaning and transformation functions

Integrate with cloud storage (AWS S3, GCS)

Implement CI/CD pipelines using GitHub Actions

Add real-time processing using Kafka or Spark Streaming

Enhance logging with dynamic log levels and alerts
