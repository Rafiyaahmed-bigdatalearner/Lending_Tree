# Lending_Tree ETL Pipeline

A scalable ETL pipeline built with **PySpark** to process and clean loan data for analytics, reporting, and machine learning.

---

## Project Overview
The Lending_Tree ETL pipeline ingests raw loan data, performs data cleaning and transformation, handles invalid records, and outputs processed data in **both Parquet and CSV formats**. It is designed for large-scale distributed processing using Spark on YARN or a local Spark setup.

**Key goals:**
- Ensure data quality and consistency.
- Enable analytics and ML applications.
- Maintain a clean and professional data pipeline workflow.

---

## Tech Stack
- **Python 3.x**
- **PySpark** (ETL & large-scale data processing)
- **YARN / Spark Cluster** support
- **CSV & Parquet** file formats
- **Logging & Error Handling**

---

## Folder Structure
Lending_Tree/
├─ bad_records/ # Invalid or quarantined loan data
├─ config/ # Configuration files (config.yaml)
├─ data/ # Raw input datasets
├─ output/ # Processed output datasets
├─ src/ # ETL scripts (loans_etl.py)
├─ tests/ # Unit tests for ETL functions
├─ docs/ # Documentation, diagrams, or screenshots
├─ .gitignore
├─ README.md
└─ requirements.txt


---

## Key Features
1. **Schema validation:** Ensures input CSVs conform to a predefined schema.  
2. **Data cleaning & transformation:**  
   - Converts dates, handles nulls, standardizes loan purposes.  
   - Converts loan term from months to years.  
3. **Audit columns:** Adds ingestion timestamp and ETL pipeline metadata.  
4. **Quarantine invalid rows:** Automatically stores invalid records in `bad_records/`.  
5. **Flexible output formats:** Writes cleaned data to Parquet (partitioned by `loan_status`) and CSV.  
6. **Logging & error handling:** Provides detailed logs for monitoring and debugging.

---

## How to Run

### Local Python (with dependencies)
```bash
git clone https://github.com/Rafiyaahmed-bigdatalearner/Lending_Tree.git
cd Lending_Tree
pip install -r requirements.txt

python src/loans_etl.py \
  --input data/loans_data.csv \
  --parquet_output output/parquet \
  --csv_output output/csv

| loan_id | member_id | loan_amount | funded_amount | interest_rate | loan_status | loan_purpose       | loan_term_years |
| ------- | --------- | ----------- | ------------- | ------------- | ----------- | ------------------ | --------------- |
| L001    | M001      | 10000.00    | 10000.00      | 12.5          | Fully Paid  | credit_card        | 3               |
| L002    | M002      | 5000.00     | 5000.00       | 9.9           | Current     | debt_consolidation | 2               |

![ETL Flow](docs/ETL_flow.png.png)

## Future Improvements

Add unit tests for cleaning and transformation functions.

Integrate with cloud storage (AWS S3, GCS) for production-ready ETL.

Implement CI/CD pipelines using GitHub Actions.

Add real-time processing using Kafka or Spark Streaming.

Enhance logging with dynamic log levels and alerts.
