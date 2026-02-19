# src/loans_etl.py

import argparse
import logging
import getpass
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_replace, col, when

# -------------------------
# Logging Configuration
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Function: Create Spark Session
# -------------------------
def create_spark_session():
    username = getpass.getuser()
    spark = SparkSession.builder \
        .config("spark.ui.port", "0") \
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
        .config("spark.shuffle.useOldFetchProtocol", "true") \
        .enableHiveSupport() \
        .master("yarn") \
        .appName("Loans_ETL_Pipeline") \
        .getOrCreate()
    logger.info("Spark session created successfully")
    return spark

# -------------------------
# Function: Read CSV Data
# -------------------------
def read_loans_data(spark, input_path):
    logger.info(f"Reading loans data from: {input_path}")
    loans_schema = """
        loan_id string,
        member_id string,
        loan_amount float,
        funded_amount float,
        loan_term_months string,
        interest_rate float,
        monthly_installment float,
        issue_date string,
        loan_status string,
        loan_purpose string,
        loan_title string
    """
    df = spark.read.format("csv") \
        .option("header", True) \
        .schema(loans_schema) \
        .load(input_path)
    logger.info(f"Read {df.count()} rows from input data")
    return df

# -------------------------
# Function: Clean & Transform Data
# -------------------------
def clean_loans_data(df):
    logger.info("Starting data cleaning...")

    # Add ingestion timestamp
    df = df.withColumn("ingest_date", current_timestamp())

    # Drop rows with nulls in key columns
    columns_to_check = [
        "loan_amount", "funded_amount", "loan_term_months",
        "interest_rate", "monthly_installment", "issue_date",
        "loan_status", "loan_purpose"
    ]
    df = df.na.drop(subset=columns_to_check)
    logger.info(f"Rows after dropping nulls: {df.count()}")

    # Convert loan term months to years
    df = df.withColumn(
        "loan_term_years",
        (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")
    ).drop("loan_term_months")

    # Clean loan purpose
    loan_purpose_lookup = [
        "debt_consolidation", "credit_card", "home_improvement",
        "other", "major_purchase", "medical", "small_business",
        "car", "vacation", "moving", "house",
        "wedding", "renewable_energy", "educational"
    ]
    df = df.withColumn(
        "loan_purpose",
        when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other")
    )

    logger.info("Data cleaning completed")
    return df

# -------------------------
# Function: Write Output
# -------------------------
def write_output(df, parquet_path, csv_path):
    logger.info(f"Writing Parquet output to: {parquet_path}")
    df.write.format("parquet").mode("overwrite").save(parquet_path)

    logger.info(f"Writing CSV output to: {csv_path}")
    df.write.format("csv").option("header", True).mode("overwrite").save(csv_path)

    logger.info("Data successfully written to output paths")

# -------------------------
# Main Function
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Loans ETL Pipeline")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--parquet_output", required=True, help="Output Parquet path")
    parser.add_argument("--csv_output", required=True, help="Output CSV path")
    args = parser.parse_args()

    try:
        spark = create_spark_session()
        df = read_loans_data(spark, args.input)
        cleaned_df = clean_loans_data(df)
        write_output(cleaned_df, args.parquet_output, args.csv_output)
        logger.info("ETL Pipeline completed successfully")
    except Exception as e:
        logger.error("ETL Pipeline failed", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()


