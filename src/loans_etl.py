# src/loans_etl.py

import argparse
import logging
import getpass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, DateType
)
from pyspark.sql.functions import (
    col, current_timestamp, to_date, regexp_replace, when, lit
)

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
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.enabled", "true") \
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

    loans_schema = StructType([
        StructField("loan_id", StringType(), False),
        StructField("member_id", StringType(), False),
        StructField("loan_amount", DecimalType(18,2), True),
        StructField("funded_amount", DecimalType(18,2), True),
        StructField("loan_term_months", StringType(), True),
        StructField("interest_rate", DecimalType(5,2), True),
        StructField("monthly_installment", DecimalType(18,2), True),
        StructField("issue_date", StringType(), True),  # convert later
        StructField("loan_status", StringType(), True),
        StructField("loan_purpose", StringType(), True),
        StructField("loan_title", StringType(), True)
    ])

    df = spark.read.format("csv") \
        .option("header", True) \
        .schema(loans_schema) \
        .load(input_path)

    df.cache()
    logger.info(f"Read {df.count()} rows from input data")
    return df

# -------------------------
# Function: Clean & Transform Data
# -------------------------
def clean_loans_data(df: DataFrame) -> DataFrame:
    logger.info("Starting data cleaning...")

    # Convert issue_date to DateType
    df = df.withColumn("issue_date", to_date(col("issue_date"), "yyyy-MM-dd"))

    # Add audit columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("ingested_by", lit("loans_etl_pipeline"))

    # Drop rows with nulls in key columns
    key_columns = [
        "loan_amount", "funded_amount", "loan_term_months",
        "interest_rate", "monthly_installment", "issue_date",
        "loan_status", "loan_purpose"
    ]
    valid_df = df.dropna(subset=key_columns)
    invalid_df = df.subtract(valid_df)

    # Optional: write invalid rows to a quarantine path
    if invalid_df.count() > 0:
        invalid_df.write.mode("overwrite").parquet("bad_records/loans_invalid")
        logger.info(f"Quarantined {invalid_df.count()} invalid rows")

    # Convert loan term months to years
    df = valid_df.withColumn(
        "loan_term_years",
        (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")
    ).drop("loan_term_months")

    # Standardize loan purpose
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

    logger.info(f"Rows after cleaning: {df.count()}")
    logger.info("Data cleaning completed")
    return df

# -------------------------
# Function: Write Output
# -------------------------
def write_output(df: DataFrame, parquet_path: str, csv_path: str):
    logger.info(f"Writing Parquet output to: {parquet_path}")
    df.write \
      .mode("overwrite") \
      .partitionBy("loan_status") \
      .parquet(parquet_path)

    logger.info(f"Writing CSV output to: {csv_path}")
    df.write \
      .mode("overwrite") \
      .option("header", True) \
      .csv(csv_path)

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

    spark = None
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
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
