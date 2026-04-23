import os
import logging
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, concat_ws,
    isnan, isnull, round as spark_round
)
from pyspark.sql.types import FloatType

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../logs/cleaning.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
RAW_PATH      = "../data/raw/household_power_consumption.txt"
PROCESSED_DIR = "../data/processed/"
OUTPUT_PATH   = PROCESSED_DIR + "clean_energy_data"

# ─────────────────────────────────────────
# STEP 1 — Start Spark Session
# ─────────────────────────────────────────
def create_spark():
    spark = SparkSession.builder \
        .appName("EnergyDataCleaning") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Spark session started.")
    return spark

# ─────────────────────────────────────────
# STEP 2 — Load Raw Data
# ─────────────────────────────────────────
def load_data(spark):
    log.info(f"Loading raw data from {RAW_PATH}...")
    df = spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .option("inferSchema", "false") \
        .csv(RAW_PATH)
    log.info(f"Raw row count: {df.count()}")
    log.info(f"Columns: {df.columns}")
    return df

# ─────────────────────────────────────────
# STEP 3 — Replace '?' with NULL
# ─────────────────────────────────────────
def replace_question_marks(df):
    log.info("Replacing '?' with NULL...")
    for c in df.columns:
        df = df.withColumn(c, when(col(c) == "?", None).otherwise(col(c)))

    # Count nulls per column and log
    null_counts = {c: df.filter(col(c).isNull()).count() for c in df.columns}
    log.info(f"Null counts per column: {null_counts}")
    return df

# ─────────────────────────────────────────
# STEP 4 — Merge Date + Time into Timestamp
# ─────────────────────────────────────────
def merge_timestamp(df):
    log.info("Merging Date and Time into single Timestamp column...")
    df = df.withColumn(
        "Timestamp",
        to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy HH:mm:ss")
    )
    df = df.drop("Date", "Time")
    log.info("Timestamp column created. Date and Time columns dropped.")
    return df

# ─────────────────────────────────────────
# STEP 5 — Cast All Columns to Float
# ─────────────────────────────────────────
NUMERIC_COLS = [
    "Global_active_power",
    "Global_reactive_power",
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3"
]

def cast_columns(df):
    log.info("Casting all numeric columns to FloatType...")
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast(FloatType()))
    log.info("Casting complete.")
    return df

# ─────────────────────────────────────────
# STEP 6 — Drop Rows with Critical NULLs
# ─────────────────────────────────────────
def drop_nulls(df):
    before = df.count()
    df = df.dropna(subset=["Timestamp", "Global_active_power", "Voltage"])
    after = df.count()
    dropped = before - after
    log.info(f"Dropped {dropped} rows with critical NULLs.")
    log.info(f"Remaining rows: {after}")
    return df

# ─────────────────────────────────────────
# STEP 7 — Flag Anomalies (don't drop, just tag)
# ─────────────────────────────────────────
def flag_anomalies(df):
    log.info("Flagging anomalies...")
    df = df.withColumn(
        "anomaly_flag",
        when(col("Voltage") < 220, "low_voltage")
        .when(col("Voltage") > 250, "high_voltage")
        .when(col("Global_active_power") > 6, "high_power")
        .when(col("Global_active_power") == 0, "zero_power")
        .otherwise("normal")
    )

    # Log anomaly counts
    anomaly_counts = df.groupBy("anomaly_flag").count().collect()
    for row in anomaly_counts:
        log.info(f"  {row['anomaly_flag']}: {row['count']} rows")

    return df

# ─────────────────────────────────────────
# STEP 8 — Save Cleaned Data (pandas writer)
# bypasses winutils issue on Windows
# ─────────────────────────────────────────
def save_data(df):
    import pandas as pd
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_file = PROCESSED_DIR + "clean_energy_data.csv"
    log.info(f"Converting to pandas and saving to {output_file}...")

    # Convert Spark DataFrame to pandas
    pandas_df = df.toPandas()

    # Save as single clean CSV file
    pandas_df.to_csv(output_file, index=False)

    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    log.info(f"Saved successfully. File size: {size_mb:.2f} MB")
    log.info(f"Total rows saved: {len(pandas_df)}")

# ─────────────────────────────────────────
# STEP 9 — Quick Summary
# ─────────────────────────────────────────
def print_summary(df):
    log.info("===== CLEANED DATA SUMMARY =====")
    log.info(f"Total rows     : {df.count()}")
    log.info(f"Total columns  : {len(df.columns)}")
    log.info(f"Columns        : {df.columns}")
    normal     = df.filter(col("anomaly_flag") == "normal").count()
    anomalies  = df.filter(col("anomaly_flag") != "normal").count()
    log.info(f"Normal rows    : {normal}")
    log.info(f"Anomaly rows   : {anomalies}")
    log.info("================================")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("STAGE 2 - DATA QUALITY CLEANING STARTED")
    log.info(f"Timestamp: {datetime.now()}")
    log.info("=" * 50)

    spark = create_spark()
    df    = load_data(spark)
    df    = replace_question_marks(df)
    df    = merge_timestamp(df)
    df    = cast_columns(df)
    df    = drop_nulls(df)
    df    = flag_anomalies(df)
    print_summary(df)
    save_data(df)

    log.info("STAGE 2 - CLEANING COMPLETE")
    spark.stop()

if __name__ == "__main__":
    main()