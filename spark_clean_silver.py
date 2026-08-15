#!/usr/bin/env python3
"""
Nepal Economic Sentinel & Financial Intelligence Suite - Silver Layer Cleaner
----------------------------------------------------------------------------
Author: Data Engineering Team
Description: Reads raw Bronze JSON payloads from MinIO (S3) for a given execution 
             date (YYYY-MM-DD), enforces strict schemas, cleans data types, 
             deduplicates records, and writes to PostgreSQL Silver tables via JDBC.
"""

import sys
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, IntegerType, ArrayType, TimestampType
)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkSilverCleaner")

# ---------------------------------------------------------------------------
# CONFIGURATION & ENV VARIABLES
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "nepal-sentinel-lake"

POSTGRES_HOST = os.getenv("DB_HOST", "postgres-warehouse")
POSTGRES_PORT = os.getenv("DB_PORT", "5432")
POSTGRES_DB = os.getenv("DB_NAME", "financial_sentinel")
POSTGRES_USER = os.getenv("DB_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ---------------------------------------------------------------------------
# EXPLICIT SCHEMAS (BRONZE RAW LAYER)
# ---------------------------------------------------------------------------
# 1. NRB Forex Raw Schema
NRB_FOREX_SCHEMA = StructType([
    StructField("status", StructType([
        StructField("code", IntegerType(), True),
        StructField("message", StringType(), True)
    ]), True),
    StructField("data", StructType([
        StructField("payload", ArrayType(
            StructType([
                StructField("date", StringType(), True),
                StructField("rates", ArrayType(
                    StructType([
                        StructField("currency", StructType([
                            StructField("iso3", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("unit", IntegerType(), True)
                        ]), True),
                        StructField("buy", StringType(), True),
                        StructField("sell", StringType(), True)
                    ])
                ), True)
            ])
        ), True)
    ]), True)
])

# 2. NEPSE Floorsheet Raw Schema
NEPSE_FLOORSHEET_SCHEMA = StructType([
    StructField("status", StringType(), True),
    StructField("date", StringType(), True),
    StructField("trades", ArrayType(
        StructType([
            StructField("contract_id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("buyer_broker", StringType(), True),
            StructField("seller_broker", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("rate", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("trade_time", StringType(), True)
        ])
    ), True)
])


def create_spark_session() -> SparkSession:
    """Creates Spark Session pre-configured for S3A (MinIO) and Postgres JDBC."""
    logger.info("Initializing Spark Session with S3A and Postgres configs...")
    return (
        SparkSession.builder
        .appName("NepalSentinel-SilverCleaner")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def process_nrb_forex(spark: SparkSession, execution_date: str):
    """Parses raw NRB JSON, normalizes rates, cleans schema, and writes to Postgres."""
    date_path = execution_date.replace("-", "/")
    s3_path = f"s3a://{BUCKET_NAME}/raw/nrb/forex/{date_path}/forex.json"
    logger.info(f"Reading NRB Forex payload from S3: {s3_path}")

    try:
        raw_df = spark.read.option("multiLine", "true").schema(NRB_FOREX_SCHEMA).json(s3_path)
    except Exception as e:
        logger.error(f"Failed to read NRB file at {s3_path}: {e}")
        return

    # Check if empty
    if raw_df.rdd.isEmpty():
        logger.warning("NRB Forex DataFrame is empty. Skipping processing.")
        return

    # Unnest array structs into rows
    exploded_df = raw_df.select(
        F.explode_outer("data.payload").alias("payload_item")
    ).select(
        F.col("payload_item.date").alias("raw_date"),
        F.explode_outer("payload_item.rates").alias("rate_item")
    )

    cleaned_forex_df = exploded_df.select(
        F.to_date(F.col("raw_date")).alias("rate_date"),
        F.upper(F.col("rate_item.currency.iso3")).alias("currency_iso3"),
        F.col("rate_item.currency.name").alias("currency_name"),
        F.col("rate_item.currency.unit").cast(IntegerType()).alias("currency_unit"),
        F.col("rate_item.buy").cast(DoubleType()).alias("buying_rate"),
        F.col("rate_item.sell").cast(DoubleType()).alias("selling_rate"),
        F.current_timestamp().alias("ingested_at")
    ).filter(F.col("currency_iso3").isNotNull())

    # Deduplicate by rate_date and currency_iso3
    deduped_df = cleaned_forex_df.dropDuplicates(["rate_date", "currency_iso3"])

    logger.info(f"Writing {deduped_df.count()} NRB Forex rows to Silver PostgreSQL...")
    (
        deduped_df.write
        .mode("append")
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.nrb_forex_rates")
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .save()
    )
    logger.info("NRB Forex Silver processing complete.")


def process_nepse_floorsheet(spark: SparkSession, execution_date: str):
    """Parses NEPSE raw trade ticks, enforces numeric types, deduplicates, and writes to Postgres."""
    date_path = execution_date.replace("-", "/")
    s3_path = f"s3a://{BUCKET_NAME}/raw/nepse/floorsheet/{date_path}/floorsheet.json"
    logger.info(f"Reading NEPSE Floorsheet payload from S3: {s3_path}")

    try:
        raw_df = spark.read.option("multiLine", "true").schema(NEPSE_FLOORSHEET_SCHEMA).json(s3_path)
    except Exception as e:
        logger.error(f"Failed to read NEPSE file at {s3_path}: {e}")
        return

    if raw_df.rdd.isEmpty():
        logger.warning("NEPSE DataFrame is empty. Skipping processing.")
        return

    # Explode trade array
    trades_df = raw_df.select(
        F.col("date").alias("payload_date"),
        F.explode_outer("trades").alias("trade")
    )

    cleaned_trades_df = trades_df.select(
        F.col("trade.contract_id").alias("contract_id"),
        F.to_date(F.col("payload_date")).alias("trade_date"),
        F.upper(F.trim(F.col("trade.symbol"))).alias("symbol"),
        F.col("trade.buyer_broker").cast(IntegerType()).alias("buyer_broker"),
        F.col("trade.seller_broker").cast(IntegerType()).alias("seller_broker"),
        F.col("trade.quantity").cast(LongType()).alias("quantity"),
        F.col("trade.rate").cast(DoubleType()).alias("rate"),
        F.col("trade.amount").cast(DoubleType()).alias("total_amount"),
        F.col("trade.trade_time").alias("trade_time_str"),
        F.current_timestamp().alias("processed_at")
    ).filter(
        F.col("contract_id").isNotNull() & (F.col("quantity") > 0)
    )

    # Deduplicate trades by unique contract ID
    final_trades_df = cleaned_trades_df.dropDuplicates(["contract_id"])

    logger.info(f"Writing {final_trades_df.count()} NEPSE trade records to Silver PostgreSQL...")
    (
        final_trades_df.write
        .mode("append")
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.nepse_floorsheet_trades")
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .save()
    )
    logger.info("NEPSE Floorsheet Silver processing complete.")


# ---------------------------------------------------------------------------
# MAIN EXECUTION ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Missing execution date argument! Usage: python spark_clean_silver.py YYYY-MM-DD")
        sys.exit(1)

    execution_date_arg = sys.argv[1]
    logger.info(f"--- STARTING SILVER SPARK PROCESSING FOR DATE: {execution_date_arg} ---")

    spark_session = create_spark_session()

    try:
        process_nrb_forex(spark_session, execution_date_arg)
        process_nepse_floorsheet(spark_session, execution_date_arg)
        logger.info("--- SILVER SPARK PROCESSING FINISHED SUCCESSFULLY ---")
    except Exception as ex:
        logger.critical(f"Unhandled exception during Spark job: {ex}", exc_info=True)
        sys.exit(1)
    finally:
        spark_session.stop()
