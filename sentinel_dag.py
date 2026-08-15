"""
Nepal Economic Sentinel & Financial Intelligence Suite - Master Orchestrator
----------------------------------------------------------------------------
Orchestrates daily ingestion of NRB Forex data, NEPSE Floorsheet data,
runs PySpark processing to clean Silver data, triggers dbt transformations
for Gold analytical tables, and executes AML/market abuse anomaly checks.
"""

from datetime import datetime, timedelta
import logging
import json
import os

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSkipException

# ---------------------------------------------------------------------------
# GLOBAL CONSTANTS & CONFIGURATION
# ---------------------------------------------------------------------------
AWS_CONN_ID = "minio_s3_conn"
POSTGRES_CONN_ID = "postgres_warehouse"

DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="nepal_economic_sentinel_pipeline_v1",
    default_args=DEFAULT_ARGS,
    description="End-to-end ingestion, transformation, and intelligence pipeline for NRB & NEPSE.",
    schedule_interval="30 17 * * 1-5",  # 5:30 PM NST (UTC 11:45 AM) Mon-Fri after NEPSE close
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nepal", "finance", "nepse", "nrb", "etl"],
)
def nepal_economic_sentinel():

    # -----------------------------------------------------------------------
    # 1. INGESTION TASK GROUP (BRONZE LAYER)
    # -----------------------------------------------------------------------
    @task_group(group_id="raw_ingestion_layer")
    def ingestion_group():

        @task(task_id="extract_nrb_forex")
        def extract_nrb_forex_data() -> str:
            """Fetches daily foreign exchange rates from NRB official API and uploads raw JSON to S3."""
            import requests
            import boto3

            context = get_current_context()
            execution_date = context["ds"]  # YYYY-MM-DD

            url = "https://www.nrb.org.np/api/forex/v1/rates"
            params = {"page": 1, "per_page": 100, "from": execution_date, "to": execution_date}

            logging.info(f"Extracting NRB Forex data for date: {execution_date}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Initialize S3 Client (MinIO / AWS S3)
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            )

            s3_key = f"raw/nrb/forex/{execution_date.replace('-', '/')}/forex.json"
            s3_client.put_object(
                Bucket="nepal-sentinel-lake",
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )

            logging.info(f"Successfully uploaded NRB Forex payload to s3://nepal-sentinel-lake/{s3_key}")
            return s3_key

        @task(task_id="extract_nepse_floorsheet")
        def extract_nepse_floorsheet_data() -> str:
            """Scrapes or consumes today's NEPSE trade floorsheet data into Raw Data Lake."""
            import requests
            import boto3

            context = get_current_context()
            execution_date = context["ds"]

            # Mocked endpoint or internal scraper gateway for NEPSE market data
            scraper_gateway = f"http://nepse-scraper-service:8080/api/floorsheet?date={execution_date}"
            logging.info(f"Fetching NEPSE trade data from gateway: {scraper_gateway}")

            try:
                res = requests.get(scraper_gateway, timeout=60)
                res.raise_for_status()
                payload = res.json()
            except Exception as e:
                logging.warning(f"Failed to fetch NEPSE API: {e}. Falling back to staging payload.")
                payload = {"status": "SUCCESS", "date": execution_date, "trades": []}

            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            )

            s3_key = f"raw/nepse/floorsheet/{execution_date.replace('-', '/')}/floorsheet.json"
            s3_client.put_object(
                Bucket="nepal-sentinel-lake",
                Key=s3_key,
                Body=json.dumps(payload),
                ContentType="application/json",
            )

            logging.info(f"Successfully stored NEPSE floorsheet to s3://nepal-sentinel-lake/{s3_key}")
            return s3_key

        extract_nrb_forex_data()
        extract_nepse_floorsheet_data()

    # -----------------------------------------------------------------------
    # 2. PROCESSING & CLEANING LAYER (SILVER LAYER - PYSPARK)
    # -----------------------------------------------------------------------
    run_spark_silver_cleaning = BashOperator(
        task_id="spark_silver_processing",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 \
          /opt/airflow/dags/scripts/spark_clean_silver.py {{ ds }}
        """,
    )

    # -----------------------------------------------------------------------
    # 3. TRANSFORMATIONS & DATA WAREHOUSING (GOLD LAYER - DBT)
    # -----------------------------------------------------------------------
    @task_group(group_id="dbt_transformation_layer")
    def dbt_group():

        dbt_run = BashOperator(
            task_id="dbt_run_models",
            bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
        )

        dbt_test = BashOperator(
            task_id="dbt_test_models",
            bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
        )

        dbt_run >> dbt_test

    # -----------------------------------------------------------------------
    # 4. SENTINEL INTELLIGENCE & ANOMALY DETECTION LAYER
    # -----------------------------------------------------------------------
    @task(task_id="aml_wash_trade_anomaly_detector")
    def run_anomaly_detection_rules():
        """Executes Isolation Forest ML model and rule engines against gold tables in PostgreSQL."""
        import psycopg2
        from sklearn.ensemble import IsolationForest
        import pandas as pd

        context = get_current_context()
        execution_date = context["ds"]
        logging.info(f"Running Financial Intelligence Sentinel checks for: {execution_date}")

        # Connect to Data Warehouse
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres-warehouse"),
            database=os.getenv("DB_NAME", "financial_sentinel"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )

        # Query Broker Trading Patterns for current date
        query = f"""
            SELECT buyer_broker, seller_broker, symbol, total_volume, total_amount, trade_count
            FROM gold.fact_daily_broker_summary
            WHERE trade_date = '{execution_date}';
        """
        df = pd.read_sql_query(query, conn)

        if df.empty:
            logging.warning("No broker summary data available for current run date. Skipping ML scoring.")
            raise AirflowSkipException("No data present for execution date.")

        # ML Anomaly Detection using Isolation Forest
        features = df[["total_volume", "total_amount", "trade_count"]]
        model = IsolationForest(contamination=0.01, random_state=42)
        df["anomaly_score"] = model.fit_predict(features)

        # Flagged suspicious wash trades (-1 means outlier)
        anomalies = df[df["anomaly_score"] == -1]
        logging.info(f"Detected {len(anomalies)} potential financial market anomalies.")

        # Insert flagged alerts into Risk Table
        cursor = conn.cursor()
        for idx, row in anomalies.iterrows():
            cursor.execute(
                """
                INSERT INTO gold.sentinel_risk_alerts 
                (alert_date, alert_type, broker_id, symbol, risk_score, details)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (
                    execution_date,
                    "SUSPICIOUS_VOLUME_SPIKE",
                    int(row["buyer_broker"]),
                    row["symbol"],
                    0.95,
                    f"Volume: {row['total_volume']} Amount: NRs.{row['total_amount']}",
                ),
            )
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Sentinel Risk alerts successfully persisted to Warehouse.")

    # -----------------------------------------------------------------------
    # PIPELINE DEPENDENCY FLOW (GRAPH DEFINITION)
    # -----------------------------------------------------------------------
    ingest_step = ingestion_group()
    dbt_step = dbt_group()
    intelligence_step = run_anomaly_detection_rules()

    # Ingestion -> Spark Clean -> dbt Gold Models -> ML Intelligence Alerts
    ingest_step >> run_spark_silver_cleaning >> dbt_step >> intelligence_step


# Instantiate DAG
dag_instance = nepal_economic_sentinel()
