# /opt/airflow/dags/etl_spark_historical.py
from __future__ import annotations

import os
import glob
import logging
from datetime import datetime, timezone

import requests
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_catfish"
hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_pyspark_etl",
    schedule="0 * * * *",   # hourly; change to @daily if you want
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "nyc_taxi", "pyspark", "snowflake"],
) as dag:

    # 1) Ensure Snowflake schema + table
 
    @task
    def ensure_objects(**context):
        schema = Variable.get("target_schema_raw", default_var="RAW")

        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)

                # Create schema and tables
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{schema}"."NYC_TAXI_TRIPS" (
                    PICKUP_DATETIME   TIMESTAMP_NTZ,
                    DROPOFF_DATETIME  TIMESTAMP_NTZ,
                    PICKUP_ZONE_ID    NUMBER,
                    DROPOFF_ZONE_ID   NUMBER,
                    PASSENGER_COUNT   NUMBER,
                    TRIP_DISTANCE     FLOAT,
                    TOTAL_AMOUNT      FLOAT,
                    LOAD_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """)

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{schema}"."NYC_TAXI_TRIPS_STG" (
                    PICKUP_DATETIME   TIMESTAMP_NTZ,
                    DROPOFF_DATETIME  TIMESTAMP_NTZ,
                    PICKUP_ZONE_ID    NUMBER,
                    DROPOFF_ZONE_ID   NUMBER,
                    PASSENGER_COUNT   NUMBER,
                    TRIP_DISTANCE     FLOAT,
                    TOTAL_AMOUNT      FLOAT
                    )
                """)

                # File format
                cur.execute(f"""
                    CREATE OR REPLACE FILE FORMAT "{schema}"."NYC_TAXI_CSV_FMT"
                    TYPE = CSV
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ('', 'NULL', 'null')
                """)

                # Stage (CREATE OR REPLACE acts like DROP + CREATE)
                cur.execute(f'''
                    CREATE OR REPLACE STAGE "{schema}"."NYC_TAXI_STAGE"
                ''')

                conn.commit()
                return f"Ensured DW objects in {schema}"
            except Exception as e:
                logging.exception("Error ensuring Snowflake objects")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e



    # 2) Extract + Transform with PySpark (local mode)
    @task
    def extract_transform_with_pyspark(**context) -> str:
        """
        Find the most recent available NYC TLC Parquet file for the CURRENT YEAR,
        download it, clean with PySpark, and write cleaned CSV.
        Returns the directory path that contains the Spark output (part-*.csv).
        """
        logical_date = context["logical_date"]        # e.g. 2025-11-27
        target_year = logical_date.year               # current year, e.g. 2025
        start_month = logical_date.month             # start from current month, e.g. 11

        # Step 1: find the most recent available month in the current year
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"

        chosen_month_str = None
        chosen_url = None

        for m in range(start_month, 0, -1):
            month_str = f"{target_year:04d}-{m:02d}"   # e.g. '2025-11'
            url = base_url.format(month=month_str)
            logging.info("Checking availability of TLC file: %s", url)

            try:
                head_resp = requests.head(url, timeout=15)
                if head_resp.ok:
                    chosen_month_str = month_str
                    chosen_url = url
                    logging.info("Found available TLC file for month %s", month_str)
                    break
                else:
                    logging.warning(
                        "TLC file for %s not available yet (status %s)",
                        month_str,
                        head_resp.status_code,
                    )
            except Exception as e:
                logging.warning("Error checking %s: %s", url, e)

        if not chosen_month_str or not chosen_url:
            raise RuntimeError(
                f"No TLC tripdata Parquet found for year {target_year} (checked months {start_month}..1)."
            )

        # Step 2: download the chosen Parquet file
        raw_path = f"/tmp/yellow_tripdata_{chosen_month_str}.parquet"
        cleaned_dir = f"/tmp/nyc_taxi_cleaned_{chosen_month_str}"

        os.makedirs("/tmp", exist_ok=True)

        logging.info("Downloading NYC taxi data from %s", chosen_url)
        try:
            resp = requests.get(chosen_url, stream=True, timeout=120)
            resp.raise_for_status()
            with open(raw_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            logging.info(
                "Downloaded Parquet file to %s (%.2f MB)",
                raw_path,
                os.path.getsize(raw_path) / 1_000_000,
            )
        except Exception as e:
            logging.exception("Failed to download NYC taxi data")
            raise e

        # Step 3: PySpark local session and cleaning
        try:
            spark = (
                SparkSession.builder
                .master("local[*]")
                .appName(f"NYC_Taxi_ETL_{chosen_month_str}")
                .getOrCreate()
            )

            # Read Parquet
            df_raw = spark.read.parquet(raw_path)

            df_clean = (
                df_raw
                .select(
                    col("tpep_pickup_datetime").alias("pickup_datetime"),
                    col("tpep_dropoff_datetime").alias("dropoff_datetime"),
                    col("PULocationID").cast("int").alias("pickup_zone_id"),
                    col("DOLocationID").cast("int").alias("dropoff_zone_id"),
                    col("passenger_count").cast("int").alias("passenger_count"),
                    col("trip_distance").cast("double").alias("trip_distance"),
                    col("total_amount").cast("double").alias("total_amount"),
                )
                .dropna(subset=["pickup_datetime", "dropoff_datetime", "pickup_zone_id", "dropoff_zone_id"])
                .filter(col("trip_distance") > 0)
                .filter(col("total_amount") > 0)
            )
            df_clean = df_clean.limit(200_000)  # adjust up/down as needed

            logging.info("Cleaned rows count (Spark): %d", df_clean.count())

            # Write as single CSV (Spark creates a directory with part-*.csv)
            (
                df_clean
                .coalesce(1)
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(cleaned_dir)
            )

            logging.info("Wrote cleaned data to directory %s", cleaned_dir)
            return cleaned_dir

        except Exception as e:
            logging.exception("Error during PySpark ETL")
            raise e
        finally:
            try:
                spark.stop()
            except Exception:
                pass




    # 3) Load cleaned CSV to Snowflake

    @task
    def load_cleaned_to_snowflake(cleaned_dir: str, **context) -> str:
        """
        - PUT cleaned CSV to internal stage
        - COPY INTO staging table
        - MERGE into fact table
        Using only SQL with fully qualified object names.
        """
        schema = Variable.get("target_schema_raw", default_var="RAW")

        pattern = os.path.join(cleaned_dir, "part-*.csv")
        files = glob.glob(pattern)

        if not files:
            logging.warning("No cleaned CSV files found for pattern %s", pattern)
            return "No cleaned data"

        cleaned_file = os.path.abspath(files[0])
        logging.info("Loading cleaned data from %s", cleaned_file)

        stage_name = f'"{schema}"."NYC_TAXI_STAGE"'
        fmt_name   = f'"{schema}"."NYC_TAXI_CSV_FMT"'
        stg_table  = f'"{schema}"."NYC_TAXI_TRIPS_STG"'
        fact_table = f'"{schema}"."NYC_TAXI_TRIPS"'

        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)
                cur.execute(f'CREATE STAGE IF NOT EXISTS {stage_name}')
                
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {stg_table} (
                PICKUP_DATETIME   TIMESTAMP_NTZ,
                DROPOFF_DATETIME  TIMESTAMP_NTZ,
                PICKUP_ZONE_ID    NUMBER,
                DROPOFF_ZONE_ID   NUMBER,
                PASSENGER_COUNT   NUMBER,
                TRIP_DISTANCE     FLOAT,
                TOTAL_AMOUNT      FLOAT
                )
            """)
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {fact_table} (
                PICKUP_DATETIME   TIMESTAMP_NTZ,
                DROPOFF_DATETIME  TIMESTAMP_NTZ,
                PICKUP_ZONE_ID    NUMBER,
                DROPOFF_ZONE_ID   NUMBER,
                PASSENGER_COUNT   NUMBER,
                TRIP_DISTANCE     FLOAT,
                TOTAL_AMOUNT      FLOAT,
                LOAD_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)

                # 1) PUT local file -> stage
                put_sql = f"""
                    PUT file://{cleaned_file}
                    @{stage_name}
                    AUTO_COMPRESS = TRUE
                    OVERWRITE = TRUE
                """
                logging.info("Executing PUT:\n%s", put_sql.strip())
                cur.execute(put_sql)

                # 2) COPY INTO staging table
                copy_sql = f"""
                    COPY INTO {stg_table}
                    FROM @{stage_name}
                    FILE_FORMAT = {fmt_name}
                    ON_ERROR = 'ABORT_STATEMENT'
                """
                logging.info("Executing COPY INTO staging")
                cur.execute(copy_sql)
                copy_result = cur.fetchone()
                logging.info("COPY result: %s", copy_result)

                # 3) MERGE from staging to fact (transactional)
                merge_sql = f"""
                    MERGE INTO {fact_table} AS tgt
                    USING {stg_table} AS src
                    ON  tgt.PICKUP_DATETIME  = src.PICKUP_DATETIME
                    AND tgt.DROPOFF_DATETIME = src.DROPOFF_DATETIME
                    AND tgt.PICKUP_ZONE_ID   = src.PICKUP_ZONE_ID
                    AND tgt.DROPOFF_ZONE_ID  = src.DROPOFF_ZONE_ID
                    AND tgt.TRIP_DISTANCE    = src.TRIP_DISTANCE
                    AND tgt.TOTAL_AMOUNT     = src.TOTAL_AMOUNT
                    WHEN NOT MATCHED THEN
                    INSERT (
                        PICKUP_DATETIME,
                        DROPOFF_DATETIME,
                        PICKUP_ZONE_ID,
                        DROPOFF_ZONE_ID,
                        PASSENGER_COUNT,
                        TRIP_DISTANCE,
                        TOTAL_AMOUNT
                    )
                    VALUES (
                        src.PICKUP_DATETIME,
                        src.DROPOFF_DATETIME,
                        src.PICKUP_ZONE_ID,
                        src.DROPOFF_ZONE_ID,
                        src.PASSENGER_COUNT,
                        src.TRIP_DISTANCE,
                        src.TOTAL_AMOUNT
                    )
                """
                logging.info("Executing MERGE into fact table")
                cur.execute(merge_sql)

                # 4) TRUNCATE staging for next run
                logging.info("Truncating staging table %s", stg_table)
                cur.execute(f'TRUNCATE TABLE {stg_table}')

                conn.commit()

                # Optional: row count in fact table
                cur.execute(f'SELECT COUNT(*) FROM {fact_table}')
                total = cur.fetchone()[0]
                msg = f"COPY + MERGE completed; total rows in {schema}.NYC_TAXI_TRIPS = {total}"
                logging.info(msg)
                return msg

            except Exception as e:
                logging.exception("Error during COPY/MERGE into Snowflake")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e



    # Wiring
    t_ensure = ensure_objects()
    t_clean = extract_transform_with_pyspark()
    t_load = load_cleaned_to_snowflake(t_clean)

    t_ensure >> t_clean >> t_load

