# /opt/airflow/dags/weather_realtime_etl.py
from __future__ import annotations

import logging
import json
from datetime import datetime, timezone

import requests
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_catfish"

default_args = {
    "depends_on_past": False,
    "retries": 1,
}

# One shared hook, same pattern as your taxi ETL DAG
hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

with DAG(
    dag_id="nyc_weather_realtime_etl",
    description="Fetch near-real-time NYC weather and load into Snowflake",
    schedule="0 * * * *",  # hourly; adjust if you want more/less frequent
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "weather", "realtime", "snowflake"],
) as dag:

    @task
    def ensure_weather_table(**context) -> str:
        """
        Ensure Snowflake schema + RAW_WEATHER table exist.
        Uses the same schema variable pattern as the taxi ETL DAG.
        """
        schema = Variable.get("target_schema_raw", default_var="RAW")

        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)

                # Create schema if not exists
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

                # Weather fact table (realtime feed)
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{schema}"."RAW_WEATHER" (
                      OBSERVED_AT     TIMESTAMP_NTZ,
                      CITY            STRING,
                      TEMP_F          FLOAT,
                      WEATHER_DESC    STRING,
                      HUMIDITY_PCT    NUMBER,
                      RAW_JSON        VARIANT,
                      LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                    """
                )

                conn.commit()
                msg = f"Ensured {schema}.RAW_WEATHER"
                logging.info(msg)
                return msg

            except Exception as e:
                logging.exception("Error ensuring RAW_WEATHER table in Snowflake")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e

    @task
    def fetch_and_load_weather(**context) -> str:
        """
        Fetch current weather for New York (or city in Variable),
        then insert one row into <schema>.RAW_WEATHER in Snowflake.
        """
        schema = Variable.get("target_schema_raw", default_var="RAW")

        # 1) Get API key & city from Airflow Variables
        try:
            api_key = Variable.get("openweather_api_key")
        except KeyError:
            raise RuntimeError(
                "Airflow Variable 'openweather_api_key' is not set. "
                "Create it in the UI with your OpenWeather API key."
            )

        city = Variable.get("weather_city", default_var="New York")

        # 2) Call OpenWeather API
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": api_key,
            "units": "imperial",  # Fahrenheit
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            logging.info("Fetched weather data for city=%s", city)
        except Exception as e:
            logging.exception("Failed to fetch weather from OpenWeather")
            raise e

        # 3) Parse JSON into a row
        try:
            observed_at = pd.Timestamp.utcnow()
            city_name = data.get("name", city)
            temp_f = float(data["main"]["temp"])
            desc = str(data["weather"][0]["description"])
            humidity_pct = int(data["main"]["humidity"])

            # We’ll keep the full JSON as a VARIANT column as well
            raw_json_str = json.dumps(data)

        except Exception as e:
            logging.exception("Failed to parse OpenWeather JSON payload")
            raise e

        # 4) Insert into Snowflake with transaction and error handling
        insert_sql = f"""
            INSERT INTO "{schema}"."RAW_WEATHER"
              (OBSERVED_AT, CITY, TEMP_F, WEATHER_DESC, HUMIDITY_PCT, RAW_JSON)
            SELECT
              %s AS OBSERVED_AT,
              %s AS CITY,
              %s AS TEMP_F,
              %s AS WEATHER_DESC,
              %s AS HUMIDITY_PCT,
              PARSE_JSON(%s) AS RAW_JSON
        """

        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)

                cur.execute(
                    insert_sql,
                    (
                        observed_at.to_pydatetime(),  # TIMESTAMP_NTZ
                        city_name,
                        temp_f,
                        desc,
                        humidity_pct,
                        raw_json_str,
                    ),
                )

                conn.commit()
                msg = (
                    f"Inserted weather row for city={city_name}, "
                    f"temp_f={temp_f}, humidity={humidity_pct} into {schema}.RAW_WEATHER"
                )
                logging.info(msg)
                return msg

            except Exception as e:
                logging.exception("Error inserting weather row into Snowflake")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e

    t_ensure = ensure_weather_table()
    t_fetch_load = fetch_and_load_weather()

    t_ensure >> t_fetch_load
