# NYC Taxi Analytics – ETL with Airflow, PySpark & Snowflake

This project implements an end-to-end analytics pipeline for NYC Yellow Taxi data using:

- **Apache Airflow** – orchestration
- **PySpark** – batch ETL for historical trips
- **Snowflake** – data warehouse
- **OpenWeather API** – real-time weather feed (second data source)

The output of this layer is used by dbt models and BI dashboards for analysis and forecasting.

---

## High-Level Architecture

**Data sources**

- **Historical:** NYC TLC Yellow Taxi trip data (Parquet), one file per month.
- **Real-time:** Current weather data for New York City from the **OpenWeather** API.

**Processing**

1. **Batch ETL DAG** (`nyc_taxi_pyspark_etl`)
   - Discovers the most recent available NYC TLC Parquet file for the current year.
   - Downloads and transforms the data with PySpark.
   - Writes a cleaned CSV extract to `/tmp` inside the Airflow container.
   - Loads into Snowflake using `PUT` + `COPY INTO` + `MERGE` into a fact table.

2. **Real-time Weather DAG** (`nyc_weather_realtime_etl`)
   - Periodically calls the OpenWeather API for current weather in NYC.
   - Parses temperature / humidity / description.
   - Inserts one row per run into a Snowflake table for weather observations.

**Storage**

All data lands in a Snowflake **raw schema** (e.g. `RAW`) as:

- `RAW.NYC_TAXI_TRIPS` – historical taxi trips (fact table)
- `RAW.NYC_TAXI_TRIPS_STG` – staging table for COPY/MERGE
- `RAW.RAW_WEATHER` – real-time weather observations

---

## Historical NYC Taxi ETL (Batch)

**DAG:** `nyc_taxi_pyspark_etl`  
**File:** `dags/etl_spark_historical.py`

### What it does

1. **Find latest month in current year**
   - Builds URLs like  
     `yellow_tripdata_YYYY-MM.parquet`  
     under the public TLC CloudFront endpoint.
   - Sends `HEAD` requests from the Airflow task to find the most recent existing month.

2. **Download & transform with PySpark**
   - Downloads the Parquet file to `/tmp` inside the Airflow container.
   - Starts a local Spark session (`master=local[*]`).
   - Reads the Parquet and selects / casts relevant columns:

     - pickup / dropoff datetime
     - pickup / dropoff zone IDs
     - passenger count
     - trip distance
     - total amount

   - Drops invalid rows and filters out non-positive distance or amount.
   - Optionally limits row count for resource-constrained environments.
   - Writes a **single cleaned CSV** to `/tmp/nyc_taxi_cleaned_YYYY-MM/part-*.csv`.

3. **Load into Snowflake**
   - Ensures schema, stage, file format, staging and fact tables exist (`CREATE ... IF NOT EXISTS`).
   - Uses the Snowflake Python connector (via `SnowflakeHook`) to run:

     - `PUT file://... @<SCHEMA>.NYC_TAXI_STAGE`
     - `COPY INTO <SCHEMA>.NYC_TAXI_TRIPS_STG ...`
     - `MERGE` from staging into `<SCHEMA>.NYC_TAXI_TRIPS`
     - `TRUNCATE` the staging table

   - All wrapped in a transaction with `commit` / `rollback` and logging.

---

## Real-Time Weather Feed (Second Data Source)

**DAG:** `nyc_weather_realtime_etl`  
**File:** `dags/weather_realtime_etl.py`

This DAG provides the **real-time / near-real-time** data source required by the project, complementing the historical taxi archive.

### What it does

On each run (e.g. hourly):

1. **Ensure Weather Table Exists**

   Creates a table in the same raw schema as the taxi data (schema name controlled by Airflow Variable `target_schema_raw`, e.g. `RAW`):

   ```sql
   CREATE TABLE IF NOT EXISTS <SCHEMA>.RAW_WEATHER (
     OBSERVED_AT   TIMESTAMP_NTZ,
     CITY          STRING,
     TEMP_F        FLOAT,
     WEATHER_DESC  STRING,
     HUMIDITY_PCT  NUMBER,
     RAW_JSON      VARIANT,
     LOAD_TS       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
   );
