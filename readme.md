# NYC Taxi ETL with Airflow, PySpark & Snowflake

This repository contains an end-to-end ETL pipeline that loads **recent NYC Yellow Taxi trip data** into a **Snowflake** data warehouse using **Apache Airflow** and **PySpark**.

The goals of this ETL layer are to:

- Orchestrate data ingestion with **Airflow**
- Transform raw trip records with **PySpark**
- Load clean data into **Snowflake** using a proper **DW pattern**:
  - internal stage
  - staging table
  - `COPY INTO` + `MERGE` into a fact table

---

## High-Level Architecture

**Data source**

- Public NYC TLC Yellow Taxi trip data (Parquet):  
  `yellow_tripdata_YYYY-MM.parquet`  
  served via a public CloudFront endpoint.

**Pipeline steps**

1. **Discover latest available month** in the current year by probing TLC Parquet URLs.
2. **Download** the Parquet file for that month into the Airflow environment.
3. **Transform with PySpark**:
   - Read Parquet into a Spark DataFrame
   - Select and cast relevant columns
   - Filter out invalid/zero-distance/zero-amount trips
   - Optionally limit rows to fit local resources
   - Write a single cleaned CSV file to a local `/tmp` directory
4. **Load into Snowflake**:
   - Upload CSV to a Snowflake **internal stage** via `PUT`
   - `COPY INTO` a **staging table**
   - `MERGE` from staging into a **fact table**
   - `TRUNCATE` the staging table after a successful merge

All of this is orchestrated by an Airflow DAG called **`nyc_taxi_pyspark_etl`**.

---

## Prerequisites

### 1. Environment

- Docker / Docker Compose (or another way to run Airflow)
- A running **Airflow** instance with:
  - Python 3.x
  - `apache-airflow-providers-snowflake` installed
- **PySpark** installed in the same Python environment as the Airflow tasks, for example:

  ```bash
  pip install pyspark
