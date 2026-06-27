# Global Mart Consolidation Pipeline

## Overview
This project orchestrates a data consolidation pipeline for Global Mart using Apache Airflow, Apache Beam, dbt, and PostgreSQL. It reads raw CSV data (Bronze), structures it via Apache Beam into Parquet (Silver), and aggregates it via dbt into a PostgreSQL data warehouse (Gold). All components are fully Dockerized for environment isolation.

## Architecture & Tech Stack
- **Apache Airflow**: Orchestrates the pipeline (DAG).
- **Apache Beam**: Processes and denormalizes raw data using Python (multi-threaded ingestion), saving to a Parquet Silver Layer.
- **Python Loader**: A custom script automatically bridges the gap by extracting the Parquet data and inserting it into PostgreSQL, ensuring nested structs are properly mapped.
- **dbt**: Seeds reference datasets (exchange rates), runs views/tables (Gold Layer), and tests data quality.
- **PostgreSQL**: Serves as the Gold Layer Analytics database.
- **Docker Compose**: Containerizes all services natively with custom builds.

## Instructions

### 1. Configuration (.env)
Ensure you have created a `.env` file at the root of the project to match the Docker network. A template of the necessary variables is:
```env
PROJECT_ROOT=/opt/airflow
BEAM_PIPELINE_PATH=${PROJECT_ROOT}/beam/beam_pipeline.py
SILVER_LAYER_PATH=${PROJECT_ROOT}/silver_layer
DBT_PROJECT_PATH=${PROJECT_ROOT}/dbt_project
DB_HOST=postgres_gold
DB_PORT=5433
DB_USER=admin
DB_PASSWORD=adminpassword
DB_NAME=supermarket_db
```

### 2. Setting up the Services
Start all the containerized services (Airflow and PostgreSQL) via Docker Compose. The `--build` flag is critical on first run because it builds a custom Airflow image containing `dbt` and `apache-beam`:
```bash
docker compose up --build -d
```

### 3. Initialize Airflow
Because Airflow runs entirely in Docker, you do NOT need to install it locally. You must run the initial migrations and create an admin user:
```bash
docker compose run --rm airflow-webserver airflow db migrate
docker compose run --rm airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

### 4. Triggering the Pipeline
Once initialized, you can trigger the pipeline directly through the container:
```bash
docker compose exec airflow-webserver airflow dags trigger global_mart_consolidation_pipeline
```
Or you can navigate to `http://localhost:8080` to trigger it via the UI.

### 5. Running Modularized Tests (Optional)
If you wish to run the dbt tests manually within the container (after Apache Beam has generated the silver data):
```bash
docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt_project && dbt deps && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."
```
