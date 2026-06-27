from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 20),
    'retries': 2, # Resiliencia: Reintentos [cite: 97]
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'global_mart_consolidation_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Ejecutar Apache Beam [cite: 92]
    extract_and_transform_silver = BashOperator(
        task_id='extract_and_transform_silver',
        bash_command='python /path/to/tu/repo/beam_pipeline.py' 
    )

    # Task 2: Sensor para verificar el archivo Parquet (Opcional pero solicitado) [cite: 94]
    sensor_silver_data = FileSensor(
        task_id='sensor_silver_data',
        filepath='/path/to/tu/repo/silver_layer/',
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600
    )

    # Task 3: Ejecutar dbt run y dbt test [cite: 95]
    load_and_model_gold = BashOperator(
        task_id='load_and_model_gold',
        bash_command='cd /path/to/tu/repo/dbt_project && dbt run && dbt test'
    )

    # Definición de la jerarquía (Beam >> dbt) [cite: 97]
    extract_and_transform_silver >> sensor_silver_data >> load_and_model_gold