"""
DAG definition for Global Mart Consolidation Pipeline.
"""
from datetime import datetime, timedelta
import os
from typing import Dict, Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from dotenv import load_dotenv

load_dotenv()

def get_default_args() -> Dict[str, Any]:
    """
    Returns the default arguments for the Airflow DAG.

    Returns:
        Dict[str, Any]: Default configuration arguments.
    """
    return {
        'owner': 'data_engineer',
        'depends_on_past': False,
        'start_date': datetime(2026, 6, 20),
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }

def create_dag() -> DAG:
    """
    Creates and configures the DAG for the consolidation pipeline.

    Returns:
        DAG: The configured Airflow DAG object.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    beam_pipeline_path = os.path.join(project_root, 'beam', 'beam_pipeline.py')
    silver_layer_path = os.path.join(project_root, 'silver_layer/')
    dbt_project_path = os.path.join(project_root, 'dbt_project')
    load_script_path = os.path.join(project_root, 'beam', 'load_to_postgres.py')

    with DAG(
        'global_mart_consolidation_pipeline',
        default_args=get_default_args(),
        schedule_interval='@daily',
        catchup=False,
        doc_md=__doc__
    ) as dag:

        env_vars = os.environ.copy()

        extract_and_transform_silver = BashOperator(
            task_id='extract_and_transform_silver',
            bash_command=f'python {beam_pipeline_path}',
            env=env_vars
        )

        sensor_silver_data = FileSensor(
            task_id='sensor_silver_data',
            filepath=silver_layer_path,
            fs_conn_id='fs_default',
            poke_interval=30,
            timeout=600
        )

        load_parquet_to_postgres = BashOperator(
            task_id='load_parquet_to_postgres',
            bash_command=f'python {load_script_path}',
            env=env_vars
        )

        load_and_model_gold = BashOperator(
            task_id='load_and_model_gold',
            bash_command=f'cd {dbt_project_path} && dbt deps && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .',
            env=env_vars
        )

        extract_and_transform_silver >> sensor_silver_data >> load_parquet_to_postgres >> load_and_model_gold

    return dag

dag = create_dag()