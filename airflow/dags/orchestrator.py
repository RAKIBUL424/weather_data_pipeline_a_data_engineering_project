import sys
from airflow import DAG
from docker.types import Mount
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/api_requests")

from insert_records import fetch_data
    

default_args = {
    "owner": "airflow", 
    "description": "A DAG to orchestrate data.",
    "start_date": datetime(2025, 11, 29),
    "catchup": False,
}

dag = DAG(
    dag_id="weather_api_dbt_orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=6),  # Using schedule instead of schedule_interval
)

with dag:
    task1 = PythonOperator(
        task_id="ingest_data_task",
        python_callable=fetch_data
    )
    task2 = DockerOperator(
        task_id="transform_data_task",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source= '/home/rakibul/repos/weather-data-project/dbt/my_project',
                target= '/usr/app',
                type= 'bind'
            ),
            Mount(
                source= '/home/rakibul/repos/weather-data-project/dbt/',
                target= '/root/.dbt/',
                type= 'bind'
            )
        ],
        auto_remove='success',
        command="run",
        docker_url="unix://var/run/docker.sock",
        network_mode="weather-data-project_my_network"
    )

    task1 >> task2