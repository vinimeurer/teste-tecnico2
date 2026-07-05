from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime


def _run_silver_transformation():
    from src.pipeline.silver_layer import main

    main()


@dag(
    dag_id="transform_silver",
    start_date=datetime(2026, 7, 2),
    schedule="@once",
    catchup=False,
    default_args={"owner": "engenharia", "retries": 0},
)
def transform_silver_dag():
    PythonOperator(
        task_id="transform_silver",
        python_callable=_run_silver_transformation,
    )


transform_silver_dag()
