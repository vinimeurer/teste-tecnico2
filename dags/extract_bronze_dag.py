"""Airflow DAG for bronze layer extraction.

Orchestrates the extraction of all five raw data sources into the
bronze layer (Parquet format) using PySpark embedded in the Airflow
worker.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipeline.bronze_layer import main as run_bronze

logger = logging.getLogger(__name__)

DAG_ID = "extract_bronze"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def _run_bronze_extraction(**context) -> None:
    """Execute the bronze layer extraction and log results."""
    logger.info("Iniciando extração da camada bronze...")
    results = run_bronze()
    for source, path in results.items():
        logger.info("Fonte '%s' consolidada em: %s", source, path)
    logger.info("Extração da camada bronze concluída com sucesso.")
    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key="bronze_results", value=results)


with DAG(
    dag_id=DAG_ID,
    description="Extrai dados brutos das 5 fontes e consolida na camada bronze (Parquet)",
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze"],
) as dag:

    extract_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=_run_bronze_extraction,
    )

    extract_bronze
