"""Bronze layer consolidation orchestrator.

Creates a SparkSession and runs all five extraction modules
to persist raw data as Parquet in the bronze layer.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession

from src.config import APP_NAME_BRONZE, QUALITY_OUTPUT_PATH, SOURCES
from src.bronze.bronze_veiculos import extract_to_bronze as extract_veiculos
from src.bronze.bronze_motoristas import extract_to_bronze as extract_motoristas
from src.bronze.bronze_geocercas import extract_to_bronze as extract_geocercas
from src.bronze.bronze_viagens import extract_to_bronze as extract_viagens
from src.bronze.bronze_posicoes import extract_to_bronze as extract_posicoes
from src.validation.quality_checks import run_all

logger = logging.getLogger(__name__)

EXTRACTORS: dict[str, Any] = {
    "veiculos": extract_veiculos,
    "motoristas": extract_motoristas,
    "geocercas": extract_geocercas,
    "viagens": extract_viagens,
    "posicoes": extract_posicoes,
}


def create_spark_session(app_name: str = APP_NAME_BRONZE) -> SparkSession:
    """Create and configure a SparkSession for bronze extraction.

    Parameters
    ----------
    app_name : str
        Application name for the Spark session.

    Returns
    -------
    SparkSession
        Configured SparkSession.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def run_bronze_extraction(
    spark: SparkSession,
    config: dict | None = None,
) -> dict[str, str]:
    """Run all extraction modules to populate the bronze layer.

    Iterates over each source, calls the corresponding extractor,
    and logs progress and results.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Application configuration. Falls back to src.config.SOURCES.

    Returns
    -------
    dict[str, str]
        Mapping of source name to bronze output path.
    """
    cfg = config or SOURCES
    results: dict[str, str] = {}

    for name, extractor in EXTRACTORS.items():
        logger.info("Iniciando extração da fonte: %s", name)
        try:
            output_path = extractor(spark, cfg)
            source_cfg = cfg.get(name, {})
            raw_path = source_cfg.get("raw_path", "?")
            logger.info(
                "Extração concluída: %s (%s) -> %s", name, raw_path, output_path
            )
            results[name] = output_path
            df = spark.read.parquet(output_path)
            run_all(df, name, "bronze", QUALITY_OUTPUT_PATH)
        except Exception:
            logger.exception("Falha na extração da fonte: %s", name)
            raise

    return results


def main() -> dict[str, str]:
    """Entry point for bronze layer consolidation.

    Creates a SparkSession, runs all extractions, stops the session,
    and returns the results.

    Returns
    -------
    dict[str, str]
        Mapping of source name to bronze output path.
    """
    spark = create_spark_session()
    try:
        results = run_bronze_extraction(spark, SOURCES)
        return results
    finally:
        spark.stop()
