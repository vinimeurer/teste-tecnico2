"""Gold layer consolidation orchestrator.

Creates a SparkSession and runs all gold transformation modules
to produce enriched and aggregated data in the gold layer.
"""

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

from src.config import APP_NAME_GOLD, SOURCES
from src.gold.gold_posicoes_enriquecidas import transform_to_gold as transform_posicoes
from src.gold.gold_eventos_geocercas import transform_to_gold as transform_eventos
from src.gold.gold_viagens_enriquecidas import transform_to_gold as transform_viagens
from src.gold.gold_metricas_agregadas import transform_to_gold as transform_metricas

logger = logging.getLogger(__name__)

TRANSFORMERS: dict[str, Any] = {
    "posicoes_enriquecidas": transform_posicoes,
    "eventos_geocercas": transform_eventos,
    "viagens_enriquecidas": transform_viagens,
    "metricas_agregadas": transform_metricas,
}


def create_spark_session(app_name: str = APP_NAME_GOLD) -> SparkSession:
    """Create and configure a SparkSession for gold transformations.

    Parameters
    ----------
    app_name : str
        Application name for the Spark session.

    Returns
    -------
    SparkSession
        Configured SparkSession.
    """
    src_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    existing = os.environ.get("PYTHONPATH", "")
    if src_parent not in existing.split(":"):
        os.environ["PYTHONPATH"] = (
            f"{src_parent}:{existing}" if existing else src_parent
        )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def run_gold_transformation(
    spark: SparkSession,
    config: dict | None = None,
) -> dict[str, Any]:
    """Run all gold transformation modules in order.

    Iterates over each transformation, calls the corresponding function,
    and logs progress and results. The order is:
    1. Enriched positions (needed by events)
    2. Geocerca events (needed by metrics)
    3. Enriched viagens (needed by metrics)
    4. Aggregated metrics

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Application configuration. Falls back to src.config.SOURCES.

    Returns
    -------
    dict[str, Any]
        Mapping of transformation name to output path(s).
    """
    cfg = config or SOURCES
    results: dict[str, Any] = {}

    for name, transformer in TRANSFORMERS.items():
        logger.info("Iniciando transformação gold: %s", name)
        try:
            output = transformer(spark, cfg)
            logger.info("Gold concluída: %s -> %s", name, output)
            results[name] = output
        except Exception:
            logger.exception("Falha na transformação gold: %s", name)
            raise

    return results


def main() -> dict[str, Any]:
    """Entry point for gold layer consolidation.

    Creates a SparkSession, runs all gold transformations, stops the
    session, and returns the results.

    Returns
    -------
    dict[str, Any]
        Mapping of transformation name to output path(s).
    """
    spark = create_spark_session()
    try:
        results = run_gold_transformation(spark, SOURCES)
        return results
    finally:
        spark.stop()
