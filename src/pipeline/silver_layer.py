import logging
from typing import Any

from pyspark.sql import SparkSession

from src.config import APP_NAME_SILVER, SOURCES
from src.silver.silver_veiculos import transform_to_silver as transform_veiculos
from src.silver.silver_motoristas import transform_to_silver as transform_motoristas
from src.silver.silver_geocercas import transform_to_silver as transform_geocercas
from src.silver.silver_viagens import transform_to_silver as transform_viagens
from src.silver.silver_posicoes import transform_to_silver as transform_posicoes

logger = logging.getLogger(__name__)

TRANSFORMERS: dict[str, Any] = {
    "veiculos": transform_veiculos,
    "motoristas": transform_motoristas,
    "geocercas": transform_geocercas,
    "viagens": transform_viagens,
    "posicoes": transform_posicoes,
}


def create_spark_session(app_name: str = APP_NAME_SILVER) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def run_silver_transformation(
    spark: SparkSession,
    config: dict | None = None,
) -> dict[str, str]:
    cfg = config or SOURCES
    results: dict[str, str] = {}

    for name, transformer in TRANSFORMERS.items():
        logger.info("Transformando camada silver: %s", name)
        try:
            output_path = transformer(spark, cfg)
            logger.info("Silver concluída: %s -> %s", name, output_path)
            results[name] = output_path
        except Exception:
            logger.exception("Falha na transformação silver: %s", name)
            raise

    return results


def main() -> dict[str, str]:
    spark = create_spark_session()
    try:
        results = run_silver_transformation(spark, SOURCES)
        return results
    finally:
        spark.stop()
