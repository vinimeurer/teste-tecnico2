from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import TimestampType

from src.config import SOURCES


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    source = (config or SOURCES).get("viagens", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def _carregar_ids_validos(spark: SparkSession, config: dict) -> tuple[set, set]:
    veic_path: str = config.get("veiculos", {}).get("bronze_path", "")
    mot_path: str = config.get("motoristas", {}).get("bronze_path", "")
    veic_validos = {r["veiculo_id"] for r in spark.read.parquet(veic_path).select("veiculo_id").distinct().collect()}
    mot_validos = {r["motorista_id"] for r in spark.read.parquet(mot_path).select("motorista_id").distinct().collect()}
    return veic_validos, mot_validos


def clean(df: DataFrame, veic_validos: set, mot_validos: set) -> DataFrame:
    status_validos = {"em_transito", "concluida", "cancelada", "atrasada"}

    df = df.select([trim(col(c)).alias(c) for c in df.columns])

    df = df.withColumn("data_inicio", col("data_inicio").cast(TimestampType()))
    df = df.withColumn("data_fim_prevista", col("data_fim_prevista").cast(TimestampType()))
    df = df.withColumn("data_fim_real", col("data_fim_real").cast(TimestampType()))

    df = df.withColumn(
        "referencia_valida",
        (col("veiculo_id").isin(veic_validos) & col("motorista_id").isin(mot_validos)),
    )

    df = df.withColumn("nota_fiscal", when(col("nota_fiscal").isNull(), lit("NAO INFORMADO")).otherwise(col("nota_fiscal")))

    df = df.withColumn(
        "status",
        when(col("status").isin(status_validos), col("status")).otherwise(lit(None)),
    )

    return df


def write_silver(df: DataFrame, output_path: str) -> str:
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_silver(spark: SparkSession, config: dict | None = None) -> str:
    cfg = config or SOURCES
    source = cfg.get("viagens", {})
    silver_path: str = source.get("silver_path", "")
    veic_validos, mot_validos = _carregar_ids_validos(spark, cfg)
    df_bronze = read_bronze(spark, cfg)
    df_clean = clean(df_bronze, veic_validos, mot_validos)
    return write_silver(df_clean, silver_path)
