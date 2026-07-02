from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import DateType

from src.config import SOURCES


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    source = (config or SOURCES).get("motoristas", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def clean(df: DataFrame) -> DataFrame:
    df = df.select([trim(col(c)).alias(c) for c in df.columns])

    df = df.withColumn("nome", when(col("nome") == "", lit(None)).otherwise(col("nome")))

    df = df.withColumn("validade_cnh", col("validade_cnh").cast(DateType()))
    df = df.withColumn("data_admissao", col("data_admissao").cast(DateType()))

    return df


def write_silver(df: DataFrame, output_path: str) -> str:
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_silver(spark: SparkSession, config: dict | None = None) -> str:
    source = (config or SOURCES).get("motoristas", {})
    silver_path: str = source.get("silver_path", "")
    df_bronze = read_bronze(spark, config)
    df_clean = clean(df_bronze)
    return write_silver(df_clean, silver_path)
