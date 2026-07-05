from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import DateType, StringType

from src.config import SOURCES


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read the bronze layer for veiculos from the specified path.

    Parameters
    ----------

    spark : SparkSession
        Active PySpark session.

    config : dict or None
        Optional configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing the bronze layer data for veiculos.
    """
    source = (config or SOURCES).get("veiculos", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def clean(df: DataFrame) -> DataFrame:
    """Clean the veiculos DataFrame by standardizing and validating fields.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the veiculos data.

    Returns
    -------
    DataFrame
        Cleaned DataFrame with standardized and validated fields.
    """
    status_validos = {"ativo", "inativo", "em_manutencao"}

    df = df.select([trim(col(c)).alias(c) for c in df.columns])

    df = df.withColumn(
        "placa", when(col("placa") == "INVALIDA", lit(None)).otherwise(col("placa"))
    )

    df = df.withColumn(
        "data_ultima_revisao", col("data_ultima_revisao").cast(DateType())
    )

    df = df.withColumn(
        "status",
        when(col("status").isin(status_validos), col("status")).otherwise(lit(None)),
    )

    return df


def write_silver(df: DataFrame, output_path: str) -> str:
    """Write the cleaned DataFrame to the silver layer as Parquet.

    Parameters
    ----------
    df : DataFrame
        Cleaned DataFrame to be written.

    output_path : str
        Destination path for the silver layer Parquet files.

    Returns
    -------
    str
        Path where the data was written.
    """
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_silver(spark: SparkSession, config: dict | None = None) -> str:
    """Transform the bronze layer data for veiculos to the silver layer.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.

    config : dict or None
        Optional configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    str
        Path to the written silver Parquet directory.
    """
    source = (config or SOURCES).get("veiculos", {})
    silver_path: str = source.get("silver_path", "")
    df_bronze = read_bronze(spark, config)
    df_clean = clean(df_bronze)
    return write_silver(df_clean, silver_path)
