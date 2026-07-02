"""Extraction module for motoristas (drivers) data.

Reads raw JSON data from the motoristas source and persists it
in the bronze layer as Parquet.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from src.config import SOURCES

MOTORISTAS_SCHEMA = StructType(
    [
        StructField("motorista_id", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("cnh", StringType(), True),
        StructField("categoria_cnh", StringType(), True),
        StructField("validade_cnh", DateType(), True),
        StructField("telefone", StringType(), True),
        StructField("data_admissao", DateType(), True),
        StructField("base_operacional", StringType(), True),
        StructField("status", StringType(), True),
    ]
)


def read_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Read raw motoristas JSON data.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    path : str
        Path to the raw JSON file.

    Returns
    -------
    DataFrame
        Raw data with inferred schema.
    """
    return spark.read.option("multiLine", True).json(path)


def validate_and_cast_schema(df: DataFrame) -> DataFrame:
    """Validate and cast columns to the expected bronze schema.

    Parameters
    ----------
    df : DataFrame
        Raw DataFrame with inferred types.

    Returns
    -------
    DataFrame
        DataFrame with enforced schema.
    """
    for field in MOTORISTAS_SCHEMA.fields:
        if field.name not in df.columns:
            raise ValueError(
                f"Coluna obrigatória '{field.name}' ausente em motoristas."
            )
    return df.select(
        [
            df[field.name].cast(field.dataType).alias(field.name)
            for field in MOTORISTAS_SCHEMA.fields
        ]
    )


def write_bronze(df: DataFrame, output_path: str) -> str:
    """Write DataFrame to bronze layer as Parquet with overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Data to persist.
    output_path : str
        Destination directory for the Parquet files.

    Returns
    -------
    str
        Path where the data was written.
    """
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def extract_to_bronze(spark: SparkSession, config: dict | None = None) -> str:
    """Extract motoristas raw data and persist in bronze layer as Parquet.

    Reads JSON from the raw data path, enforces the expected schema,
    and writes to the bronze output directory in overwrite mode.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Application configuration. Falls back to src.config.SOURCES.

    Returns
    -------
    str
        Path to the written bronze Parquet directory.
    """
    source = (config or SOURCES).get("motoristas", {})
    raw_path: str = source.get("raw_path", "")
    bronze_path: str = source.get("bronze_path", "")

    df_raw = read_raw_data(spark, raw_path)
    df_casted = validate_and_cast_schema(df_raw)
    return write_bronze(df_casted, bronze_path)
