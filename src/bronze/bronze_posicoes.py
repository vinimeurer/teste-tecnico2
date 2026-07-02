"""Extraction module for posicoes (GPS positions) data.

Reads raw Parquet data from the rastreamento source and persists it
in the bronze layer as Parquet with an enforced schema.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import SOURCES

POSICOES_SCHEMA = StructType(
    [
        StructField("posicao_id", StringType(), True),
        StructField("viagem_id", StringType(), True),
        StructField("veiculo_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("velocidade_kmh", IntegerType(), True),
        StructField("ignicao", BooleanType(), True),
        StructField("odometro_metros", DoubleType(), True),
    ]
)


def read_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Read raw posicoes Parquet data.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    path : str
        Path to the raw Parquet file.

    Returns
    -------
    DataFrame
        Raw data with inferred schema.
    """
    return spark.read.parquet(path)


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
    for field in POSICOES_SCHEMA.fields:
        if field.name not in df.columns:
            raise ValueError(
                f"Coluna obrigatória '{field.name}' ausente em posicoes."
            )
    return df.select(
        [
            df[field.name].cast(field.dataType).alias(field.name)
            for field in POSICOES_SCHEMA.fields
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
    """Extract posicoes raw data and persist in bronze layer as Parquet.

    Reads Parquet from the raw data path, enforces the expected schema,
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
    source = (config or SOURCES).get("posicoes", {})
    raw_path: str = source.get("raw_path", "")
    bronze_path: str = source.get("bronze_path", "")

    df_raw = read_raw_data(spark, raw_path)
    df_casted = validate_and_cast_schema(df_raw)
    return write_bronze(df_casted, bronze_path)
