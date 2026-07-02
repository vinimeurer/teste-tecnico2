"""Extraction module for viagens (trips) data.

Reads raw CSV data from the viagens source and persists it
in the bronze layer as Parquet.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import SOURCES

VIAGENS_SCHEMA = StructType(
    [
        StructField("viagem_id", StringType(), True),
        StructField("veiculo_id", StringType(), True),
        StructField("motorista_id", StringType(), True),
        StructField("geocerca_origem_id", StringType(), True),
        StructField("geocerca_destino_id", StringType(), True),
        StructField("data_inicio", TimestampType(), True),
        StructField("data_fim_prevista", TimestampType(), True),
        StructField("data_fim_real", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("distancia_km", IntegerType(), True),
        StructField("peso_carga_kg", IntegerType(), True),
        StructField("nota_fiscal", StringType(), True),
    ]
)


def read_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Read raw viagens CSV data.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    path : str
        Path to the raw CSV file.

    Returns
    -------
    DataFrame
        Raw data with inferred schema.
    """
    return spark.read.csv(path, header=True, inferSchema=True)


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
    for field in VIAGENS_SCHEMA.fields:
        if field.name not in df.columns:
            raise ValueError(
                f"Coluna obrigatória '{field.name}' ausente em viagens."
            )
    return df.select(
        [
            df[field.name].cast(field.dataType).alias(field.name)
            for field in VIAGENS_SCHEMA.fields
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
    """Extract viagens raw data and persist in bronze layer as Parquet.

    Reads CSV from the raw data path, enforces the expected schema,
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
    source = (config or SOURCES).get("viagens", {})
    raw_path: str = source.get("raw_path", "")
    bronze_path: str = source.get("bronze_path", "")

    df_raw = read_raw_data(spark, raw_path)
    df_casted = validate_and_cast_schema(df_raw)
    return write_bronze(df_casted, bronze_path)
