from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.config import SOURCES

GEO_COLUMNS = ["geocerca_id", "nome", "tipo", "uf", "raio_km", "ativo"]


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read the bronze layer for geocercas from the specified path.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Optional configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing the bronze layer data for geocercas.
    """
    source = (config or SOURCES).get("geocercas", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def clean(df: DataFrame) -> DataFrame:
    """Clean the geocercas DataFrame by extracting relevant properties.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing 'properties' and 'geometry' columns.

    Returns
    -------
    DataFrame
        Cleaned DataFrame with selected columns and geometry.
    """
    for c in GEO_COLUMNS:
        df = df.withColumn(c, col("properties").getItem(c))
    df = df.select(*GEO_COLUMNS, col("geometry"))
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
    """Transform the bronze layer data for geocercas to the silver layer.

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
    source = (config or SOURCES).get("geocercas", {})
    silver_path: str = source.get("silver_path", "")
    df_bronze = read_bronze(spark, config)
    df_clean = clean(df_bronze)
    return write_silver(df_clean, silver_path)
