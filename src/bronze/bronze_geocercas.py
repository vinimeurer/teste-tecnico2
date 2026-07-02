"""Extraction module for geocercas (geofences) data.

Reads raw GeoJSON data from the geocercas source, extracts the
FeatureCollection structure into a tabular format (properties +
geometry as nested structs), and persists it in the bronze layer
as Parquet.
"""

from pyspark.sql import DataFrame, SparkSession

from src.config import SOURCES


def read_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Read raw GeoJSON data as a JSON document.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    path : str
        Path to the raw GeoJSON file.

    Returns
    -------
    DataFrame
        Single-row DataFrame containing the full FeatureCollection.
    """
    return spark.read.option("multiLine", True).json(path)


def explode_features(df: DataFrame) -> DataFrame:
    """Explode the features array into one row per geofence.

    Parameters
    ----------
    df : DataFrame
        DataFrame with a 'features' array column.

    Returns
    -------
    DataFrame
        One row per feature, with 'properties' and 'geometry' as struct columns.
    """
    from pyspark.sql.functions import col, explode

    return df.select(explode(col("features")).alias("feature")).select(
        col("feature.properties").alias("properties"),
        col("feature.geometry").alias("geometry"),
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
    """Extract geocercas raw data and persist in bronze layer as Parquet.

    Reads GeoJSON from the raw data path, explodes the features array
    into individual rows keeping properties and geometry as nested
    structs, and writes to the bronze output directory in overwrite mode.

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
    source = (config or SOURCES).get("geocercas", {})
    raw_path: str = source.get("raw_path", "")
    bronze_path: str = source.get("bronze_path", "")

    df_raw = read_raw_data(spark, raw_path)
    df_features = explode_features(df_raw)
    return write_bronze(df_features, bronze_path)
