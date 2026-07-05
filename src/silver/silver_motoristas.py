from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import DateType

from src.config import SOURCES


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read bronze data for motoristas

    Parameters
    ----------
    spark : SparkSession
        SparkSession object to read the data.
    config : dict, optional
        Configuration dictionary containing the source paths. If not provided, the default SOURCES will be used.

    Returns
    -------
    DataFrame
        A DataFrame containing the bronze data for motoristas.
    """
    source = (config or SOURCES).get("motoristas", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def clean(df: DataFrame) -> DataFrame:
    """Clean the DataFrame by trimming whitespace and handling empty strings.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to be cleaned.

    Returns
    -------
    DataFrame
        A cleaned DataFrame with trimmed whitespace and empty strings replaced with None.
    """
    df = df.select([trim(col(c)).alias(c) for c in df.columns])

    df = df.withColumn("nome", when(col("nome") == "", lit(None)).otherwise(col("nome")))

    df = df.withColumn("validade_cnh", col("validade_cnh").cast(DateType()))
    df = df.withColumn("data_admissao", col("data_admissao").cast(DateType()))

    return df


def write_silver(df: DataFrame, output_path: str) -> str:
    """Write the cleaned DataFrame to the silver path in Parquet format.

    Parameters
    ----------
    df : DataFrame
        The cleaned DataFrame to be written.

    output_path : str
        The path where the silver data will be written.
    
    Returns
    -------
    str
        The output path where the silver data was written.
    """
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_silver(spark: SparkSession, config: dict | None = None) -> str:
    """Transform bronze data to silver data for motoristas.

    Parameters
    ----------
    spark : SparkSession
        SparkSession object to read and write the data.
    config : dict, optional
        Configuration dictionary containing the source paths. If not provided, the default SOURCES will be used.

    Returns
    -------
    str
        The output path where the silver data was written.
    """
    source = (config or SOURCES).get("motoristas", {})
    silver_path: str = source.get("silver_path", "")
    df_bronze = read_bronze(spark, config)
    df_clean = clean(df_bronze)
    return write_silver(df_clean, silver_path)
