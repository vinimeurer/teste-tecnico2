from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import TimestampType

from src.config import SOURCES


def read_bronze(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read bronze data for viagens

    Parameters
    ----------
    spark : SparkSession
        Spark session object 
    config : dict | None, optional
        Configuration dictionary, by default None

    Returns
    -------
    DataFrame
        DataFrame containing bronze data for viagens
    """
    source = (config or SOURCES).get("viagens", {})
    path: str = source.get("bronze_path", "")
    return spark.read.parquet(path)


def _carregar_ids_validos(spark: SparkSession, config: dict) -> tuple[set, set]:
    """Load valid vehicle and driver IDs from bronze data
    
    Parameters
    ----------
    spark : SparkSession
        Spark session object
    config : dict
        Configuration dictionary containing source paths

    Returns
    -------
    tuple[set, set]
        A tuple containing two sets: valid vehicle IDs and valid driver IDs
    """
    veic_path: str = config.get("veiculos", {}).get("bronze_path", "")
    mot_path: str = config.get("motoristas", {}).get("bronze_path", "")
    veic_validos = {r["veiculo_id"] for r in spark.read.parquet(veic_path).select("veiculo_id").distinct().collect()}
    mot_validos = {r["motorista_id"] for r in spark.read.parquet(mot_path).select("motorista_id").distinct().collect()}
    return veic_validos, mot_validos


def clean(df: DataFrame, veic_validos: set, mot_validos: set) -> DataFrame:
    """Clean the DataFrame by trimming whitespace, casting types, and validating references.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to be cleaned.
    veic_validos : set
        A set of valid vehicle IDs.
    mot_validos : set
        A set of valid driver IDs.

    Returns
    -------
    DataFrame
        A cleaned DataFrame with trimmed whitespace, casted types, and validated references.
    """
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
    """Transform bronze data to silver data for viagens.

    Parameters
    ----------
    spark : SparkSession
        Spark session object
    config : dict | None, optional
        Configuration dictionary, by default None

    Returns
    -------
    str
        The output path where the silver data was written.
    """
    cfg = config or SOURCES
    source = cfg.get("viagens", {})
    silver_path: str = source.get("silver_path", "")
    veic_validos, mot_validos = _carregar_ids_validos(spark, cfg)
    df_bronze = read_bronze(spark, cfg)
    df_clean = clean(df_bronze, veic_validos, mot_validos)
    return write_silver(df_clean, silver_path)
