from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lag, when, lit, row_number, collect_list, last
from pyspark.sql.window import Window

from src.config import SOURCES


def read_gold_posicoes(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read gold enriched posicoes from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing gold enriched posicoes.
    """
    source = (config or SOURCES).get("posicoes", {})
    path: str = source.get("gold_path", "")
    return spark.read.parquet(path)


def detect_events(df: DataFrame) -> DataFrame:
    """Detect entry and exit events of geocercas along each voyage.

    Uses a window function ordered by timestamp within each voyage/vehicle
    to detect transitions between 'em_rota' and 'em_geocerca' classifications.

    Parameters
    ----------
    df : DataFrame
        DataFrame with enriched posicoes (classificacao, geocerca_id, etc.).

    Returns
    -------
    DataFrame
        DataFrame with detected events containing columns:
        evento_id, viagem_id, veiculo_id, geocerca_id, nome_geocerca,
        tipo_evento (entrada/saida), timestamp, latitude, longitude.
    """
    window_spec = Window.partitionBy("viagem_id", "veiculo_id").orderBy("timestamp")

    df_with_lag = df.withColumn(
        "classificacao_anterior",
        lag("classificacao").over(window_spec),
    )

    df_eventos = df_with_lag.filter(
        (
            (col("classificacao") == "em_geocerca")
            & (col("classificacao_anterior") == "em_rota")
        )
        | (
            (col("classificacao") == "em_rota")
            & (col("classificacao_anterior") == "em_geocerca")
        )
    )

    df_eventos = df_eventos.withColumn(
        "tipo_evento",
        when(
            (col("classificacao") == "em_geocerca")
            & (col("classificacao_anterior") == "em_rota"),
            lit("entrada"),
        ).otherwise(lit("saida")),
    )

    window_evento_id = Window.orderBy("viagem_id", "timestamp")
    df_eventos = df_eventos.withColumn(
        "evento_id",
        row_number().over(window_evento_id),
    )

    window_fill = Window.partitionBy("viagem_id", "veiculo_id").orderBy("timestamp")
    df_filled = df_eventos.withColumn(
        "geocerca_id", last("geocerca_id", True).over(window_fill),
    ).withColumn(
        "nome_geocerca", last("nome_geocerca", True).over(window_fill),
    )

    df_exploded = df_filled.select(
        col("evento_id"),
        col("viagem_id"),
        col("veiculo_id"),
        col("geocerca_id"),
        col("nome_geocerca"),
        col("tipo_evento"),
        col("timestamp"),
        col("latitude"),
        col("longitude"),
    ).filter(col("geocerca_id").isNotNull())

    return df_exploded


def write_gold(df: DataFrame, output_path: str) -> str:
    """Write the events DataFrame to the gold layer as Parquet.

    Parameters
    ----------
    df : DataFrame
        Events DataFrame to be written.
    output_path : str
        Destination path for the gold layer Parquet files.

    Returns
    -------
    str
        Path where the data was written.
    """
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_gold(spark: SparkSession, config: dict | None = None) -> str:
    """Transform gold enriched posicoes to detected geocerca events.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    str
        Path to the written gold Parquet directory.
    """
    cfg = config or SOURCES
    gold_path = cfg.get("posicoes", {}).get("gold_path", "")
    eventos_path = gold_path.replace("posicoes", "eventos_geocercas")
    df = read_gold_posicoes(spark, cfg)
    df_eventos = detect_events(df)
    return write_gold(df_eventos, eventos_path)
