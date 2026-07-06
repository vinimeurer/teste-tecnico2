from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, year, month, when, lit, round, desc, coalesce,
    countDistinct, datediff, max, min, lag, lead, row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from src.config import SOURCES


def read_gold_viagens(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read gold enriched viagens from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing gold enriched viagens.
    """
    source = (config or SOURCES).get("viagens", {})
    path: str = source.get("gold_path", "")
    return spark.read.parquet(path)


def read_silver_veiculos(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read silver veiculos from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing silver veiculos data.
    """
    source = (config or SOURCES).get("veiculos", {})
    path: str = source.get("silver_path", "")
    return spark.read.parquet(path)


def read_gold_eventos(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read gold eventos geocercas from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing gold eventos geocercas.
    """
    source = (config or SOURCES).get("posicoes", {})
    path: str = source.get("gold_path", "")
    eventos_path = path.replace("posicoes", "eventos_geocercas")
    return spark.read.parquet(eventos_path)


def read_silver_geocercas(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read silver geocercas from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing silver geocercas data.
    """
    source = (config or SOURCES).get("geocercas", {})
    path: str = source.get("silver_path", "")
    return spark.read.parquet(path)


def viagens_por_mes_status(df_viagens: DataFrame) -> DataFrame:
    """Compute trip counts per month and status.

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with mes, ano, and status columns.

    Returns
    -------
    DataFrame
        DataFrame with ano, mes, status, and quantidade columns.
    """
    return (
        df_viagens.groupBy(col("ano"), col("mes"), col("status"))
        .agg(count("*").alias("quantidade"))
        .orderBy("ano", "mes", "status")
    )


def tempo_medio_viagem_rota(df_viagens: DataFrame) -> DataFrame:
    """Compute average trip duration per route (origin -> destination).

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with route info and tempo_viagem_horas.

    Returns
    -------
    DataFrame
        DataFrame with geocerca_origem_id, origem_nome, geocerca_destino_id,
        destino_nome, tempo_medio_horas, and quantidade_viagens.
    """
    return (
        df_viagens.groupBy(
            col("geocerca_origem_id"),
            col("origem_nome"),
            col("geocerca_destino_id"),
            col("destino_nome"),
        )
        .agg(
            round(avg("tempo_viagem_horas"), 2).alias("tempo_medio_horas"),
            count("*").alias("quantidade_viagens"),
        )
        .orderBy(col("quantidade_viagens").desc())
    )


def velocidade_media_viagem(df_viagens: DataFrame) -> DataFrame:
    """Compute average speed per trip.

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with velocidade_media_kmh.

    Returns
    -------
    DataFrame
        DataFrame with viagem_id, geocerca_origem_id, origem_nome,
        geocerca_destino_id, destino_nome, distancia_km, tempo_viagem_horas,
        and velocidade_media_kmh, ordered by highest speed first.
    """
    return (
        df_viagens.select(
            col("viagem_id"),
            col("geocerca_origem_id"),
            col("origem_nome"),
            col("geocerca_destino_id"),
            col("destino_nome"),
            col("distancia_km"),
            col("tempo_viagem_horas"),
            col("velocidade_media_kmh"),
        )
        .filter(col("velocidade_media_kmh").isNotNull())
        .orderBy(col("velocidade_media_kmh").desc())
    )


def taxa_atraso_mes(df_viagens: DataFrame) -> DataFrame:
    """Compute delay rate per month.

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with ano, mes, and possui_atraso columns.

    Returns
    -------
    DataFrame
        DataFrame with ano, mes, total_viagens, viagens_atrasadas,
        and taxa_atraso.
    """
    return (
        df_viagens.groupBy(col("ano"), col("mes"))
        .agg(
            count("*").alias("total_viagens"),
            sum(when(col("possui_atraso"), 1).otherwise(0)).alias("viagens_atrasadas"),
        )
        .withColumn(
            "taxa_atraso",
            round(
                col("viagens_atrasadas") / col("total_viagens"),
                4,
            ),
        )
        .orderBy("ano", "mes")
    )


def top_motoristas(df_viagens: DataFrame) -> DataFrame:
    """Get top 10 drivers by number of completed trips.

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with motorista_id, nome, and status.

    Returns
    -------
    DataFrame
        DataFrame with motorista_id, nome, and viagens_concluidas,
        limited to top 10.
    """
    return (
        df_viagens.filter(col("status") == "concluida")
        .groupBy(col("motorista_id"), col("nome"))
        .agg(count("*").alias("viagens_concluidas"))
        .orderBy(col("viagens_concluidas").desc())
        .limit(10)
    )


def utilizacao_frota_mes(
    df_viagens: DataFrame,
    df_veiculos: DataFrame,
) -> DataFrame:
    """Compute fleet utilization per month.

    Calculates the ratio of active vehicles that had at least one trip
    in each month.

    Parameters
    ----------
    df_viagens : DataFrame
        Enriched viagens DataFrame with veiculo_id, ano, mes.
    df_veiculos : DataFrame
        Silver veiculos DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with ano, mes, veiculos_com_viagem, total_veiculos_ativos,
        and taxa_utilizacao.
    """
    veiculos_ativos_count = (
        df_veiculos.filter(col("status") == "ativo")
        .agg(count("*").alias("total"))
        .collect()[0]["total"]
    )

    return (
        df_viagens.groupBy(col("ano"), col("mes"))
        .agg(countDistinct("veiculo_id").alias("veiculos_com_viagem"))
        .withColumn("total_veiculos_ativos", lit(veiculos_ativos_count))
        .withColumn(
            "taxa_utilizacao",
            round(
                col("veiculos_com_viagem") / lit(veiculos_ativos_count),
                4,
            ),
        )
        .orderBy("ano", "mes")
    )


def tempo_parado_geocerca_tipo(
    df_eventos: DataFrame,
    df_geocercas: DataFrame,
) -> DataFrame:
    """Compute average stopped time in geocercas by type.

    Pairs entry and exit events for each geocerca/voyage/vehicle
    and calculates the average duration per geocerca type.

    Parameters
    ----------
    df_eventos : DataFrame
        Gold eventos geocercas DataFrame with tipo_evento and timestamp.
    df_geocercas : DataFrame
        Silver geocercas DataFrame with geocerca_id, nome, and tipo.

    Returns
    -------
    DataFrame
        DataFrame with geocerca_id, nome, tipo, tempo_medio_parado_minutos,
        and total_eventos.
    """
    window_pair = Window.partitionBy("viagem_id", "veiculo_id", "geocerca_id").orderBy("timestamp")

    df_pares = df_eventos.withColumn(
        "proximo_timestamp",
        lead("timestamp").over(window_pair),
    )

    df_pares = df_pares.withColumn(
        "proximo_tipo",
        lead("tipo_evento").over(window_pair),
    )

    df_duracao = df_pares.filter(
        (col("tipo_evento") == "entrada")
        & (col("proximo_tipo") == "saida")
    ).withColumn(
        "tempo_parado_minutos",
        (col("proximo_timestamp").cast("long") - col("timestamp").cast("long")) / 60,
    )

    return (
        df_duracao.join(
            df_geocercas.select("geocerca_id", "nome", "tipo"),
            "geocerca_id",
            "left",
        )
        .groupBy(col("geocerca_id"), col("nome"), col("tipo"))
        .agg(
            round(avg("tempo_parado_minutos"), 2).alias("tempo_medio_parado_minutos"),
            count("*").alias("total_eventos"),
        )
        .orderBy(col("tempo_medio_parado_minutos").desc())
    )


def write_gold(df: DataFrame, output_path: str) -> str:
    """Write the metrics DataFrame to the gold layer as Parquet.

    Parameters
    ----------
    df : DataFrame
        Metrics DataFrame to be written.
    output_path : str
        Destination path for the gold layer Parquet files.

    Returns
    -------
    str
        Path where the data was written.
    """
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def transform_to_gold(spark: SparkSession, config: dict | None = None) -> dict[str, str]:
    """Compute all aggregated metrics from gold data.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    dict[str, str]
        Mapping of metric name to output Parquet path.
    """
    cfg = config or SOURCES
    base_path: str = cfg.get("viagens", {}).get("gold_path", "")
    metricas_base = base_path.replace("viagens", "metricas")

    df_viagens = read_gold_viagens(spark, cfg)
    df_veiculos = read_silver_veiculos(spark, cfg)
    df_geocercas = read_silver_geocercas(spark, cfg)
    df_eventos = read_gold_eventos(spark, cfg)

    results: dict[str, str] = {}

    metric_functions = {
        "viagens_por_mes_status": (viagens_por_mes_status, [df_viagens]),
        "tempo_medio_viagem_rota": (tempo_medio_viagem_rota, [df_viagens]),
        "velocidade_media_viagem": (velocidade_media_viagem, [df_viagens]),
        "taxa_atraso_mes": (taxa_atraso_mes, [df_viagens]),
        "top_motoristas": (top_motoristas, [df_viagens]),
        "utilizacao_frota_mes": (utilizacao_frota_mes, [df_viagens, df_veiculos]),
        "tempo_parado_geocerca_tipo": (tempo_parado_geocerca_tipo, [df_eventos, df_geocercas]),
    }

    for metric_name, (func, args) in metric_functions.items():
        output_path = f"{metricas_base}/{metric_name}"
        df_result = func(*args)
        write_gold(df_result, output_path)
        results[metric_name] = output_path

    return results
