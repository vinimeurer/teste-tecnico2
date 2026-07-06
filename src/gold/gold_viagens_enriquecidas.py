from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, year, month, round, coalesce
from pyspark.sql.types import DoubleType

from src.config import SOURCES


def read_silver_viagens(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read silver viagens from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing silver viagens data.
    """
    source = (config or SOURCES).get("viagens", {})
    path: str = source.get("silver_path", "")
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


def read_silver_motoristas(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read silver motoristas from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing silver motoristas data.
    """
    source = (config or SOURCES).get("motoristas", {})
    path: str = source.get("silver_path", "")
    return spark.read.parquet(path)


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


def enrich_viagens(
    df_viagens: DataFrame,
    df_veiculos: DataFrame,
    df_motoristas: DataFrame,
    df_geocercas: DataFrame,
) -> DataFrame:
    """Enrich viagens with vehicle, driver, and geocerca information and compute metrics.

    Joins viagens with veiculos, motoristas, and geocercas (origin/destination),
    then calculates trip duration, average speed, and delay metrics.

    Parameters
    ----------
    df_viagens : DataFrame
        Silver viagens DataFrame.
    df_veiculos : DataFrame
        Silver veiculos DataFrame.
    df_motoristas : DataFrame
        Silver motoristas DataFrame.
    df_geocercas : DataFrame
        Silver geocercas DataFrame.

    Returns
    -------
    DataFrame
        Enriched viagens DataFrame with vehicle info, driver info,
        origin/destination geocerca info, and computed metrics:
        tempo_viagem_horas, velocidade_media_kmh, atraso_minutos,
        possui_atraso, mes, ano.
    """
    df = df_viagens.join(
        df_veiculos.select("veiculo_id", "placa", "marca", "modelo", "tipo", "ano_fabricacao"),
        "veiculo_id",
        "left",
    )

    df = df.join(
        df_motoristas.select(
            "motorista_id", "nome", "cpf", "categoria_cnh", "base_operacional"
        ),
        "motorista_id",
        "left",
    )

    df_origem = df_geocercas.select(
        col("geocerca_id").alias("geocerca_origem_id"),
        col("nome").alias("origem_nome"),
        col("uf").alias("origem_uf"),
        col("tipo").alias("origem_tipo"),
    )
    df = df.join(df_origem, "geocerca_origem_id", "left")

    df_destino = df_geocercas.select(
        col("geocerca_id").alias("geocerca_destino_id"),
        col("nome").alias("destino_nome"),
        col("uf").alias("destino_uf"),
        col("tipo").alias("destino_tipo"),
    )
    df = df.join(df_destino, "geocerca_destino_id", "left")

    df = df.withColumn(
        "distancia_km_num",
        col("distancia_km").cast(DoubleType()),
    )

    df = df.withColumn(
        "tempo_viagem_horas",
        when(
            col("data_fim_real").isNotNull() & col("data_inicio").isNotNull(),
            (col("data_fim_real").cast("long") - col("data_inicio").cast("long")) / 3600,
        ).otherwise(lit(None)),
    )

    df = df.withColumn(
        "velocidade_media_kmh",
        when(
            col("tempo_viagem_horas").isNotNull()
            & (col("tempo_viagem_horas") > 0)
            & col("distancia_km_num").isNotNull(),
            round(col("distancia_km_num") / col("tempo_viagem_horas"), 2),
        ).otherwise(lit(None)),
    )

    df = df.withColumn(
        "atraso_minutos",
        when(
            col("data_fim_real").isNotNull() & col("data_fim_prevista").isNotNull(),
            (col("data_fim_real").cast("long") - col("data_fim_prevista").cast("long")) / 60,
        ).otherwise(lit(None)),
    )

    df = df.withColumn(
        "possui_atraso",
        when(
            col("atraso_minutos").isNotNull() & (col("atraso_minutos") > 15),
            lit(True),
        ).otherwise(lit(False)),
    )

    df = df.withColumn(
        "velocidade_media_kmh",
        when(col("velocidade_media_kmh") > 276, lit(None))
        .when(col("velocidade_media_kmh") <= 0, lit(None))
        .otherwise(col("velocidade_media_kmh")),
    )

    df = df.withColumn("ano", year(coalesce("data_inicio", "data_fim_prevista")))
    df = df.withColumn("mes", month(coalesce("data_inicio", "data_fim_prevista")))

    cols_to_drop = ["distancia_km_num"]
    existing = set(df.columns)
    df = df.drop(*[c for c in cols_to_drop if c in existing])

    return df


def write_gold(df: DataFrame, output_path: str) -> str:
    """Write the enriched viagens DataFrame to the gold layer as Parquet.

    Parameters
    ----------
    df : DataFrame
        Enriched viagens DataFrame to be written.
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
    """Transform silver data to gold enriched viagens.

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
    gold_path: str = cfg.get("viagens", {}).get("gold_path", "")
    df_viagens = read_silver_viagens(spark, cfg)
    df_veiculos = read_silver_veiculos(spark, cfg)
    df_motoristas = read_silver_motoristas(spark, cfg)
    df_geocercas = read_silver_geocercas(spark, cfg)
    df_enriquecido = enrich_viagens(df_viagens, df_veiculos, df_motoristas, df_geocercas)
    return write_gold(df_enriquecido, gold_path)
