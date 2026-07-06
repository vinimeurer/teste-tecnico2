from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit
from pyspark.sql.types import BooleanType

from src.config import SOURCES


def _ponto_dentro_poligono(lat: float, lon: float, coordinates: list) -> bool:
    """Check if a point is inside a polygon using Shapely.

    Parameters
    ----------
    lat : float
        Latitude of the point.
    lon : float
        Longitude of the point.
    coordinates : list
        Polygon coordinates in GeoJSON format: list of rings,
        each ring being a list of [lon, lat] pairs.

    Returns
    -------
    bool
        True if the point is inside the polygon, False otherwise.
    """
    from shapely.geometry import Point, Polygon

    if not coordinates or not coordinates[0]:
        return False
    try:
        exterior = coordinates[0]
        interiors = coordinates[1:] if len(coordinates) > 1 else []
        polygon = Polygon(exterior, interiors)
        return polygon.covers(Point(lon, lat))
    except Exception:
        return False


def _criar_udf_ponto_dentro_poligono():
    """Create a UDF for point-in-polygon checking.

    Returns
    -------
    callable
        A PySpark UDF wrapped around _ponto_dentro_poligono.
    """
    return udf(_ponto_dentro_poligono, BooleanType())


def read_silver_posicoes(spark: SparkSession, config: dict | None = None) -> DataFrame:
    """Read silver posicoes from the specified configuration.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    config : dict or None
        Configuration dictionary. If None, defaults to SOURCES.

    Returns
    -------
    DataFrame
        DataFrame containing silver posicoes data.
    """
    source = (config or SOURCES).get("posicoes", {})
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
        DataFrame containing silver geocercas data with geometry struct.
    """
    source = (config or SOURCES).get("geocercas", {})
    path: str = source.get("silver_path", "")
    return spark.read.parquet(path)


def enrich_posicoes(df_posicoes: DataFrame, df_geocercas: DataFrame) -> DataFrame:
    """Classify each GPS position as inside a geocerca or on route.

    Performs a cross-join between positions and geocercas, applies a
    point-in-polygon UDF, and aggregates matching geocercas per position.

    Parameters
    ----------
    df_posicoes : DataFrame
        DataFrame with silver posicoes (latitude, longitude, etc.).
    df_geocercas : DataFrame
        DataFrame with silver geocercas (geometry, geocerca_id, nome).

    Returns
    -------
    DataFrame
        Enriched DataFrame with columns:
        - All original posicoes columns
        - classificacao: 'em_geocerca' or 'em_rota'
        - geocerca_id: identifier of the matched geocerca (null if em_rota)
        - nome_geocerca: name of the matched geocerca (null if em_rota)
    """
    ponto_dentro_poligono_udf = _criar_udf_ponto_dentro_poligono()

    geocercas_sel = df_geocercas.select(
        col("geocerca_id"),
        col("nome").alias("nome_geocerca"),
        col("geometry.coordinates").alias("polygon_coords"),
    )

    df_cross = df_posicoes.crossJoin(geocercas_sel)

    df_cross = df_cross.withColumn(
        "dentro",
        ponto_dentro_poligono_udf(col("latitude"), col("longitude"), col("polygon_coords")),
    )

    df_dentro = df_cross.filter(col("dentro")).select(
        "posicao_id",
        col("geocerca_id"),
        col("nome_geocerca"),
    )

    df_agg = df_dentro.groupBy("posicao_id").agg(
        collect_list("geocerca_id").alias("geocercas_ids"),
        collect_list("nome_geocerca").alias("nomes_geocercas"),
    )

    df_enriquecido = df_posicoes.join(df_agg, "posicao_id", "left")

    df_enriquecido = df_enriquecido.withColumn(
        "classificacao",
        when(col("geocercas_ids").isNotNull(), lit("em_geocerca")).otherwise(lit("em_rota")),
    )

    df_enriquecido = df_enriquecido.withColumn(
        "geocerca_id",
        when(col("geocercas_ids").isNotNull(), col("geocercas_ids").getItem(0)).otherwise(lit(None)),
    )

    df_enriquecido = df_enriquecido.withColumn(
        "nome_geocerca",
        when(col("nomes_geocercas").isNotNull(), col("nomes_geocercas").getItem(0)).otherwise(lit(None)),
    )

    return df_enriquecido.select(
        col("posicao_id"),
        col("viagem_id"),
        col("veiculo_id"),
        col("latitude"),
        col("longitude"),
        col("timestamp"),
        col("velocidade_kmh"),
        col("ignicao"),
        col("odometro_metros"),
        col("classificacao"),
        col("geocerca_id"),
        col("nome_geocerca"),
    )


def write_gold(df: DataFrame, output_path: str) -> str:
    """Write the enriched DataFrame to the gold layer as Parquet.

    Parameters
    ----------
    df : DataFrame
        Enriched DataFrame to be written.
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
    """Transform silver posicoes and geocercas to gold enriched positions.

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
    gold_path: str = cfg.get("posicoes", {}).get("gold_path", "")
    df_posicoes = read_silver_posicoes(spark, cfg)
    df_geocercas = read_silver_geocercas(spark, cfg)
    df_enriquecido = enrich_posicoes(df_posicoes, df_geocercas)
    return write_gold(df_enriquecido, gold_path)
