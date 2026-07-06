import os

RAW_DATA_PATH: str = os.getenv(
    "RAW_DATA_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"),
)

BRONZE_OUTPUT_PATH: str = os.getenv(
    "BRONZE_OUTPUT_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "bronze"
    ),
)

SILVER_OUTPUT_PATH: str = os.getenv(
    "SILVER_OUTPUT_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "silver"
    ),
)

GOLD_OUTPUT_PATH: str = os.getenv(
    "GOLD_OUTPUT_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "gold"
    ),
)

APP_NAME_BRONZE: str = os.getenv("APP_NAME_BRONZE", "PipelineLogisticoBronze")
APP_NAME_SILVER: str = os.getenv("APP_NAME_SILVER", "PipelineLogisticoSilver")
APP_NAME_GOLD: str = os.getenv("APP_NAME_GOLD", "PipelineLogisticoGold")

SOURCES: dict[str, dict[str, str | list[str]]] = {
    "veiculos": {
        "raw_path": os.path.join(RAW_DATA_PATH, "veiculos", "veiculos.csv"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "veiculos"),
        "silver_path": os.path.join(SILVER_OUTPUT_PATH, "veiculos"),
        "gold_path": os.path.join(GOLD_OUTPUT_PATH, "veiculos"),
        "format": "csv",
    },
    "motoristas": {
        "raw_path": os.path.join(RAW_DATA_PATH, "motoristas", "motoristas.json"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "motoristas"),
        "silver_path": os.path.join(SILVER_OUTPUT_PATH, "motoristas"),
        "gold_path": os.path.join(GOLD_OUTPUT_PATH, "motoristas"),
        "format": "json",
    },
    "geocercas": {
        "raw_path": os.path.join(RAW_DATA_PATH, "geocercas", "geocercas.geojson"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "geocercas"),
        "silver_path": os.path.join(SILVER_OUTPUT_PATH, "geocercas"),
        "gold_path": os.path.join(GOLD_OUTPUT_PATH, "geocercas"),
        "format": "geojson",
    },
    "viagens": {
        "raw_path": os.path.join(RAW_DATA_PATH, "viagens", "viagens.csv"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "viagens"),
        "silver_path": os.path.join(SILVER_OUTPUT_PATH, "viagens"),
        "gold_path": os.path.join(GOLD_OUTPUT_PATH, "viagens"),
        "format": "csv",
    },
    "posicoes": {
        "raw_path": os.path.join(RAW_DATA_PATH, "rastreamento", "posicoes.parquet"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "rastreamento"),
        "silver_path": os.path.join(SILVER_OUTPUT_PATH, "rastreamento"),
        "gold_path": os.path.join(GOLD_OUTPUT_PATH, "rastreamento"),
        "format": "parquet",
    },
}
