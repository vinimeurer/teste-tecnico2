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

APP_NAME: str = os.getenv("APP_NAME", "PipelineLogisticoBronze")

SOURCES: dict[str, dict[str, str | list[str]]] = {
    "veiculos": {
        "raw_path": os.path.join(RAW_DATA_PATH, "veiculos", "veiculos.csv"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "veiculos"),
        "format": "csv",
    },
    "motoristas": {
        "raw_path": os.path.join(RAW_DATA_PATH, "motoristas", "motoristas.json"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "motoristas"),
        "format": "json",
    },
    "geocercas": {
        "raw_path": os.path.join(RAW_DATA_PATH, "geocercas", "geocercas.geojson"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "geocercas"),
        "format": "geojson",
    },
    "viagens": {
        "raw_path": os.path.join(RAW_DATA_PATH, "viagens", "viagens.csv"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "viagens"),
        "format": "csv",
    },
    "posicoes": {
        "raw_path": os.path.join(RAW_DATA_PATH, "rastreamento", "posicoes.parquet"),
        "bronze_path": os.path.join(BRONZE_OUTPUT_PATH, "rastreamento"),
        "format": "parquet",
    },
}
