from unittest.mock import MagicMock, PropertyMock, patch

import pyspark.sql.functions as F
import pytest


class MockColumn:
    """Simula pyspark.sql.Column sem precisar de JVM."""

    def __init__(self, name=None):
        self._name = name

    def alias(self, name):
        return MockColumn(name)

    def cast(self, dtype):
        return MockColumn(f"{self._name}::cast")

    def isin(self, values):
        return MockColumn(f"{self._name}.isin({values})")

    def isNull(self):
        return MockColumn(f"{self._name}.isNull")

    def getItem(self, key):
        return MockColumn(f"{self._name}[{key}]")

    def isNotNull(self):
        return MockColumn(f"{self._name}.isNotNull")

    def __eq__(self, other):
        return MockColumn(f"{self._name}=={other}")

    def __ge__(self, other):
        return MockColumn(f"{self._name}>={other}")

    def __lt__(self, other):
        return MockColumn(f"{self._name}<{other}")

    def __and__(self, other):
        return MockColumn(f"({self._name})&({other})")

    def __invert__(self):
        return MockColumn(f"~({self._name})")

    def __repr__(self):
        return f"MockColumn({self._name})"


class MockDataFrame(MagicMock):
    def col(self, name):
        return MockColumn(name)


@pytest.fixture
def spark():
    s = MagicMock()
    reader = MagicMock()
    type(s).read = PropertyMock(return_value=reader)
    return s


@pytest.fixture
def df():
    return MockDataFrame()


@pytest.fixture
def sample_config(tmp_path):
    raw = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    raw.mkdir()
    bronze.mkdir()
    silver.mkdir()
    return {
        "veiculos": {
            "raw_path": str(raw / "veiculos.csv"),
            "bronze_path": str(bronze / "veiculos"),
            "silver_path": str(silver / "veiculos"),
        },
        "motoristas": {
            "raw_path": str(raw / "motoristas.json"),
            "bronze_path": str(bronze / "motoristas"),
            "silver_path": str(silver / "motoristas"),
        },
        "geocercas": {
            "raw_path": str(raw / "geocercas.geojson"),
            "bronze_path": str(bronze / "geocercas"),
            "silver_path": str(silver / "geocercas"),
        },
        "viagens": {
            "raw_path": str(raw / "viagens.csv"),
            "bronze_path": str(bronze / "viagens"),
            "silver_path": str(silver / "viagens"),
        },
        "posicoes": {
            "raw_path": str(raw / "posicoes.parquet"),
            "bronze_path": str(bronze / "posicoes"),
            "silver_path": str(silver / "posicoes"),
        },
    }


def _mock_col(name):
    return MockColumn(name)

def _mock_trim(col_expr):
    return MockColumn(f"trim({col_expr})")

def _mock_lit(val):
    return MockColumn(f"lit({val})")

def _mock_when(*args):
    m = MagicMock()
    m.otherwise.return_value = MockColumn("when_otherwise")
    return m


def _mock_explode(col_expr):
    return MockColumn(f"explode({col_expr})")


MODULES = [
    "src.silver.silver_veiculos",
    "src.silver.silver_motoristas",
    "src.silver.silver_geocercas",
    "src.silver.silver_viagens",
    "src.silver.silver_posicoes",
    "src.bronze.bronze_veiculos",
    "src.bronze.bronze_motoristas",
    "src.bronze.bronze_geocercas",
    "src.bronze.bronze_viagens",
    "src.bronze.bronze_posicoes",
]

MOCK_ATTRS: list[tuple[str, callable]] = [
    ("col", _mock_col),
    ("trim", _mock_trim),
    ("lit", _mock_lit),
    ("when", _mock_when),
    ("explode", _mock_explode),
]


@pytest.fixture(autouse=True)
def mock_pyspark_in_modules(monkeypatch):
    for mod_name in MODULES:
        mod = __import__(mod_name, fromlist=[""])
        for attr, mock_factory in MOCK_ATTRS:
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, mock_factory)

    monkeypatch.setattr(F, "col", _mock_col)
    monkeypatch.setattr(F, "explode", _mock_explode)
