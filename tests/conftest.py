from unittest.mock import MagicMock, PropertyMock, patch

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
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    bronze.mkdir()
    silver.mkdir()
    return {
        "veiculos": {
            "bronze_path": str(bronze / "veiculos"),
            "silver_path": str(silver / "veiculos"),
        },
        "motoristas": {
            "bronze_path": str(bronze / "motoristas"),
            "silver_path": str(silver / "motoristas"),
        },
        "geocercas": {
            "bronze_path": str(bronze / "geocercas"),
            "silver_path": str(silver / "geocercas"),
        },
        "viagens": {
            "bronze_path": str(bronze / "viagens"),
            "silver_path": str(silver / "viagens"),
        },
        "posicoes": {
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


SILVER_MODULES = [
    "src.silver.silver_veiculos",
    "src.silver.silver_motoristas",
    "src.silver.silver_geocercas",
    "src.silver.silver_viagens",
    "src.silver.silver_posicoes",
]


@pytest.fixture(autouse=True)
def mock_pyspark_in_modules(monkeypatch):
    for mod_name in SILVER_MODULES:
        mod = __import__(mod_name, fromlist=[""])
        for attr, mock_factory in [
            ("col", _mock_col),
            ("trim", _mock_trim),
            ("lit", _mock_lit),
            ("when", _mock_when),
        ]:
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, mock_factory)
