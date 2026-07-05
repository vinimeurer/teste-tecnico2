import pytest

import src.bronze.bronze_viagens as bv_mod


def test_read_raw_data_with_config(spark, df, sample_config):
    spark.read.csv.return_value = df
    result = bv_mod.read_raw_data(spark, sample_config["viagens"]["raw_path"])
    spark.read.csv.assert_called_once_with(
        sample_config["viagens"]["raw_path"], header=True, inferSchema=True
    )
    assert result is df


def test_read_raw_data_with_path(spark, df):
    spark.read.csv.return_value = df
    result = bv_mod.read_raw_data(spark, "/custom/path.csv")
    spark.read.csv.assert_called_once_with(
        "/custom/path.csv", header=True, inferSchema=True
    )
    assert result is df


def test_validate_and_cast_schema_all_columns(spark, df):
    df.columns = [
        "viagem_id", "veiculo_id", "motorista_id", "geocerca_origem_id",
        "geocerca_destino_id", "data_inicio", "data_fim_prevista",
        "data_fim_real", "status", "distancia_km", "peso_carga_kg", "nota_fiscal",
    ]
    df.select.return_value = df

    result = bv_mod.validate_and_cast_schema(df)

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 12
    assert result is df


def test_validate_and_cast_schema_missing_column(spark, df):
    df.columns = ["viagem_id"]

    with pytest.raises(ValueError, match="Coluna obrigatória.*ausente em viagens"):
        bv_mod.validate_and_cast_schema(df)


def test_write_bronze(spark, df):
    output_path = "/some/bronze/path"
    result = bv_mod.write_bronze(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_extract_to_bronze(spark, df, sample_config):
    spark.read.csv.return_value = df
    df.columns = [
        "viagem_id", "veiculo_id", "motorista_id", "geocerca_origem_id",
        "geocerca_destino_id", "data_inicio", "data_fim_prevista",
        "data_fim_real", "status", "distancia_km", "peso_carga_kg", "nota_fiscal",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    result = bv_mod.extract_to_bronze(spark, sample_config)

    spark.read.csv.assert_called_once_with(
        sample_config["viagens"]["raw_path"], header=True, inferSchema=True
    )
    assert result == sample_config["viagens"]["bronze_path"]
    df.write.mode.assert_called_once_with("overwrite")


def test_extract_to_bronze_default_config(spark, df, monkeypatch):
    spark.read.csv.return_value = df
    df.columns = [
        "viagem_id", "veiculo_id", "motorista_id", "geocerca_origem_id",
        "geocerca_destino_id", "data_inicio", "data_fim_prevista",
        "data_fim_real", "status", "distancia_km", "peso_carga_kg", "nota_fiscal",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    test_raw = "/fake/raw/viagens.csv"
    test_bronze = "/fake/bronze/viagens"
    monkeypatch.setattr(
        bv_mod, "SOURCES",
        {"viagens": {"raw_path": test_raw, "bronze_path": test_bronze}},
    )

    result = bv_mod.extract_to_bronze(spark)

    spark.read.csv.assert_called_once_with(test_raw, header=True, inferSchema=True)
    assert result == test_bronze
