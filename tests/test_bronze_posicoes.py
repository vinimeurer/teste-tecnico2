import pytest

import src.bronze.bronze_posicoes as bp_mod


def test_read_raw_data_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = bp_mod.read_raw_data(spark, sample_config["posicoes"]["raw_path"])
    spark.read.parquet.assert_called_once_with(
        sample_config["posicoes"]["raw_path"]
    )
    assert result is df


def test_read_raw_data_with_path(spark, df):
    spark.read.parquet.return_value = df
    result = bp_mod.read_raw_data(spark, "/custom/path.parquet")
    spark.read.parquet.assert_called_once_with("/custom/path.parquet")
    assert result is df


def test_validate_and_cast_schema_all_columns(spark, df):
    df.columns = [
        "posicao_id", "viagem_id", "veiculo_id", "latitude", "longitude",
        "timestamp", "velocidade_kmh", "ignicao", "odometro_metros",
    ]
    df.select.return_value = df

    result = bp_mod.validate_and_cast_schema(df)

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 9
    assert result is df


def test_validate_and_cast_schema_missing_column(spark, df):
    df.columns = ["posicao_id"]

    with pytest.raises(ValueError, match="Coluna obrigatória.*ausente em posicoes"):
        bp_mod.validate_and_cast_schema(df)


def test_write_bronze(spark, df):
    output_path = "/some/bronze/path"
    result = bp_mod.write_bronze(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_extract_to_bronze(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = [
        "posicao_id", "viagem_id", "veiculo_id", "latitude", "longitude",
        "timestamp", "velocidade_kmh", "ignicao", "odometro_metros",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    result = bp_mod.extract_to_bronze(spark, sample_config)

    spark.read.parquet.assert_called_once_with(
        sample_config["posicoes"]["raw_path"]
    )
    assert result == sample_config["posicoes"]["bronze_path"]
    df.write.mode.assert_called_once_with("overwrite")


def test_extract_to_bronze_default_config(spark, df, monkeypatch):
    spark.read.parquet.return_value = df
    df.columns = [
        "posicao_id", "viagem_id", "veiculo_id", "latitude", "longitude",
        "timestamp", "velocidade_kmh", "ignicao", "odometro_metros",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    test_raw = "/fake/raw/posicoes.parquet"
    test_bronze = "/fake/bronze/posicoes"
    monkeypatch.setattr(
        bp_mod, "SOURCES",
        {"posicoes": {"raw_path": test_raw, "bronze_path": test_bronze}},
    )

    result = bp_mod.extract_to_bronze(spark)

    spark.read.parquet.assert_called_once_with(test_raw)
    assert result == test_bronze
