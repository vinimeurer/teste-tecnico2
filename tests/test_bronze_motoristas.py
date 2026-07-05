import pytest

import src.bronze.bronze_motoristas as bm_mod


def test_read_raw_data_with_config(spark, df, sample_config):
    spark.read.option.return_value.json.return_value = df
    result = bm_mod.read_raw_data(spark, sample_config["motoristas"]["raw_path"])
    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with(
        sample_config["motoristas"]["raw_path"]
    )
    assert result is df


def test_read_raw_data_with_path(spark, df):
    spark.read.option.return_value.json.return_value = df
    result = bm_mod.read_raw_data(spark, "/custom/path.json")
    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with("/custom/path.json")
    assert result is df


def test_validate_and_cast_schema_all_columns(spark, df):
    df.columns = [
        "motorista_id", "nome", "cpf", "cnh", "categoria_cnh",
        "validade_cnh", "telefone", "data_admissao", "base_operacional", "status",
    ]
    df.select.return_value = df

    result = bm_mod.validate_and_cast_schema(df)

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 10
    assert result is df


def test_validate_and_cast_schema_missing_column(spark, df):
    df.columns = ["motorista_id"]

    with pytest.raises(ValueError, match="Coluna obrigatória.*ausente em motoristas"):
        bm_mod.validate_and_cast_schema(df)


def test_write_bronze(spark, df):
    output_path = "/some/bronze/path"
    result = bm_mod.write_bronze(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_extract_to_bronze(spark, df, sample_config):
    spark.read.option.return_value.json.return_value = df
    df.columns = [
        "motorista_id", "nome", "cpf", "cnh", "categoria_cnh",
        "validade_cnh", "telefone", "data_admissao", "base_operacional", "status",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    result = bm_mod.extract_to_bronze(spark, sample_config)

    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with(
        sample_config["motoristas"]["raw_path"]
    )
    assert result == sample_config["motoristas"]["bronze_path"]
    df.write.mode.assert_called_once_with("overwrite")


def test_extract_to_bronze_default_config(spark, df, monkeypatch):
    spark.read.option.return_value.json.return_value = df
    df.columns = [
        "motorista_id", "nome", "cpf", "cnh", "categoria_cnh",
        "validade_cnh", "telefone", "data_admissao", "base_operacional", "status",
    ]
    df.select.return_value = df
    df.write.parquet.return_value = None

    test_raw = "/fake/raw/motoristas.json"
    test_bronze = "/fake/bronze/motoristas"
    monkeypatch.setattr(
        bm_mod, "SOURCES",
        {"motoristas": {"raw_path": test_raw, "bronze_path": test_bronze}},
    )

    result = bm_mod.extract_to_bronze(spark)

    assert result == test_bronze
