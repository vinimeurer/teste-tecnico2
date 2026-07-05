from unittest.mock import ANY

import src.silver.silver_motoristas as sm_mod
from tests.conftest import MockColumn


def test_read_bronze_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = sm_mod.read_bronze(spark, sample_config)
    spark.read.parquet.assert_called_once_with(sample_config["motoristas"]["bronze_path"])
    assert result is df


def test_read_bronze_default_config(spark, df, monkeypatch):
    test_path = "/fake/bronze/motoristas"
    monkeypatch.setattr(sm_mod, "SOURCES", {"motoristas": {"bronze_path": test_path}})
    spark.read.parquet.return_value = df
    result = sm_mod.read_bronze(spark)
    spark.read.parquet.assert_called_once_with(test_path)
    assert result is df


def test_clean_calls_select_with_trim(df):
    df.columns = ["motorista_id", "nome", "cpf", "cnh", "categoria_cnh",
                  "validade_cnh", "data_admissao", "telefone", "email", "status"]
    df.select.return_value = df
    df.withColumn.return_value = df

    result = sm_mod.clean(df)

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 10
    assert all(isinstance(a, MockColumn) for a in args[0])

    df.withColumn.assert_any_call("nome", ANY)
    df.withColumn.assert_any_call("validade_cnh", ANY)
    df.withColumn.assert_any_call("data_admissao", ANY)
    assert result is df


def test_clean_empty_nome(df):
    df.columns = ["nome"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sm_mod.clean(df)

    df.withColumn.assert_any_call("nome", ANY)


def test_clean_dates_casted(df):
    df.columns = ["validade_cnh", "data_admissao"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sm_mod.clean(df)

    df.withColumn.assert_any_call("validade_cnh", ANY)
    df.withColumn.assert_any_call("data_admissao", ANY)


def test_write_silver(df):
    output_path = "/some/silver/path"
    result = sm_mod.write_silver(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_transform_to_silver(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = ["motorista_id", "nome", "cpf", "cnh", "categoria_cnh",
                  "validade_cnh", "data_admissao", "telefone", "email", "status"]
    df.select.return_value = df
    df.withColumn.return_value = df
    df.write.parquet.return_value = None

    result = sm_mod.transform_to_silver(spark, sample_config)

    spark.read.parquet.assert_called_once_with(sample_config["motoristas"]["bronze_path"])
    assert result == sample_config["motoristas"]["silver_path"]
    df.write.mode.assert_called_once_with("overwrite")
