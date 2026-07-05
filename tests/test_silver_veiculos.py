from unittest.mock import ANY

import src.silver.silver_veiculos as sv_mod
from tests.conftest import MockColumn


def test_read_bronze_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = sv_mod.read_bronze(spark, sample_config)
    spark.read.parquet.assert_called_once_with(sample_config["veiculos"]["bronze_path"])
    assert result is df


def test_read_bronze_default_config(spark, df, monkeypatch):
    test_path = "/fake/bronze/veiculos"
    monkeypatch.setattr(sv_mod, "SOURCES", {"veiculos": {"bronze_path": test_path}})
    spark.read.parquet.return_value = df
    result = sv_mod.read_bronze(spark)
    spark.read.parquet.assert_called_once_with(test_path)
    assert result is df


def test_clean_calls_select_with_trim(df):
    df.columns = ["veiculo_id", "placa", "tipo", "marca", "modelo",
                  "ano_fabricacao", "capacidade_kg", "status",
                  "data_ultima_revisao", "cavalo_reboque", "renavam"]
    df.select.return_value = df
    df.withColumn.return_value = df

    result = sv_mod.clean(df)

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 11
    assert all(isinstance(a, MockColumn) for a in args[0])

    df.withColumn.assert_any_call("placa", ANY)
    df.withColumn.assert_any_call("data_ultima_revisao", ANY)
    df.withColumn.assert_any_call("status", ANY)
    assert result is df


def test_clean_invalida_placa(df):
    df.columns = ["placa"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df)

    df.withColumn.assert_any_call("placa", ANY)


def test_clean_data_ultima_revisao_casted(df):
    df.columns = ["data_ultima_revisao"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df)

    df.withColumn.assert_any_call("data_ultima_revisao", ANY)


def test_clean_status_validated(df):
    df.columns = ["status"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df)

    df.withColumn.assert_any_call("status", ANY)


def test_write_silver(df):
    output_path = "/some/silver/path"
    result = sv_mod.write_silver(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_transform_to_silver(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = ["veiculo_id", "placa", "tipo", "marca", "modelo",
                  "ano_fabricacao", "capacidade_kg", "status",
                  "data_ultima_revisao", "cavalo_reboque", "renavam"]
    df.select.return_value = df
    df.withColumn.return_value = df
    df.write.parquet.return_value = None

    result = sv_mod.transform_to_silver(spark, sample_config)

    spark.read.parquet.assert_called_once_with(sample_config["veiculos"]["bronze_path"])
    assert result == sample_config["veiculos"]["silver_path"]
    df.write.mode.assert_called_once_with("overwrite")
