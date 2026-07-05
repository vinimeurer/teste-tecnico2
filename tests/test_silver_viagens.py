from unittest.mock import ANY

import src.silver.silver_viagens as sv_mod
from tests.conftest import MockColumn


def test_read_bronze_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = sv_mod.read_bronze(spark, sample_config)
    spark.read.parquet.assert_called_once_with(sample_config["viagens"]["bronze_path"])
    assert result is df


def test_read_bronze_default_config(spark, df, monkeypatch):
    test_path = "/fake/bronze/viagens"
    monkeypatch.setattr(sv_mod, "SOURCES", {"viagens": {"bronze_path": test_path}})
    spark.read.parquet.return_value = df
    result = sv_mod.read_bronze(spark)
    spark.read.parquet.assert_called_once_with(test_path)
    assert result is df


def test_carregar_ids_validos(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.select.return_value = df
    df.distinct.return_value = df

    collect_veic = [{"veiculo_id": "VEI-001"}, {"veiculo_id": "VEI-002"}]
    collect_mot = [{"motorista_id": "MOT-001"}, {"motorista_id": "MOT-002"}]
    df.collect.side_effect = [collect_veic, collect_mot]

    veic_validos, mot_validos = sv_mod._carregar_ids_validos(spark, sample_config)

    assert veic_validos == {"VEI-001", "VEI-002"}
    assert mot_validos == {"MOT-001", "MOT-002"}
    assert spark.read.parquet.call_count == 2


def test_clean_calls_select_with_trim(df):
    df.columns = ["viagem_id", "veiculo_id", "motorista_id", "data_inicio",
                  "data_fim_prevista", "data_fim_real", "origem", "destino",
                  "distancia_km", "nota_fiscal", "status", "carga"]
    df.select.return_value = df
    df.withColumn.return_value = df

    result = sv_mod.clean(df, {"VEI-001"}, {"MOT-001"})

    df.select.assert_called_once()
    args = df.select.call_args[0]
    assert len(args[0]) == 12
    assert all(isinstance(a, MockColumn) for a in args[0])

    df.withColumn.assert_any_call("data_inicio", ANY)
    df.withColumn.assert_any_call("data_fim_prevista", ANY)
    df.withColumn.assert_any_call("data_fim_real", ANY)
    df.withColumn.assert_any_call("referencia_valida", ANY)
    df.withColumn.assert_any_call("nota_fiscal", ANY)
    df.withColumn.assert_any_call("status", ANY)
    assert result is df


def test_clean_timestamps_casted(df):
    df.columns = ["data_inicio", "data_fim_prevista", "data_fim_real"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df, set(), set())

    df.withColumn.assert_any_call("data_inicio", ANY)
    df.withColumn.assert_any_call("data_fim_prevista", ANY)
    df.withColumn.assert_any_call("data_fim_real", ANY)


def test_clean_referencia_valida(df):
    df.columns = ["veiculo_id", "motorista_id"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df, {"VEI-001"}, {"MOT-001"})

    df.withColumn.assert_any_call("referencia_valida", ANY)


def test_clean_nota_fiscal_null_filled(df):
    df.columns = ["nota_fiscal"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df, set(), set())

    df.withColumn.assert_any_call("nota_fiscal", ANY)


def test_clean_status_validated(df):
    df.columns = ["status"]
    df.select.return_value = df
    df.withColumn.return_value = df

    sv_mod.clean(df, set(), set())

    df.withColumn.assert_any_call("status", ANY)


def test_write_silver(df):
    output_path = "/some/silver/path"
    result = sv_mod.write_silver(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_transform_to_silver(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = ["viagem_id", "veiculo_id", "motorista_id", "data_inicio",
                  "data_fim_prevista", "data_fim_real", "origem", "destino",
                  "distancia_km", "nota_fiscal", "status", "carga"]
    df.select.return_value = df
    df.distinct.return_value = df
    df.withColumn.return_value = df
    df.write.parquet.return_value = None

    df.collect.side_effect = [
        [{"veiculo_id": "VEI-001"}, {"veiculo_id": "VEI-002"}],
        [{"motorista_id": "MOT-001"}, {"motorista_id": "MOT-002"}],
    ]

    result = sv_mod.transform_to_silver(spark, sample_config)

    assert result == sample_config["viagens"]["silver_path"]
    df.write.mode.assert_called_once_with("overwrite")
