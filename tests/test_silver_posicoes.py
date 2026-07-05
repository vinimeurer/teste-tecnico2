from unittest.mock import ANY

import src.silver.silver_posicoes as sp_mod


def test_read_bronze_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = sp_mod.read_bronze(spark, sample_config)
    spark.read.parquet.assert_called_once_with(sample_config["posicoes"]["bronze_path"])
    assert result is df


def test_read_bronze_default_config(spark, df, monkeypatch):
    test_path = "/fake/bronze/posicoes"
    monkeypatch.setattr(sp_mod, "SOURCES", {"posicoes": {"bronze_path": test_path}})
    spark.read.parquet.return_value = df
    result = sp_mod.read_bronze(spark)
    spark.read.parquet.assert_called_once_with(test_path)
    assert result is df


def test_clean_applies_four_filters(df):
    df.columns = ["latitude", "longitude", "velocidade_kmh", "timestamp"]
    df.filter.return_value = df

    result = sp_mod.clean(df)

    assert df.filter.call_count == 4
    assert result is df


def test_clean_first_filter_involves_lat_lon(df):
    df.columns = ["latitude", "longitude"]
    df.filter.return_value = df

    sp_mod.clean(df)

    args = df.filter.call_args_list[0]
    expr = args[0][0]
    assert "latitude" in str(expr)
    assert "longitude" in str(expr)


def test_clean_filters_negative_speed(df):
    df.columns = ["velocidade_kmh"]
    df.filter.return_value = df

    sp_mod.clean(df)

    df.filter.assert_any_call(ANY)


def test_clean_filters_high_speed(df):
    df.columns = ["velocidade_kmh"]
    df.filter.return_value = df

    sp_mod.clean(df)

    df.filter.assert_any_call(ANY)


def test_clean_filters_null_timestamp(df):
    df.columns = ["timestamp"]
    df.filter.return_value = df

    sp_mod.clean(df)

    df.filter.assert_any_call(ANY)


def test_write_silver(df):
    output_path = "/some/silver/path"
    result = sp_mod.write_silver(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_transform_to_silver(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = ["latitude", "longitude", "velocidade_kmh", "timestamp"]
    df.filter.return_value = df
    df.write.parquet.return_value = None

    result = sp_mod.transform_to_silver(spark, sample_config)

    spark.read.parquet.assert_called_once_with(sample_config["posicoes"]["bronze_path"])
    assert result == sample_config["posicoes"]["silver_path"]
    assert df.filter.call_count == 4
    df.write.mode.assert_called_once_with("overwrite")
