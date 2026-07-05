import src.bronze.bronze_geocercas as bg_mod


def test_read_raw_data_with_config(spark, df, sample_config):
    spark.read.option.return_value.json.return_value = df
    result = bg_mod.read_raw_data(spark, sample_config["geocercas"]["raw_path"])
    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with(
        sample_config["geocercas"]["raw_path"]
    )
    assert result is df


def test_read_raw_data_with_path(spark, df):
    spark.read.option.return_value.json.return_value = df
    result = bg_mod.read_raw_data(spark, "/custom/path.geojson")
    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with("/custom/path.geojson")
    assert result is df


def test_explode_features(spark, df):
    df.select.return_value = df
    df.columns = ["features"]

    result = bg_mod.explode_features(df)

    assert df.select.call_count == 2
    assert result is df


def test_write_bronze(spark, df):
    output_path = "/some/bronze/path"
    result = bg_mod.write_bronze(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_extract_to_bronze(spark, df, sample_config):
    spark.read.option.return_value.json.return_value = df
    df.select.return_value = df
    df.write.parquet.return_value = None

    result = bg_mod.extract_to_bronze(spark, sample_config)

    spark.read.option.assert_called_once_with("multiLine", True)
    spark.read.option().json.assert_called_once_with(
        sample_config["geocercas"]["raw_path"]
    )
    assert df.select.call_count == 2
    assert result == sample_config["geocercas"]["bronze_path"]
    df.write.mode.assert_called_once_with("overwrite")


def test_extract_to_bronze_default_config(spark, df, monkeypatch):
    spark.read.option.return_value.json.return_value = df
    df.select.return_value = df
    df.write.parquet.return_value = None

    test_raw = "/fake/raw/geocercas.geojson"
    test_bronze = "/fake/bronze/geocercas"
    monkeypatch.setattr(
        bg_mod, "SOURCES",
        {"geocercas": {"raw_path": test_raw, "bronze_path": test_bronze}},
    )

    result = bg_mod.extract_to_bronze(spark)

    assert result == test_bronze
