import src.silver.silver_geocercas as sg_mod


def test_read_bronze_with_config(spark, df, sample_config):
    spark.read.parquet.return_value = df
    result = sg_mod.read_bronze(spark, sample_config)
    spark.read.parquet.assert_called_once_with(sample_config["geocercas"]["bronze_path"])
    assert result is df


def test_read_bronze_default_config(spark, df, monkeypatch):
    test_path = "/fake/bronze/geocercas"
    monkeypatch.setattr(sg_mod, "SOURCES", {"geocercas": {"bronze_path": test_path}})
    spark.read.parquet.return_value = df
    result = sg_mod.read_bronze(spark)
    spark.read.parquet.assert_called_once_with(test_path)
    assert result is df


def test_clean_explodes_properties(df):
    df.columns = ["properties", "geometry"]
    df.withColumn.return_value = df
    df.select.return_value = df

    result = sg_mod.clean(df)

    assert df.withColumn.call_count == 6
    expected_cols = ["geocerca_id", "nome", "tipo", "uf", "raio_km", "ativo"]
    for c in expected_cols:
        df.withColumn.assert_any_call(c, df.col("properties").getItem(c))

    df.select.assert_called_once_with("geocerca_id", "nome", "tipo", "uf", "raio_km", "ativo", df.col("geometry"))
    assert result is df


def test_write_silver(df):
    output_path = "/some/silver/path"
    result = sg_mod.write_silver(df, output_path)
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode().parquet.assert_called_once_with(output_path)
    assert result == output_path


def test_transform_to_silver(spark, df, sample_config):
    spark.read.parquet.return_value = df
    df.columns = ["properties", "geometry"]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.write.parquet.return_value = None

    result = sg_mod.transform_to_silver(spark, sample_config)

    spark.read.parquet.assert_called_once_with(sample_config["geocercas"]["bronze_path"])
    assert result == sample_config["geocercas"]["silver_path"]
    df.write.mode.assert_called_once_with("overwrite")
