import pytest
from pyspark.sql import SparkSession
from src.utils.data_transformation import EnrichingTransformation, DateTransformation

spark = SparkSession.builder.getOrCreate()

def test_get_formatted_column_names():
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    df = spark.createDataFrame(data, ["Name", "Registration Date"])
    df_transformed = EnrichingTransformation.get_formatted_column_names(df)

    assert df_transformed.columns == ["name", "registration_date"], "Column names should be transformed to snakecase."

def test_add_file_source_column():
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    df = spark.createDataFrame(data, ["Name", "Registration Date"])
    df_transformed = EnrichingTransformation.add_file_source_column(df)

    assert "file_source" in df_transformed.columns, "Column 'file_source' should be added."

def test_unix_to_date():
    data = [("Alice", 1577836800), ("Bob", 1580515200)]
    df = spark.createDataFrame(data, ["Name", "Registration Unix"])
    df_transformed = DateTransformation.unix_to_date(df, "Registration Unix")

    assert "Registration Unix_date" in df_transformed.columns, "Column 'Registration Unix_date' should be added."

def test_extract_year_month():
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    df = spark.createDataFrame(data, ["Name", "Registration Date"])
    df_transformed = DateTransformation.extract_year_month(df, "Registration Date")

    assert "Registration Date_year_month" in df_transformed.columns, "Column 'Registration Date_year_month' should be added."

def test_extract_year_month_no_column():
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    df = spark.createDataFrame(data, ["Name", "Registration Date"])

    with pytest.raises(ValueError) as excinfo:
        DateTransformation.extract_year_month(df, "Non-existing column")

    assert "Column 'Non-existing column' not found in DataFrame." in str(excinfo.value)
