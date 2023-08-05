""" Modulo to test Data transformations."""
import pytest
from pyspark.sql import SparkSession
from src.utils.data_transformation import EnrichingTransformation, DateTransformation

spark = SparkSession.builder.getOrCreate()

def test_get_formatted_column_names():
    """ Test get_foomatted_colum_names funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])
    dataframe_transformed = EnrichingTransformation.get_formatted_column_names(dataframe)

    assert dataframe_transformed.columns == ["name", "registration_date"], "Column names should be transformed to snakecase."

def test_add_file_source_column():
    """ Test add_file_source_column funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])
    dataframe_transformed = EnrichingTransformation.add_file_source_column(dataframe)

    assert "file_source" in dataframe_transformed.columns, "Column 'file_source' should be added."

def test_unix_to_date():
    """ Test unix_to_date funcitons."""
    data = [("Alice", 1577836800), ("Bob", 1580515200)]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Unix"])
    dataframe_transformed = DateTransformation.unix_to_date(dataframe, "Registration Unix")

    assert "Registration Unix_date" in dataframe_transformed.columns, "Column 'Registration Unix_date' should be added."

def test_extract_year_month():
    """ Test extract_year_month funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])
    dataframe_transformed = DateTransformation.extract_year_month(dataframe, "Registration Date")

    assert "Registration Date_year_month" in dataframe_transformed.columns, "Column 'Registration Date_year_month' should be added."

def test_extract_year_month_no_col():
    """ Test extract_year_month_no_column funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])

    with pytest.raises(ValueError) as excinfo:
        DateTransformation.extract_year_month(dataframe, "Non-existing column")

    assert "Column 'Non-existing column' not found in DataFrame." in str(excinfo.value)
    
def test_extract_year():
    """ Test extract_year funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])
    dataframe_transformed = DateTransformation.extract_year(dataframe, "Registration Date")

    assert "Registration Date_year" in dataframe_transformed.columns, "Column 'Registration Date_year' should be added."

def test_extract_year_no_col():
    """ Test extract_year_no_column funcitons."""
    data = [("Alice", "2020-01-01"), ("Bob", "2020-02-01")]
    dataframe = spark.createDataFrame(data, ["Name", "Registration Date"])

    with pytest.raises(ValueError) as excinfo:
        DateTransformation.extract_year(dataframe, "Non-existing column")

    assert "Column 'Non-existing column' not found in DataFrame." in str(excinfo.value)
