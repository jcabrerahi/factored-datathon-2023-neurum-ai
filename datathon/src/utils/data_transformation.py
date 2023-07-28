from datathon.src.utils.functions import custom_functions

# Standard Python Libraries
from typing import List, Union

# Third-Party Libraries
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date, from_unixtime, date_format, input_file_name
from pyspark.sql.types import ArrayType, StructType


class EnrichingTransformation:
    """
    DateTransformation class contains methods to transform dates
    """
    @staticmethod
    def get_formatted_column_names(dataframe: DataFrame) -> DataFrame:
        """
        get_formatted_column_names get a dataframe and return it with the columns names formatted
        Args:
            dataframe: DataFrame
        Returns:
            DataFrame with the columns names formatted
        """
        columns = dataframe.columns
        columns_replaced = [custom_functions.to_snakecase(column) for column in columns]
        for index, column_name in enumerate(columns):
            dataframe = dataframe.withColumnRenamed(
                column_name, columns_replaced[index]
            )
        return dataframe 
    
    
    @staticmethod
    def add_file_source_column(df, column_name="file_source"):
        """
        Add a column to the DataFrame with the name of the input file using input_file_name .

        Parameters:
            - df (pyspark.sql.DataFrame): The DataFrame to which the "file_source" column will be added.
            - column_name (str, optional): The desired name for the column that will contain the name of the file.
            By default, "file_source" is used.

        Return:
            pyspark.sql.DataFrame: The DataFrame with the "file_source" column added.
        """
        return df.withColumn(column_name, input_file_name())


class DateTransformation:
    """
    DateTransformation class contains methods to transform dates
    """  
    @staticmethod
    def unix_to_date(dataframe: DataFrame, column_name: str) -> DataFrame:
        """
        unix_to_date get a dataframe and return it with the column name + '_date' in unix
        format converted to date
        Args:
            dataframe: DataFrame
            column_name: str
        Returns:
            DataFrame with the column in unix format converted to date
        """
        dataframe = dataframe.withColumn(column_name + "_date", to_date(from_unixtime(column_name)))
        return dataframe


    @staticmethod
    def extract_year_month(dataframe: DataFrame, column_name: str) -> DataFrame:
        """
        Extracts the year and month from a date column in the DataFrame.

        Parameters:
            dataframe (DataFrame): The input DataFrame.
            column_name (str): The name of the date column to extract year and month from.

        Returns:
            DataFrame: A new DataFrame with an additional column "year_month" containing
            the year and month extracted from the specified date column.
        """
        if column_name not in dataframe.columns:
            raise ValueError(f"Column '{column_name}' not found in DataFrame.")

        new_column_name = f"{column_name}_year_month"
        df_with_year_month = dataframe.withColumn(new_column_name, date_format(col(column_name), "yyyy-MM"))

        return df_with_year_month
