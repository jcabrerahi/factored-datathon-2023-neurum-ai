from datathon.src.utils.functions import custom_functions

# Standard Python Libraries
from typing import List, Union

# Third-Party Libraries
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, from_unixtime
from pyspark.sql.types import ArrayType, StructType


class DateTransformation:
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
