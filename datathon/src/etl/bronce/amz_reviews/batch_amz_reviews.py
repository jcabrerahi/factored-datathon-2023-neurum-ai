# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

!pip install unidecode boto3

# COMMAND ----------

from datathon.src.utils.data_transformation import DateTransformation
from datathon.src.utils.data_transformation import EnrichingTransformation
from datathon.config.logging import setup_logging
from datathon.config.integration_config import AWSConfig

from pyspark.sql.functions import col
import logging
import boto3
import time
from typing import List

# COMMAND ----------

# MAGIC %md
# MAGIC # Config & credentials

# COMMAND ----------

# Azure credentials
storage_account_name = dbutils.secrets.get(scope="azure_credentials", key="storage.account_name")
sas_token = dbutils.secrets.get(scope="azure_credentials", key="storage.sas_token")
container_name = "source-files"

# AWS credentials
aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

# == S3 config
path_bucket = "neurum-ai-factored-datathon"
path_bronce_amz_reviews = f"s3a://{path_bucket}/bronce/amazon/reviews"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews

# COMMAND ----------

@setup_logging("logs_batch_amz_review")
def load_chunked_data_from_paths(container_name: str, paths_partitions: List[str], path_folder: str, storage_account_name: str):
    """
     Loads data from a list of routes into a cumulative Spark DataFrame.

     Parameters:
     - container_name (str): Name of the container in which the data is located.
     - paths_partitions (list): List of paths where the data partitions are located.
     - path_folder (str): Path to the folder that contains the data partitions.
     - storage_account_name (str): Name of the Azure storage account where the data is located.

     Returns:
     - df_all_partitions (DataFrame): Spark DataFrame containing all data loaded from the provided paths.

     The function iterates through the list of routes, loading the data for each route into a Spark DataFrame.
     The data for all routes is accumulated in a single DataFrame. Every 5 partitions is saved
     the DataFrame in delta format in the path indicated in 'path_bronce_amz_reviews'.
     In addition, transformations are performed on the extracted data, such as changing the date format.

    """

    df_all_partitions = None
    counter = 0

    logging.info("Iniciando proceso de carga de datos")

    # Recorrer los paths_partitions y cargar los datos en el DataFrame acumulativo
    for path_info in paths_partitions:
        path = f"{path_folder}/{path_info.name}"
        counter += 1
        print(path, counter)

        # Extract
        df_partition = spark.read.format("json").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

        # Transform
        df_reviews = EnrichingTransformation.get_formatted_column_names(df_partition)
        df_reviews = DateTransformation.unix_to_date(df_reviews, "unix_review_time")
        df_reviews = EnrichingTransformation.add_file_source_column(df_reviews)
        df_reviews = (DateTransformation.extract_year_month(df_reviews, "unix_review_time_date")
                      .withColumnRenamed("unix_review_time_date_year_month", "year_month")
                      )

        if df_all_partitions is None:
            df_all_partitions = df_reviews
        else:
            df_all_partitions = df_all_partitions.union(df_reviews)
        
        # # Load
        # == Append to delta table every 100 partitions
        if counter % 500 == 0:
            print("="*10,"Saving chunk into delta")
            logging.info(f"Saving chunk into delta")
            counter = 0

            start_time = time.time()

            (df_all_partitions.coalesce(1)
                .write.format("delta")
                .partitionBy('year_month')
                .option("overwriteSchema", "true")
                .mode("append")
                .save(path_bronce_amz_reviews)
            )

            df_all_partitions = None

            end_time = time.time()
            elapsed_time = end_time - start_time

            logging.info(f"Saved chunk into delta | Last_partition_processed: {path} | Elapsed time: {elapsed_time} seconds")
            
    return df_all_partitions

# COMMAND ----------

# Configure AWS credentials to logging
aws_config = AWSConfig(access_key=aws_access_key, secret_key=aws_secret_key)
aws_config.setup_aws_credentials()

path_reviews = "amazon_reviews/"
paths = dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_reviews}")
load_chunked_data_from_paths(container_name, paths, path_reviews[:-1], storage_account_name)

# COMMAND ----------

# df_test.explain(mode='cost')

# COMMAND ----------

dbutils.notebook.exit("Done")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data quality

# COMMAND ----------

# 55933 per partition
df_test = spark.read.format("delta").load(path_bronce_amz_reviews)
# print(df_test.count())
display(df_test.groupBy("file_source").count())

# COMMAND ----------

print(df_test.count())

# COMMAND ----------

display(df_test.filter(col("asin") == "1622234642"))

# COMMAND ----------


