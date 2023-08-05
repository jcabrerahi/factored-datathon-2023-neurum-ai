# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

!pip install unidecode

# COMMAND ----------

import logging
import time
from typing import List

from pyspark.sql.functions import col

from config.integration_config import AWSConfig
from config.custom_logging import setup_logging
from src.utils.data_transformation import (DateTransformation,
                                                    EnrichingTransformation)

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

aws_config = AWSConfig(aws_access_key_id=aws_access_key, aws_secret_key=aws_secret_key)
boto3_config = aws_config.create_boto3_session()

# == S3 config
path_bucket = "neurum-ai-factored-datathon"
path_bronze_amz_reviews = f"s3a://{path_bucket}/bronce/amazon/reviews"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
spark.conf.set("spark.sql.files.maxRecordsPerFile", 500000)
spark.conf.get("spark.sql.files.maxRecordsPerFile")

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews

# COMMAND ----------

@setup_logging(boto3_config, "logs_batch_amz_review")
def load_chunked_data_from_paths(container_name: str, paths_partitions: List[str], path_folder: str, storage_account_name: str) -> None:
    """
     Loads data from a list of routes into a cumulative Spark DataFrame and save to delta table every 200 partitions.
     In addition, transformations are performed on the extracted data, such as changing the date format, format column
     names, etc.

     Parameters:
     - container_name (str): Name of the container in which the data is located.
     - paths_partitions (list): List of paths where the data partitions are located.
     - path_folder (str): Path to the folder that contains the data partitions.
     - storage_account_name (str): Name of the Azure storage account where the data is located.

     Returns:
     - None
    """
    start_time_total = time.time()
    try:
        end_time_total = time.time()
        elapsed_time_total = round((end_time_total - start_time_total)/60, 2)
        return df_all_partitions

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise e  # opcional: este comando re-lanza la excepción después de registrarla


# COMMAND ----------

# Configure AWS credentials to logging
# aws_config = AWSConfig(access_key=aws_access_key, secret_key=aws_secret_key)
# aws_config.setup_aws_credentials()

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
df_test = spark.read.format("delta").load(path_bronze_amz_reviews)
# print(df_test.count())
# display(df_test.groupBy("file_source").count())
# display(df_test)

# COMMAND ----------

print(df_test.count())

# COMMAND ----------

# path_reviews = "amazon_reviews/partition_1322"
# df_partition = spark.read.format("json").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_reviews}")
# print(df_partition.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{path_bronze_amz_reviews}`")

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{path_bronze_amz_reviews}` ZORDER BY asin")

# COMMAND ----------

spark.sql(f"VACUUM delta.`{path_bronze_amz_reviews}` RETAIN 168 HOURS")
