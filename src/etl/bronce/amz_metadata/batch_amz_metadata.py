# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

!pip install unidecode boto3

# COMMAND ----------

from src.utils.data_transformation import DateTransformation
from src.utils.data_transformation import EnrichingTransformation
from config.custom_logging import setup_logging
from config.integration_config import AWSConfig

from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StructType
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
path_bronce_amz_metadata = f"s3a://{path_bucket}/bronce/amazon/metadata"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") # Restore the pre-Spark 3.0 date parsing behavior. This should allow the date parsing to work as expected

# COMMAND ----------

spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000000)
spark.conf.get("spark.sql.files.maxRecordsPerFile")

# COMMAND ----------

json_schema_metadata = {
    "fields":[{"metadata":{},"name":"also_buy","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"also_view","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"asin","nullable":True,"type":"string"},
              {"metadata":{},"name":"brand","nullable":True,"type":"string"},
              {"metadata":{},"name":"category","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"date","nullable":True,"type":"string"},
              {"metadata":{},"name":"description","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"details","nullable":True,"type":"string"},
              {"metadata":{},"name":"feature","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"fit","nullable":True,"type":"string"},
              {"metadata":{},"name":"image","nullable":True,"type":{"containsNull":True,"elementType":"string","type":"array"}},
              {"metadata":{},"name":"main_cat","nullable":True,"type":"string"},
              {"metadata":{},"name":"price","nullable":True,"type":"string"},
              {"metadata":{},"name":"rank","nullable":True,"type":"string"},
              {"metadata":{},"name":"similar_item","nullable":True,"type":"string"},
              {"metadata":{},"name":"tech1","nullable":True,"type":"string"},
              {"metadata":{},"name":"tech2","nullable":True,"type":"string"},
              {"metadata":{},"name":"title","nullable":True,"type":"string"}],
    "type":"struct"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews

# COMMAND ----------

@setup_logging("logs_batch_amz_metadata")
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
    lenght = len(paths_partitions)
    schema = StructType.fromJson(json_schema_metadata)

    try:
        df_all_partitions = None
        counter = 0
        counter_total = 1100

        logging.info("Iniciando proceso de carga de datos")

        # Recorrer los paths_partitions y cargar los datos en el DataFrame acumulativo
        for path_info in paths_partitions[1100:]:
            path = f"{path_folder}/{path_info.name}"
            counter += 1
            counter_total += 1

            end_time_total = time.time()
            elapsed_time_total = round((end_time_total - start_time_total)/60, 2)

            if counter % 100 == 0:
                print(path, counter, counter_total, "Total time:", elapsed_time_total)

            # Extract
            df_partition = spark.read.format("json").schema(schema).load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

            # Transform
            df_process = EnrichingTransformation.get_formatted_column_names(df_partition)
            df_process = DateTransformation.extract_date_from_string(df_process, "date", "MMMM dd, yyyy")
            df_process = EnrichingTransformation.add_file_source_column(df_process)
            df_process = (
                df_process.withColumn("year", date_format(col("parsed_date"), "yyyy"))
                # .withColumnRenamed("parsed_date_year_month", "year_month")
            )

            if df_all_partitions is None:
                df_all_partitions = df_process
            else:
                df_all_partitions = df_all_partitions.union(df_process)
            
            # # Load
            # == SAVE - Append to delta table every 100 partitions
            if counter % 100 == 0 or counter_total % 1503 == 0:
                print("="*5,f"Saving chunk into delta {counter_total}/{lenght} {round((counter_total/lenght)*100)}%| Elapsed time: {elapsed_time_total} mins.")
                logging.info(f"Saving chunk into delta {counter_total}/{lenght} {round((counter_total/lenght)*100)}%| Elapsed time: {elapsed_time_total} mins.")
                counter = 0

                start_time = time.time()

                (df_all_partitions
                    .write.format("delta")
                    .partitionBy('year')
                    .option("overwriteSchema", "true")
                    .mode("append")
                    .save(path_bronce_amz_metadata)
                )

                df_all_partitions = None

                end_time = time.time()
                elapsed_time = round((end_time - start_time)/60)

                print("="*5,f"Saved chunk into delta | Last_partition_processed: {path} | Saving time: {elapsed_time} seconds | Elapsed time: {elapsed_time_total} mins.")
                logging.info(f"Saved chunk into delta | Last_partition_processed: {path} | Saving time: {elapsed_time} seconds | Elapsed time: {elapsed_time_total} mins.")

        return df_all_partitions

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise e  # opcional: este comando re-lanza la excepción después de registrarla
        

# COMMAND ----------

# Configure AWS credentials to logging
aws_config = AWSConfig(access_key=aws_access_key, secret_key=aws_secret_key)
aws_config.setup_aws_credentials()

path_metadata = "amazon_metadata/"
paths = dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_metadata}")
paths.sort()
load_chunked_data_from_paths(container_name, paths, path_metadata[:-1], storage_account_name)

# COMMAND ----------

display(df_partition.groupBy("asin").count().orderBy(col("count").desc()))

# COMMAND ----------

# df_test.explain(mode='cost')

# COMMAND ----------

dbutils.notebook.exit("Done")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data quality

# COMMAND ----------

# 10000 per partition
df_test = spark.read.format("delta").load(path_bronce_amz_metadata).filter(col("year") >= 2017)
# print(df_test.count())
display(df_test.groupBy("year").count())

# COMMAND ----------

display(df_test.count()) # 14993059. 15023059

# COMMAND ----------

# df_test_sample.count()
df_test.write.format("delta").saveAsTable("test_db.amazon.sample_metadata")

# COMMAND ----------

# path_metadata = "amazon_metadata/partition_2"
# schema = StructType.fromJson(json_schema_metadata)
# df_partition = spark.read.format("json").schema(schema).load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_metadata}")
# display(df_partition)

# COMMAND ----------

df_test.explain(mode='cost')

# COMMAND ----------

display(df_test.groupBy("asin").count())

# COMMAND ----------

display(df_test.filter(col("year_month").isNull()))

# COMMAND ----------

print(df_test.filter(col("year_month").isNull()).count())

# COMMAND ----------

display(df_test.filter(col("asin") == "1622234642"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{path_bronce_amz_metadata}`")

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{path_bronce_amz_metadata}` ZORDER BY asin")

# COMMAND ----------

spark.sql(f"VACUUM delta.`{path_bronce_amz_metadata}` RETAIN 168 HOURS")
