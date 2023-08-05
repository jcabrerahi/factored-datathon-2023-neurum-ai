# Databricks notebook source
from pyspark.sql.functions import *	
import pandas as pd
import numpy as np
import json

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
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# == S3 config
path_bucket = "neurum-ai-factored-datathon"
path_bronze_amz_reviews_batch = f"s3a://{path_bucket}/bronce/amazon/reviews"
path_bronze_amz_reviews_stream = f"s3a://{path_bucket}/bronce/amazon/stream_reviews"
path_bronze_amz_metadata = f"s3a://{path_bucket}/bronce/amazon/metadata"

path_silver_amz_reviews_batch = f"s3a://{path_bucket}/silver/amazon/reviews"
path_silver_amz_reviews_stream = f"s3a://{path_bucket}/silver/amazon/stream_reviews"
path_silver_amz_metadata = f"s3a://{path_bucket}/silver/amazon/metadata"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviews stream

# COMMAND ----------

reviews_stream_df = spark.readStream.format("delta").load(path_bronze_amz_reviews_stream)

# COMMAND ----------

json_schema_reviews = {
    "fields":[{"metadata":{},"name":"asin","nullable":True,"type":"string"},
              {"metadata":{},"name":"image","nullable":True,"type":"string"},
              {"metadata":{},"name":"overall","nullable":True,"type":"string"},
              {"metadata":{},"name":"reviewText","nullable":True,"type":"string"},
              {"metadata":{},"name":"reviewerID","nullable":True,"type":"string"},
              {"metadata":{},"name":"reviewerName","nullable":True,"type":"string"},
              {"metadata":{},"name":"style","nullable":True,"type":"string"},
              {"metadata":{},"name":"summary","nullable":True,"type":"string"},
              {"metadata":{},"name":"unix_review_time","nullable":True,"type":"string"},
              {"metadata":{},"name":"verified","nullable":True,"type":"string"},
              {"metadata":{},"name":"vote","nullable":True,"type":"string"},
              {"metadata":{},"name":"file_source","nullable":True,"type":"string"}],
    "type":"struct"
}

# COMMAND ----------

schema = StructType.fromJson(json_schema_reviews)
schema

# COMMAND ----------

reviews_stream_df = reviews_stream_df.withColumn("parsed_body" ,from_json(col("body"), schema))

# COMMAND ----------

for variable in json_schema_reviews['fields']:
    name = variable["name"]
    reviews_stream_df = reviews_stream_df.withColumn(name, col(f"parsed_body.{name}"))
reviews_stream_df = reviews_stream_df.drop("parsed_body")
reviews_stream_df = reviews_stream_df.drop("body")

# COMMAND ----------

reviews_stream_df = (reviews_stream_df
                     .withColumnRenamed("reviewText", "review_text")
                     .withColumnRenamed("reviewerID", "reviewer_id")
                     .withColumnRenamed("reviewerName", "reviewer_name"))

# COMMAND ----------

reviews_stream_df = (reviews_stream_df
                        .withColumn("image", from_json(col("image"), ArrayType(StringType())))
                        .withColumn("image", array_remove(col("image"), "88"))
                        .withColumn("image", when(size(col("image")) == 0, lit(None)).otherwise(col("image"))))

# COMMAND ----------

reviews_stream_df = (reviews_stream_df
                    .withColumn("reviewer_name", regexp_replace(regexp_replace(col("reviewer_name"), "&amp;", "&"), "&quot;", "\"")))

# COMMAND ----------

reviews_stream_df = (reviews_stream_df
        .withColumn("review_word_count", size(split(col("review_text"), "[\\s\n]+")))
        .withColumn("keyword", when(col("review_word_count") == 1, col("review_text")).otherwise(lit(None))))

# COMMAND ----------

# display(reviews_stream_df)

# COMMAND ----------

(reviews_stream_df.writeStream
 .format("delta")
 .outputMode("append")
 .trigger(processingTime='30 seconds')
 .partitionBy("date_utc")
 .option("checkpointLocation", f"{path_silver_amz_reviews_stream}/_checkpoints")
 .start(path_silver_amz_reviews_stream))

# COMMAND ----------

