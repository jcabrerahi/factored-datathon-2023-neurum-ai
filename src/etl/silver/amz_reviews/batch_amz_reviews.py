# Databricks notebook source
from pyspark.sql.functions import *	
import pandas as pd
import numpy as np
import json
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
# MAGIC # Metadata

# COMMAND ----------

metadata_df = (spark.read.format("delta").load(path_bronze_amz_metadata)
               .drop('parsed_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rank:**
# MAGIC * From string type to ArrayType
# MAGIC * Extract the higher rank

# COMMAND ----------

metadata_df = metadata_df.withColumn("rank", from_json(col("rank"), ArrayType(StringType())))

# COMMAND ----------

metadata_df = (metadata_df
.withColumn("rank", transform(col("rank"), lambda x: regexp_replace(x, ">#", "")))
.withColumn("rank", transform(col("rank"), lambda x: struct(regexp_extract(x, '(\d{1,3}(?:,\d{3})*)', 1).alias("number"), regexp_extract(x, 'in\s+(.+)', 1).alias("cat"))))
.withColumn("higher_rank", struct(array_min(col("rank.number")).alias("higher_rank"), concat_ws(", ", col("rank.cat")).alias("cat"))))

# COMMAND ----------

# MAGIC %md
# MAGIC **main_cat:**
# MAGIC * Extract the text in the alt
# MAGIC * replace the corrupted &
# MAGIC * Capitalize the values
# MAGIC
# MAGIC -- that allow us to reduce the number of different categories

# COMMAND ----------

metadata_df = (metadata_df
               .withColumn("main_cat", when(col("main_cat").contains("img src="), regexp_extract(col("main_cat"), r'alt=["\']([^"\']*)', 1)).otherwise(col("main_cat")))
               .withColumn("main_cat", regexp_replace(col("main_cat"), "&amp;", "&"))
               .withColumn("main_cat", initcap(col("main_cat"))))

# COMMAND ----------

# MAGIC %md
# MAGIC **first_category:**
# MAGIC * Extract only the first element of the list of categories (if does not exist then Unknown)

# COMMAND ----------

metadata_df = metadata_df.withColumn("first_category", coalesce(col("category").getItem(0), lit("Unknown")))

# COMMAND ----------

# MAGIC %md
# MAGIC **date:**
# MAGIC * Clean invalid dates and parse to a YYYY-mm-dd format

# COMMAND ----------

metadata_df = metadata_df.withColumn("date", to_date(col("date"), "MMMM dd, yyyy"))

# COMMAND ----------

# MAGIC %md
# MAGIC **details:**
# MAGIC * clean empty details

# COMMAND ----------

metadata_df = metadata_df.withColumn("details", when(col("details") == "{}", lit(None)).otherwise(col("details")))

# COMMAND ----------

# MAGIC %md
# MAGIC **features:**
# MAGIC * clean empty feature

# COMMAND ----------

metadata_df = metadata_df.withColumn("feature", array_remove(col("feature"), ""))

# COMMAND ----------

# MAGIC %md
# MAGIC **fit:**
# MAGIC * clean empty fit

# COMMAND ----------

metadata_df = metadata_df.withColumn("fit", when(col("fit") == "", lit(None)).otherwise(col("fit")))

# COMMAND ----------

# MAGIC %md
# MAGIC **description:**
# MAGIC * Extract only the text of the array

# COMMAND ----------

metadata_df = metadata_df.withColumn("description", col("description").getItem(0))

# COMMAND ----------

# MAGIC %md
# MAGIC **price:**
# MAGIC * Clean the price
# MAGIC * Parse to float

# COMMAND ----------

metadata_df = metadata_df.withColumn("price", regexp_replace(col("price"), "[^0-9\.]", "").cast("float"))

# COMMAND ----------

# MAGIC %md
# MAGIC **title:**
# MAGIC * Clean the html-like title
# MAGIC * Clean empty title

# COMMAND ----------

metadata_df = (metadata_df
        .withColumn("title", when((col("title").startswith("<")), lit(None)).otherwise(col("title")))
        .withColumn("title", when(col('title') == '', lit(None)).otherwise(col("title")))
        .withColumn("title", regexp_replace(col("title"), '" />', ''))
        .withColumn("title", regexp_replace(col("title"), "&amp;", "&")))

# COMMAND ----------

windowSpec = Window.partitionBy("asin").orderBy(col("date").desc())
metadata_df = metadata_df.withColumn("row_number", row_number().over(windowSpec)).filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

(metadata_df.dropDuplicates().write
    .format("delta")
    .mode("overwrite") 
    .partitionBy("year")
    .save(path_silver_amz_metadata))

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviews batch

# COMMAND ----------

reviews_batch_df = spark.read.format("delta").load(path_bronze_amz_reviews_batch)

# COMMAND ----------

# MAGIC %md
# MAGIC **image:**
# MAGIC * Convert to array
# MAGIC * Remove corrupted values (88)
# MAGIC * replace empty array with null

# COMMAND ----------

reviews_batch_df = (reviews_batch_df
                        .withColumn("image", from_json(col("image"), ArrayType(StringType())))
                        .withColumn("image", array_remove(col("image"), "88"))
                        .withColumn("image", when(size(col("image")) == 0, lit(None)).otherwise(col("image"))))

# COMMAND ----------

# MAGIC %md
# MAGIC **reviewer_name:**
# MAGIC * Replace & and "

# COMMAND ----------

reviews_batch_df = (reviews_batch_df
                    .withColumn("reviewer_name", regexp_replace(regexp_replace(col("reviewer_name"), "&amp;", "&"), "&quot;", "\"")))

# COMMAND ----------

# MAGIC %md
# MAGIC **date:**
# MAGIC * renamed from unix_review_time_date

# COMMAND ----------

reviews_batch_df = (reviews_batch_df.withColumnRenamed("unix_review_time_date" ,"date"))

# COMMAND ----------

# MAGIC %md
# MAGIC **keyword:**
# MAGIC * if the review_text has only 1 word, then it is set to keyword

# COMMAND ----------

reviews_batch_df = (reviews_batch_df
        .withColumn("review_word_count", size(split(col("review_text"), "[\\s\n]+")))
        .withColumn("keyword", when(col("review_word_count") == 1, col("review_text")).otherwise(lit(None))))

# COMMAND ----------

(reviews_batch_df.dropDuplicates().write
    .format("delta")
    .mode("overwrite") 
    .partitionBy("year_month")
    .save(path_silver_amz_reviews_batch))