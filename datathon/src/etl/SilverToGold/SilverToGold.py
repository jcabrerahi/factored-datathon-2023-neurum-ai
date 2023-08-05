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

path_silver_amz_reviews_batch = f"s3a://{path_bucket}/silver/amazon/reviews"
path_silver_amz_reviews_stream = f"s3a://{path_bucket}/silver/amazon/stream_reviews"
path_silver_amz_metadata = f"s3a://{path_bucket}/silver/amazon/metadata"

path_gold_amz_reviews = f"s3a://{path_bucket}/gold/amazon/reviews"

# COMMAND ----------

reviews_columns = ["asin", "image", "overall", "review_text", "reviewer_id", "reviewer_name", "summary", "verified", "vote", "date"]
metadata_columns = ["asin", "also_buy", "brand", "first_category", "description", "image", "main_cat", "price", "higher_rank", "title"]

# COMMAND ----------

metadata_df = spark.read.format("delta").load(path_silver_amz_metadata).select(metadata_columns).withColumnRenamed("image", "image_met")
reviews_batch_df = spark.read.format("delta").load(path_silver_amz_reviews_batch).select(reviews_columns)
reviews_stream_df = spark.read.format("delta").load(path_silver_amz_reviews_stream).withColumn("date", to_date("date_utc")).select(reviews_columns)

# COMMAND ----------

reviews_df = (reviews_batch_df.union(reviews_stream_df)
                    .withColumn("year_month", trunc("date", "month"))
                    .withColumn("possible_sentiment", initcap(col("possible_sentiment"))))

# COMMAND ----------

join_df = reviews_df.join(metadata_df, on="asin", how="left")

# COMMAND ----------

overall_brand_df = join_df.groupBy("brand").agg(avg("overall").alias("overall_brand"))
join_df = (join_df.join(overall_brand_df, on="brand", how="left")
           .withColumn("brand_deviation", col("overall") - col("overall_brand"))
           .withColumn("brand_deviation_text", when(col("brand_deviation") > 0, lit("better")).otherwise(
                                               when(col("brand_deviation") < 0, lit("worse")).otherwise(lit("normal")))))

# COMMAND ----------

overall_reviewer_df = join_df.groupBy("reviewer_id").agg(avg("overall").alias("overall_reviewer"))
join_df = (join_df.join(overall_reviewer_df, on="reviewer_id", how="left")
           .withColumn("reviewer_deviation", col("overall") - col("overall_reviewer"))
           .withColumn("reviewer_deviation_text", when(col("reviewer_deviation") > 0, lit("better")).otherwise(
                                               when(col("reviewer_deviation") < 0, lit("worse")).otherwise(lit("normal")))))

# COMMAND ----------

join_columns = ["asin", "image", "overall", "review_text", "reviewer_id", "reviewer_name", "summary", "verified", "vote", "date", "also_buy", "brand", "first_category", "description", "image_met", "main_cat", "price", "higher_rank", "title", "brand_deviation_text", "reviewer_deviation_text", "year_month"]

# COMMAND ----------

(join_df.dropDuplicates().select(join_columns).write
    .format("delta")
    .mode("overwrite") 
    .option("mergeSchema", "true")
    .partitionBy("year_month")
    .save(path_gold_amz_reviews))
