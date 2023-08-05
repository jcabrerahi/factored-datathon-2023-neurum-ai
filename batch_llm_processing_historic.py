# Databricks notebook source
# !pip install openai ratelimit backoff

# COMMAND ----------

import logging
import time
from typing import List

from config.integration_config import AWSConfig
from config.custom_logging import setup_logging

import torch
from transformers import pipeline as tpipeline
from pyspark.sql. functions import collect_list, concat_ws, col, count, sum as pysum, pandas_udf, lit
from pyspark.sql.types import StringType
import pandas as pd

# COMMAND ----------

# AWS credentials
aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

aws_config = AWSConfig(aws_access_key_id=aws_access_key, aws_secret_key=aws_secret_key)
boto3_config = aws_config.create_boto3_session()

# == S3 config
path_data_bucket = "neurum-ai-factored-datathon"
# path_silver_amz_reviews = f"s3a://{path_data_bucket}/silver/amazon/reviews"
path_gold_amz_reviews = f"s3a://{path_data_bucket}/gold/amazon/reviews"
path_gold_amz_reviews_output = f"s3a://{path_data_bucket}/gold/amazon/llm_reviews"

path_llm_amz_reviews = f"s3a://{path_data_bucket}/gold/amazon/llm_reviews_enrichment"

path_llm_models_bucket = "trird-party-llm-factored"
path_llama_model = f"s3a://{path_llm_models_bucket}"
folder_llama_model = f"s3a://{path_llm_models_bucket}/llama_model/"
folder_llama_tokenizer = f"s3a://{path_llm_models_bucket}/llama_tokenizer/"

# == Mount S3 and Databricks
db_mount_name = "/dbfs/mnt/llama_model"
# dbutils.fs.mounts()
# dbutils.fs.mount(path_llama_model, db_mount_name[5:])
# dbutils.fs.unmount(db_mount_name[5:])

# COMMAND ----------

df_silver_reviews_2 = spark.read.format("delta").load(path_gold_amz_reviews).filter(col("year_month") >= "2018-10")
print(df_silver_reviews_2.count()) # Total 144110113

# COMMAND ----------

df_silver_reviews = spark.read.format("delta").load(path_gold_amz_reviews).filter(col("year_month") >= "2018-10").filter(col("review_text").isNotNull()).filter(col("asin") == "0425255689")
print(df_silver_reviews.count())  # 139M
# display(df_silver_reviews)

# COMMAND ----------

# summarizer_broadcast = tpipeline("sentiment-analysis")
# summarizer_broadcast_2 = sc.broadcast(summarizer_broadcast)

# COMMAND ----------

# def get_sentiment(): 
#     global _sentiment 
#     if '_sentiment' not in globals():
#         # _sentiment = tpipeline("sumarization", model="t5-small", tokenizer="t5-small", num_beams=10, min_new_tokens=50)
#         _sentiment = tpipeline("sentiment-analysis")
#     return _sentiment

# @pandas_udf('string')
# def summarize_review(reviews: pd.Series) -> pd.Series:
#     sentiment = get_sentiment()
#     summaries = sentiment(reviews.tolist(), batch_size=8, truncation=True)
#     return pd.Series([summary['label'] for summary in summaries])

# review_by_product_df = df_silver_reviews.withColumn("sentiment", summarize_review(df_silver_reviews["review_text"]))

# COMMAND ----------

# display(review_by_product_df)

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

sentiment_analyzer = tpipeline("sentiment-analysis")

# Definir una función que utiliza un modelo de análisis de sentimientos de Hugging Face
def analyze_sentiment(text_series):
    results = sentiment_analyzer(text_series.tolist(), batch_size=1, truncation=True)
    sentiments = [result['label'].capitalize() for result in results]
    return pd.Series(sentiments)

# Definir la UDF de Pandas usando la función anterior
analyze_sentiment_udf = pandas_udf(analyze_sentiment, returnType=StringType())

# Supongamos que tienes un DataFrame de Spark llamado 'df' con una columna de texto llamada 'review'
# Aplicar la UDF a esa columna y añadir los resultados como una nueva columna
result_df = (df_silver_reviews
             .withColumn("sentiment", analyze_sentiment_udf(df_silver_reviews["review_text"]))
             .withColumn("actionable", lit(None).cast(StringType()))
             .withColumn("keywords", lit(None).cast(StringType()))
             .withColumn("response", lit(None).cast(StringType()))
             )

# COMMAND ----------

# display(result_df)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save(path_gold_amz_reviews_output)

# COMMAND ----------


