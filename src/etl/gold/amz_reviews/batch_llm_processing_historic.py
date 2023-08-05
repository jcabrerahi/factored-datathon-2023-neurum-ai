# Databricks notebook source
# MAGIC %md
# MAGIC # Use of LLM
# MAGIC
# MAGIC Advances in Natural Language Processing (NLP) have unlocked unprecedented opportunities for businesses to get value out of their text data. Natural Language Processing can be used for a wide range of applications, in this case we use to get the sentiment, actionable from seller, summary into 3 keywords and response to the review.
# MAGIC
# MAGIC We tried with Llama2-7b and dolly2-3b, but, for compute capacity and cost per hour of these models we decide to change the model and reduce to BERT model distilled versions: **distilbert-base-uncased-finetuned-sst-2-english**.

# COMMAND ----------

import logging
import time
from typing import List

from config.integration_config import AWSConfig
from config.custom_logging import setup_logging

import torch
from transformers import pipeline as hf_pipeline
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

df_gold_reviews = spark.read.format("delta").load(path_gold_amz_reviews)
print(df_gold_reviews.count()) # Total 144110113

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC # LLM

# COMMAND ----------

sentiment_analyzer = hf_pipeline("sentiment-analysis")

def analyze_sentiment(text_series):
    results = sentiment_analyzer(text_series.tolist(), batch_size=1, truncation=True)
    sentiments = [result['label'].capitalize() for result in results]
    return pd.Series(sentiments)

analyze_sentiment_udf = pandas_udf(analyze_sentiment, returnType=StringType())

result_df = (df_silver_reviews
             .withColumn("sentiment", analyze_sentiment_udf(df_silver_reviews["review_text"]))
             .withColumn("actionable", lit(None).cast(StringType()))
             .withColumnRename("keywords", "keyword")
             .withColumn("response", lit(None).cast(StringType()))
             )

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("date").save(path_gold_amz_reviews_output)
