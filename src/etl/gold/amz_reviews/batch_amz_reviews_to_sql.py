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

path_gold_amz_reviews = f"s3a://{path_bucket}/gold/amazon/reviews"

# COMMAND ----------

gold_reviews_df = spark.read.format("delta").load(path_gold_amz_reviews)

# COMMAND ----------

display(gold_reviews_df.limit(10))

# COMMAND ----------

gold_reviews_df.write.format("delta").saveAsTable("test_db.amazon.gold_reviews")