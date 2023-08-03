# Databricks notebook source
from pyspark.sql.functions import *	
import pandas as pd

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

# COMMAND ----------

reviews_df = (spark.read.format("delta").load(path_bronce_amz_reviews))
display(reviews_df)

# COMMAND ----------

display(reviews_df.groupBy("asin").agg(count("*"))) ## Not a good partition, too many partitions 
display(reviews_df.groupBy("date").agg(count("*"))) ## good partition, but too many partitions
display(reviews_df.withColumn("first_date", trunc("date", "month")).groupBy("first_date").agg(count("*"))) ## good partition
display(reviews_df.withColumn("first_date", trunc("date", "year")).groupBy("first_date").agg(count("*"))) ## good partition
display(reviews_df.groupBy("overall").agg(count("*"))) ## Not a good partition, partitions too big

# COMMAND ----------

display(reviews_df.filter(col("asin") == "B000MXQ2CU").groupBy("asin", "overall").agg(count("*")))

# COMMAND ----------

## user that make more reviews
## daytime that more reviews are posted <= not possible timestamp is only until day
## number of not verified reviews
## maybe we dont need the sentiment analysis, we have the "overall" metric
## sentiment, satisfaction, advice

# COMMAND ----------

# MAGIC %md
# MAGIC # Metadata

# COMMAND ----------

metadata_df = (spark.read.format("delta").load(path_bronze_amz_metadata)
               .drop('parsed_date'))

# COMMAND ----------

dtypes_pdf = pd.DataFrame(metadata_df.dtypes, columns = ['Column Name','Data type'])
dtypes_pdf

# COMMAND ----------

print(f'There are total {metadata_df.count()} row, Let print first 2 data rows:')
metadata_df.limit(2).toPandas()

# COMMAND ----------

array_columns = dtypes_pdf[dtypes_pdf['Data type'].str.contains(pat='array')]['Column Name'].tolist()
string_columns = dtypes_pdf[dtypes_pdf['Data type'] == 'string']['Column Name'].tolist()

# COMMAND ----------

missing_values = {} 
total_count = metadata_df.count()
for index, c in enumerate(metadata_df.columns):
    print(c)
    if c in string_columns:    # check string columns with None, Null and Empty values
        missing_count = metadata_df.filter((col(c).isNull()) | (col(c) == "{}") | (col(c) == "")).count()
        missing_values.update({c:missing_count/total_count})
    if c in array_columns:  # check empty arrays
        missing_count = metadata_df.filter(size(col(c)) == 0).count()
        missing_values.update({c:missing_count/total_count})
    print(missing_values)
missing_df = pd.DataFrame.from_dict(missing_values, orient='index', columns=['%null'])
missing_df.sort_values(by="%null", ascending=False)

# COMMAND ----------

display(metadata_df.withColumn("clean_price", regexp_replace(col("price"), "[^0-9\.]", "").cast("float")).groupBy("clean_price").count().orderBy(col("count").desc()))

# COMMAND ----------

display(metadata_df.groupBy("price").count().orderBy(col("count").desc()))

# COMMAND ----------

display(metadata_df
        .withColumn("title", when((col("title").startswith("<")), lit(None)).otherwise(col("title")))
        .withColumn("title", when(col('title') == '', lit(None)).otherwise(col("title")))
        .withColumn("title", regexp_replace(col("title"), '" />', ''))
        .withColumn("title", regexp_replace(col("title"), "&amp;", "&"))
        .groupBy("title").count().orderBy(col("count").desc()))

# COMMAND ----------

display(metadata_df.groupBy("file_source").count().orderBy(col("count").desc()))

# COMMAND ----------

display(metadata_df.groupBy("year").count().orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviews batch

# COMMAND ----------

reviews_batch_df = (spark.read.format("delta").load(path_bronze_amz_reviews_batch))

# COMMAND ----------

print('Columns overview')
pd.DataFrame(reviews_batch_df.dtypes, columns = ['Column Name','Data type'])

# COMMAND ----------

print(f'There are total {reviews_batch_df.count()} row, Let print first 2 data rows:')
reviews_batch_df.limit(2).toPandas()

# COMMAND ----------

df = (reviews_batch_df
        .withColumn("image", from_json(col("image"), ArrayType(StringType())))
        .withColumn("image", array_remove(col("image"), "88"))
        .withColumn("image", when(size(col("image")) == 0, lit(None)).otherwise(col("image"))))

# COMMAND ----------

df = (reviews_batch_df
        .withColumn("review_word_count", size(split(col("review_text"), "[\\s\n]+")))
        .withColumn("keyword", when(col("review_word_count") == 1, col("review_text")).otherwise(lit(None)))
        .withColumn("sentiment", 
                    when(((col("overall").isin([4.0, 5.0])) & (col("keyword").isNotNull())), lit("positive")).otherwise(
                    when(((col("overall").isin([0.0, 1.0, 2.0])) & (col("keyword").isNotNull())), lit("negative")).otherwise(lit(None))))
        )

# COMMAND ----------

display(reviews_batch_df.groupBy("overall").count().orderBy(col("count").desc()))

# COMMAND ----------

display(df.groupBy("keyword", "overall", "sentiment").count().orderBy(col("count").desc()))

# COMMAND ----------

display(df.filter(col("review_word_count") == 0).select("keyword").limit(10))

# COMMAND ----------

display(reviews_batch_df.groupBy("verified").count().orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviews stream

# COMMAND ----------

reviews_stream_df = (spark.read.format("delta").load(path_bronze_amz_reviews_stream))

# COMMAND ----------

print('Columns overview')
pd.DataFrame(reviews_stream_df.dtypes, columns = ['Column Name','Data type'])

# COMMAND ----------

print(f'There are total {reviews_stream_df.count()} row, Let print first 2 data rows:')
reviews_stream_df.limit(2).toPandas()

# COMMAND ----------


