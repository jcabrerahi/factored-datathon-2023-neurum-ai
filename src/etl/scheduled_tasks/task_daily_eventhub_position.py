# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Summary
# MAGIC
# MAGIC This task update the position's values for differente stream sources, in this case is updateing the position to EventHub stream source.

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from config.integration_config import AWSConfig
from src.connectors.db.dynamo_db import DynamoDBStore

from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType
from pyspark.sql.functions import max as pymax, min as pymin
import datetime

aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

# COMMAND ----------

# MAGIC %md
# MAGIC # Config & credentials

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

table_name = "stream_events_position"

path_bucket = "neurum-ai-factored-datathon"
path_bronze_amz_stream_reviews = f"s3a://{path_bucket}/bronze/amazon/stream_reviews"

# COMMAND ----------

# MAGIC %md
# MAGIC # Custom functions

# COMMAND ----------

def get_max_values_from_delta_table(spark, table_path):
    """ Get the maximum values for sequenceNumber, offset, and enqueuedTime from a Delta table.

    Args:
        spark (SparkSession): The Spark session.
        table_path (str): The path to the Delta table.

    Returns:
        dict: A dictionary containing the maximum values for sequenceNumber, offset, and enqueuedTime.
    """
    df_amz_stream_reviews = spark.read.format("delta").load(table_path)
    df_max_values = df_amz_stream_reviews.agg(
        pymax("sequenceNumber").alias("sequenceNumber"),
        pymax("offset").alias("offset"),
        pymax("enqueuedTime").alias("enqueuedTime")
    )
    max_values = df_max_values.collect()[0].asDict()
    return max_values

# COMMAND ----------

# MAGIC %md
# MAGIC # Task

# COMMAND ----------

max_values = get_max_values_from_delta_table(spark, path_bronze_amz_stream_reviews)
print(max_values)
sequence_number = max_values["sequenceNumber"]
offset = max_values["offset"]
enqueued_time = max_values["enqueuedTime"]

aws_config = AWSConfig(aws_access_key_id=aws_access_key, aws_secret_key=aws_secret_key)
boto3_config = aws_config.create_boto3_session()
dynamo_instance = DynamoDBStore(boto3_config, table_name)

table_name = 'stream_events_position'
item_key = {"event_source": "factored_azure_eventhub"}

# Define parameters to update
update_expression = {
    "SET": {
        "enqueuedTime": str(enqueued_time),
        "LastModification": str(datetime.datetime.now().isoformat()),
        "maxOffset": offset,
        "sequenceNumber": sequence_number
    }
}

# Update dynamoDB table with new values
response = dynamo_instance.update_item(table_name, item_key, update_expression)
