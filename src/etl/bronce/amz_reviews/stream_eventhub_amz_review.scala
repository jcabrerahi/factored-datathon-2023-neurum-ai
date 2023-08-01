// Databricks notebook source
// MAGIC %md
// MAGIC # Python config
// MAGIC
// MAGIC Retrieve offset value from DynamoDB to ingest EventHub from "fromOffset" instead "StartOfStream"

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC from config.integration_config import AWSConfig
// MAGIC from src.connectors.db.dynamo_db import DynamoDBStore
// MAGIC
// MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType
// MAGIC from pyspark.sql.functions import max as pymax, min as pymin
// MAGIC
// MAGIC aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
// MAGIC aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")
// MAGIC table_name = "stream_events_position"
// MAGIC
// MAGIC aws_config = AWSConfig(aws_access_key_id=aws_access_key, aws_secret_key=aws_secret_key)
// MAGIC boto3_config = aws_config.create_boto3_session()
// MAGIC
// MAGIC def get_offset_value():
// MAGIC     dynamo_instance = DynamoDBStore(boto3_config, table_name)
// MAGIC     data_from_dynamodb = dynamo_instance.retrieve_item({"event_source": "factored_azure_eventhub"})
// MAGIC     return data_from_dynamodb["enqueuedTime"]
// MAGIC
// MAGIC spark.udf.register("getOffsetValue", get_offset_value)

// COMMAND ----------

// MAGIC %md
// MAGIC # Import libraries

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }

// COMMAND ----------

// MAGIC %md
// MAGIC # Config & credentials

// COMMAND ----------

import java.time.Instant
val dateString = "2023-07-27T23:25:19.836000Z" 
val instant = Instant.parse(dateString)

// COMMAND ----------

// AWS credentials
val aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
val aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_key)

// Azure EventHub
val path_endpoint = dbutils.secrets.get(scope="azure_credentials", key="event_hub.endpoint")

// S3 config
val path_bucket = "neurum-ai-factored-datathon"
val path_bronce_amz_stream_reviews = s"s3a://$path_bucket/bronce/amazon/stream_reviews"

val startFromBeginning = false
val offsetValue: String = spark.sql("SELECT getOffsetValue() as offsetValue").collect()(0).getAs[String]("offsetValue")

// ========= CONFIG
val connectionString = ConnectionStringBuilder(path_endpoint)
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(
    if (startFromBeginning)
      EventPosition.fromStartOfStream
    else
      // EventPosition.fromOffset("90720")
      EventPosition.fromEnqueuedTime(instant)
  )
  .setConsumerGroup("neurum_ai")


// COMMAND ----------

// MAGIC %md
// MAGIC # ETL

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extract

// COMMAND ----------

val df_eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transform

// COMMAND ----------

import org.apache.spark.sql.functions._

val df_eventhubs_decoded = df_eventhubs
  .withColumn("body", col("body").cast("string"))
  .withColumn("offset", col("offset").cast("long"))
  .withColumn("date_utc", date_format(col("enqueuedTime"), "yyyy-MM-dd"))

// COMMAND ----------

import org.apache.spark.util.SizeEstimator
val dfSize = SizeEstimator.estimate(df_eventhubs_decoded)
println(s"Estimated size of the dataFrame weatherDF = ${dfSize/1000000} mb")

// COMMAND ----------

display(df_eventhubs_decoded)

// COMMAND ----------

print(df_eventhubs_decoded.count())

// COMMAND ----------

// MAGIC %python
// MAGIC path_bucket = "neurum-ai-factored-datathon"
// MAGIC path_bronce_amz_stream_reviews = f"s3a://{path_bucket}/bronce/amazon/stream_reviews"
// MAGIC
// MAGIC df_eventhubs_decoded = spark.read.format("delta").load(path_bronce_amz_stream_reviews)
// MAGIC max_enqueued_time = df_eventhubs_decoded.agg(pymin("enqueuedTime")).collect()[0][0]
// MAGIC print(max_enqueued_time)
// MAGIC
// MAGIC display(df_eventhubs_decoded.groupBy("enqueuedTime").count())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load

// COMMAND ----------

df_eventhubs_decoded.writeStream.format("delta").mode("overwrite").save(path_bronce_amz_stream_reviews)

// COMMAND ----------

df_eventhubs_decoded.writeStream.format("delta")
  .outputMode("append")
  // .trigger()
  .partitionBy("date_utc")
  .option("checkpointLocation", s"$path_bronce_amz_stream_reviews/_checkpoints")
  .start(path_bronce_amz_stream_reviews)

// COMMAND ----------

// MAGIC %md
// MAGIC # Data quality

// COMMAND ----------

// # 55933 per partition
// df_test = spark.read.format("delta").load(path_bronce_amz_reviews)
// # print(df_test.count())
// display(df_test.groupBy("file_source").count())

// COMMAND ----------

// MAGIC %md
// MAGIC # Optimize
