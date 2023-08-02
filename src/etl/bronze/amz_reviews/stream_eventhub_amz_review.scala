// Databricks notebook source
// MAGIC %md
// MAGIC # Summary
// MAGIC
// MAGIC This notebook read the stream amazon reviews from Azure eventHub to process and save as a Bronze delta table in our pipeline
// MAGIC
// MAGIC __Details:__
// MAGIC - We use Scala in this notebook to take advantage of library: "org.apache.spark.eventhubs".

// COMMAND ----------

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
// MAGIC def get_enqueued_time_value():
// MAGIC     dynamo_instance = DynamoDBStore(boto3_config, table_name)
// MAGIC     data_from_dynamodb = dynamo_instance.retrieve_item({"event_source": "factored_azure_eventhub"})
// MAGIC     return data_from_dynamodb["enqueuedTime"]
// MAGIC
// MAGIC spark.udf.register("get_enqueued_time_value", get_enqueued_time_value)

// COMMAND ----------

// MAGIC %md
// MAGIC # Import libraries

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split, date_format, col }
import org.apache.spark.sql.streaming.Trigger
import java.time.{Instant, LocalDateTime, ZoneOffset, ZoneId}
import java.time.format.DateTimeFormatter

// COMMAND ----------

// MAGIC %md
// MAGIC # Config & credentials

// COMMAND ----------

def format_time(date_string: String): Instant = {
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
  val localDateTime = LocalDateTime.parse(date_string, formatter)
  val zonedDateTime = localDateTime.atZone(ZoneId.of("UTC"))
  zonedDateTime.toInstant
}

// COMMAND ----------

// ======================== Widgets and init values
dbutils.widgets.dropdown("start_from_beginnig", "true", Seq("true", "false"))
dbutils.widgets.text("beginnig_time", "") // 2023-07-30 00:00:21.274000

val startFromBeginning = dbutils.widgets.get("start_from_beginnig").toBoolean
val beginnigTimeManual: String = dbutils.widgets.get("beginnig_time").trim()

val beginnigTimeDynamo: String = spark.sql("SELECT get_enqueued_time_value() as positionValue").collect()(0).getAs[String]("positionValue")
val parsedBegginingTimeManual: Instant = if (!beginnigTimeManual.isEmpty) format_time(beginnigTimeManual) else Instant.MIN
val parsedBegginingTime = format_time(beginnigTimeDynamo)
val beginingTime = if (beginnigTimeManual.isEmpty) parsedBegginingTime else parsedBegginingTimeManual

// COMMAND ----------

// ======================== Third parties credentials and configuration
// AWS credentials
val aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
val aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_key)

// Azure EventHub
val path_endpoint = dbutils.secrets.get(scope="azure_credentials", key="event_hub.endpoint")

// S3 config
val path_bucket = "neurum-ai-factored-datathon"
val path_bronze_amz_stream_reviews = s"s3a://$path_bucket/bronce/amazon/stream_reviews"

// ======================== Session configuration
val connectionString = ConnectionStringBuilder(path_endpoint)
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(
    if (startFromBeginning)
      EventPosition.fromStartOfStream
    else
      EventPosition.fromEnqueuedTime(beginingTime)
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

val df_eventhubs_decoded = df_eventhubs
  .withColumn("body", col("body").cast("string"))
  .withColumn("offset", col("offset").cast("long"))
  .withColumn("date_utc", date_format(col("enqueuedTime"), "yyyy-MM-dd"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load

// COMMAND ----------

df_eventhubs_decoded.writeStream
  .format("delta")
  .outputMode("append")
  .trigger(Trigger.AvailableNow)
  .partitionBy("date_utc")
  .option("checkpointLocation", s"$path_bronze_amz_stream_reviews/_checkpoints")
  .option("maxOffset", 120000) // Max time 5 minutes
  .start(path_bronze_amz_stream_reviews)

// COMMAND ----------

// MAGIC %md
// MAGIC # Data quality

// COMMAND ----------

// # 55933 per partition
// df_test = spark.read.format("delta").load(path_bronze_amz_reviews)
// # print(df_test.count())
// display(df_test.groupBy("file_source").count())

// COMMAND ----------

// MAGIC %md
// MAGIC # Optimize

// COMMAND ----------

spark.sql(s"OPTIMIZE delta.`$path_bronze_amz_stream_reviews`")

// COMMAND ----------

spark.sql(s"VACUUM delta.`$path_bronze_amz_stream_reviews` RETAIN 168 HOURS")
