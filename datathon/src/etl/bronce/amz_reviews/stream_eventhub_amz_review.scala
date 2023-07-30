// Databricks notebook source
// MAGIC %md
// MAGIC # Import libraries

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }

// COMMAND ----------

// MAGIC %md
// MAGIC # Config & credentials

// COMMAND ----------

// AWS credentials
val aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
val aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

val path_endpoint = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_4;SharedAccessKey=zkZkK6UnK6PpFAOGbgcBfnHUZsXPZpuwW+AEhEH24uc=;EntityPath=factored_datathon_amazon_reviews_4"

// == S3 config
val path_bucket = "neurum-ai-factored-datathon"
val path_bronce_amz_metadata = s"s3a://$path_bucket/bronce/amazon/stream_reviews"

val connectionString = ConnectionStringBuilder(path_endpoint)
  .build

// val eventHubsConf = EventHubsConf(connectionString)
//   .setStartingPosition(EventPosition.fromStartOfStream)
//   .setConsumerGroup("neurum_ai")

val startFromBeginning = true // true si quieres empezar desde el principio, false si quieres empezar desde el último offset
val lastOffset = retrieveOffset()  // función que recupera el último offset consumido

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(
    if (startFromBeginning) 
      EventPosition.fromStartOfStream 
    else 
      EventPosition.fromSequenceNumber(lastOffset)
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

display(df_eventhubs)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transform

// COMMAND ----------

import org.apache.spark.sql.functions._

val df_eventhubs_decoded = df_eventhubs
  .withColumn("body", col("body").cast("string"))
  .withColumn("year_month", date_format(col("enqueuedTime"), "yyyy-MM"))

// COMMAND ----------

display(df_eventhubs_decoded)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load

// COMMAND ----------

(
  df_eventhubs_decoded.writeStream.format("delta")
  .outputMode("append")
  .trigger(once=True)
  .partitionBy("year_month")
  .option("checkpointLocation", s"$path_bronce_amz_metadata/_checkpoints")
  .start(path_bronce_amz_metadata)
)

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

// COMMAND ----------


