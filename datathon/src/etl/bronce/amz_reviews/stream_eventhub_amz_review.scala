// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
 
// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.

// val path_endpoint = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_listener;SharedAccessKey=sJJnyi8GGTBAa55jY89kacoT6hXAzWx2B+AEhCPEKYE=;EntityPath=factored_datathon_amazon_review"
val path_endpoint = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_4;SharedAccessKey=zkZkK6UnK6PpFAOGbgcBfnHUZsXPZpuwW+AEhEH24uc=;EntityPath=factored_datathon_amazon_reviews_4"

val connectionString = ConnectionStringBuilder(path_endpoint)
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromStartOfStream)
  .setConsumerGroup("neurum_ai")


// COMMAND ----------

val df_eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

display(df_eventhubs)

// COMMAND ----------

import org.apache.spark.sql.functions._

val df_eventhubs_decoded = df_eventhubs.withColumn("body", col("body").cast("string"))

// COMMAND ----------

display(df_eventhubs_decoded)

// COMMAND ----------


