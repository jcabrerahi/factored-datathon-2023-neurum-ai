// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
 
// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.

val path_endpoint = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_listener;SharedAccessKey=sJJnyi8GGTBAa55jY89kacoT6hXAzWx2B+AEhCPEKYE=;EntityPath=factored_datathon_amazon_review"

val connectionString = ConnectionStringBuilder(path_endpoint)
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)


// COMMAND ----------

val df_eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

display(df_eventhubs)
