from azure.eventhub import EventHubConsumerClient

# Establece los parámetros de conexión
connection_str = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_listener;SharedAccessKey=sJJnyi8GGTBAa55jY89kacoT6hXAzWx2B+AEhCPEKYE=;EntityPath=factored_datathon_amazon_review"
consumer_group = "$Default"  # Use el nombre de su grupo de consumidores aquí. Por lo general, puedes usar '$Default'.
eventhub_name = "factored_datathon_amazon_review"

# Crea el cliente del consumidor
client = EventHubConsumerClient.from_connection_string(
    connection_str, consumer_group, eventhub_name=eventhub_name
)


# Define la función callback
def on_event(partition_context, event):
    # Imprime el contenido del evento (puedes hacer lo que necesites con los datos aquí)
    print("Received event from partition: {}.".format(partition_context.partition_id))
    print("Event data: ", event.body_as_str())


# Recibe los eventos
with client:
    client.receive(on_event=on_event)
