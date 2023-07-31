import boto3

# Crear una sesión con el perfil 'personal'
session = boto3.Session(profile_name="personal")

# Crea un recurso DynamoDB utilizando las credenciales y la configuración de la región de los archivos de configuración de AWS CLI
dynamodb = session.resource("dynamodb")

# Accede a tu tabla
table = dynamodb.Table("azure_eventhub_offset")

# Obten un artículo de la tabla
response = table.get_item(Key={"id": 0})

# Imprime el artículo
print(response.get("Item"))
