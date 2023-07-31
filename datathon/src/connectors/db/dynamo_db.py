import boto3

class DynamoDBStore:
    """
    A class to interact with a DynamoDB table for storing and retrieving values.
    """

    def __init__(self, table_name):
        """
        Initializes the DynamoDBStore with the specified DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table to interact with.
        """
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def create_item(self, item):
        """
        Stores a new item in the DynamoDB table.

        Args:
            item (dict): The item to store.

        Returns:
            dict: The response from DynamoDB.
        """
        response = self.table.put_item(Item=item)
        return response

    def retrieve_item(self, key):
        """
        Retrieves an item from the DynamoDB table.

        Args:
            key (dict): The key of the item to retrieve.

        Returns:
            dict: The retrieved item.
        """
        response = self.table.get_item(Key=key)
        return response.get('Item')

    def update_item(self, key, update_expression, expression_attribute_values):
        """
        Updates an item in the DynamoDB table.

        Args:
            key (dict): The key of the item to update.
            update_expression (str): A string representation of the update to apply.
            expression_attribute_values (dict): A dictionary of expression attribute values.

        Returns:
            dict: The response from DynamoDB.
        """
        response = self.table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        return response

    def delete_item(self, key):
        """
        Deletes an item from the DynamoDB table.

        Args:
            key (dict): The key of the item to delete.

        Returns:
            dict: The response from DynamoDB.
        """
        response = self.table.delete_item(Key=key)
        return response
