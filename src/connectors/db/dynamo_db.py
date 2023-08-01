""" A module to interact with a AWS DynamoDB table CRUD."""
from src.connectors.db.abstract_db import AbstractStore


class DynamoDBStore(AbstractStore):
    """ A class to interact with a DynamoDB table CRUD."""

    def __init__(self, boto3_session, table_name: str):
        """ Initializes the DynamoDBStore with the specified DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table to interact with.
        """
        self.dynamodb = boto3_session.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)

    def create_item(self, item):
        """ Stores a new item in the DynamoDB table.

        Args:
            item (dict): The item to store.

        Returns:
            dict: The response from DynamoDB.
        """
        response = self.table.put_item(Item=item)
        return response

    def retrieve_item(self, key):
        """ Retrieves an item from the DynamoDB table.

        Args:
            key (dict): The key of the item to retrieve.

        Returns:
            dict: The retrieved item.
        """
        response = self.table.get_item(Key=key)
        return response.get("Item")

    def update_item(self, table_name: str, key, update_expression):
        """ Updates an item in the DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table.
            key (dict): The key of the item to update.
            update_expression (dict): A dictionary representation of the update to apply.

        Returns:
            dict: The response from DynamoDB.
        """
        expression_attribute_values = {}
        update_expression_str = "SET " + ", ".join([f"{attr} = :{attr}" for attr, value in update_expression.get("SET", {}).items()])
        for attr, value in update_expression.get("SET", {}).items():
            expression_attribute_values[f":{attr}"] = value

        response = self.table.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression=update_expression_str,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
        return response


    def delete_item(self, key):
        """ Deletes an item from the DynamoDB table.

        Args:
            key (dict): The key of the item to delete.

        Returns:
            dict: The response from DynamoDB.
        """
        response = self.table.delete_item(Key=key)
        return response
