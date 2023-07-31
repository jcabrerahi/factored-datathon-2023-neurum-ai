from abc import ABC, abstractmethod

class AbstractStore(ABC):
    """
    An abstract class that defines the interface for all storage operations.
    """

    @abstractmethod
    def create_item(self, item):
        """
        Stores a new item.

        Args:
            item (dict): The item to store.
        """
        pass

    @abstractmethod
    def retrieve_item(self, key):
        """
        Retrieves an item.

        Args:
            key (dict): The key of the item to retrieve.
        """
        pass

    @abstractmethod
    def update_item(self, key, update_expression, expression_attribute_values):
        """
        Updates an item.

        Args:
            key (dict): The key of the item to update.
            update_expression (str): A string representation of the update to apply.
            expression_attribute_values (dict): A dictionary of expression attribute values.
        """
        pass

    @abstractmethod
    def delete_item(self, key):
        """
        Deletes an item.

        Args:
            key (dict): The key of the item to delete.
        """
        pass
