""" A module to abstract class AbstractStore."""

from abc import ABC, abstractmethod


class AbstractStore(ABC):
    """ An abstract class that defines the interface for all storage ops."""

    @abstractmethod
    def create_item(self, item):
        """ Stores a new item.

        Args:
            item (dict): The item to store.
        """

    @abstractmethod
    def retrieve_item(self, key):
        """ Retrieves an item.

        Args:
            key (dict): The key of the item to retrieve.
        """

    @abstractmethod
    def update_item(self, key, update_expression):
        """ Updates an item.

        Args:
            key (dict): The key of the item to update.
            update_expression (str): A string representation of the update to
            apply.
        """

    @abstractmethod
    def delete_item(self, key):
        """ Deletes an item.

        Args:
            key (dict): The key of the item to delete.
        """
