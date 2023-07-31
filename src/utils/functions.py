""" Module with pure python functions to feed pyspark transformations."""

import re

import unidecode


class CustomFunctions:
    """
    A collection of custom utility functions.
    """
    @staticmethod
    def to_snakecase(word: str) -> str:
        """to_snakecase convert any string to snakecase format
        Args:
            word: str input
        Returns:
            str in snakecase format
        """
        word = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", word)
        word = (
            re.sub("([a-z0-9])([A-Z])", r"\1_\2", word)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("\r", "")
            .replace("__", "_")
        )
        word = re.sub("([^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ: ])", "_", word)
        return unidecode.unidecode(word)
