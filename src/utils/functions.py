""" Module with pure python functions to feed pyspark transformations."""

import re

import unidecode


class CustomFunctions:
    """ A collection of custom utility functions."""
    @staticmethod
    def to_snakecase(word: str) -> str:
        """to_snakecase convert any string to snakecase format
        Args:
            word: str input
        Returns:
            str in snakecase format
        """
        print(f"Original: '{word}'")
        word = re.sub('([a-z0-9])([A-Z])', r'\1_\2', word)
        word = word.lower()
        word = re.sub(r"\s+", "_", word)  # Aquí cambiamos los espacios por guiones bajos.
        word = re.sub(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ_]+", "_", word)
        word = word.replace("__", "_")
        word = unidecode.unidecode(word).strip("_")
        print(f"Result: '{word}'")
        return word
