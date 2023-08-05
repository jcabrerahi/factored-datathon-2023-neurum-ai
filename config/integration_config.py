""" This module contains the necessary configuration to connect to AWS boto3.

It includes an AWSConfig class that allows configuring AWS credentials for a
boto3 session. This configuration is essential for interacting with any AWS
service.
"""

import boto3


class AWSConfig:
    """ A utility class to configure AWS credentials for boto3.

    Parameters:
        access_key (str): The AWS access key ID.
        secret_key (str): The AWS secret access key.

    Attributes:
        access_key (str): The AWS access key ID.
        secret_key (str): The AWS secret access key.
    """

    def __init__(self, aws_access_key_id: str, aws_secret_key: str, region_name: str = "us-east-1"):
        """ Initialize AWSConfig with the provided credentials and region.

        Args:
            aws_access_key_id (str): AWS access key ID.
            aws_secret_key (str): AWS secret access key.
            region_name (str): AWS region name to be used for the session.
        """
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_key = aws_secret_key
        self._region_name = region_name

    def create_boto3_session(self):
        """ Create and return a boto3 session using AWS credentials and region.

        Returns:
            boto3.Session: A boto3 session object.
        """
        return boto3.Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_key,
            region_name=self._region_name,
        )
