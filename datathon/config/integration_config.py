import boto3

class AWSConfig:
    """
    A utility class to configure AWS credentials for boto3.

    Parameters:
        access_key (str): The AWS access key ID.
        secret_key (str): The AWS secret access key.

    Attributes:
        access_key (str): The AWS access key ID.
        secret_key (str): The AWS secret access key.
    """

    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def setup_aws_credentials(self):
        """
        Set up AWS credentials using the provided access key and secret key.

        This method configures the default boto3 session with the provided
        AWS access key ID and secret access key.

        Returns:
            None
        """
        boto3.setup_default_session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)