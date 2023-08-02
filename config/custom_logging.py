""" Module to set up logging and upload logs to S3. """
import logging
from functools import wraps


def setup_logging(boto3_session, log_file_name: str = "logs_batch_amz_review"):
    """ Decorator to set up logging for a function and upload logs to S3.

    Args:
        boto3_session (boto3.Session): A boto3 session object.
        log_file_name (str, optional): The name of the log file (without the
        file extension).
            Defaults to "logs_batch_amz_review".

    Returns:
        function: The decorated function with logging and log upload
        functionality.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Configure the desired logging level (e.g., INFO, WARNING, ERROR)
            logging.basicConfig(level=logging.INFO)

            # Set up the handler to save the logs to a local file
            log_file_path = f"/tmp/{log_file_name}.log"  # Local path in DBs.
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(logging.INFO)

            # Create a formatter for the log format
            log_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(log_formatter)

            # Add the handler to the logger
            logger = logging.getLogger()
            logger.addHandler(file_handler)

            try:
                logging.info("Starting process")
                # Call the original function
                result = func(*args, **kwargs)
                # Set the name of the bucket and the S3 path to save file.
                s3_bucket = "neurum-ai-factored-datathon"
                s3_file_path = f"logs/{log_file_name}.log"

                # Copy the local file to S3
                s3_client = boto3_session.resource("s3")
                s3_client.upload_file(log_file_path, s3_bucket, s3_file_path)

                return result

            except Exception as error:
                logging.error("Unexpected error: %s", error)
                raise

        return wrapper

    return decorator
