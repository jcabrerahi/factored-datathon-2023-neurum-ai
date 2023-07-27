from functools import wraps
import logging
import boto3


def setup_logging(log_file_name: str = "logs_batch_amz_review"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Configure the desired logging level (e.g., INFO, WARNING, ERROR)
            logging.basicConfig(level=logging.INFO)

            # Set up the handler to save the logs to a local file
            log_file_path = f"/tmp/{log_file_name}.log"  # Local file path in Databricks
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(logging.INFO)

            # Create a formatter for the log format
            log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(log_formatter)

            # Add the handler to the logger
            logger = logging.getLogger()
            logger.addHandler(file_handler)

            try:
                logging.info("Starting process")
                # Call the original function
                result = func(*args, **kwargs)
                logging.info("Process completed")
                # Set the name of the bucket and the S3 path where to save the file
                s3_bucket = 'neurum-ai-factored-datathon'
                s3_file_path = f'logs/{log_file_name}.log'

                # Copy the local file to S3
                s3_client = boto3.client('s3')
                s3_client.upload_file(log_file_path, s3_bucket, s3_file_path)

                return result
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                raise

        return wrapper

    return decorator
