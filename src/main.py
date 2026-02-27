# Libs
import os
from typing import List
import boto3
from dotenv import load_dotenv
from loguru import logger

# Load .env file
load_dotenv()

# Configurations
AWS_ACCESS_KEY_ID: str = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY: str = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION: str = os.getenv('AWS_REGION')
BUCKET_NAME: str = os.getenv('BUCKET_NAME')
PASTA: str = os.getenv('PASTA')

logger.add("logs/file_upload.log", rotation="1 MB")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME, PASTA]):
    raise ValueError("Missing required environment variables.")

# Boto3 S3 Client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    logger.info("S3 client created successfully.")
except Exception as e:
    logger.error(f"Error creating S3 client: {e}")
    raise

# Functions
def list_objects(directory: str) -> List[str]:
    """List all objects in a specified directory from my computer"""
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list
    
def upload_files_to_s3(objects: List[str]) -> None:
    """Upload a list of files to the specified S3 bucket"""
    for file_path in objects:
        s3_key = os.path.relpath(file_path, start=os.path.dirname(objects[0]))
        try:
            logger.info(f"Uploading {file_path} to s3://{BUCKET_NAME}/{s3_key}")
            s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
            logger.success(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload {file_path}: {e}")
            raise

def delete_local_files(objects: List[str]) -> None:
    """Delete local files after upload"""
    for file_path in objects:
        try:
            #os.remove(file_path)
            logger.info(f"Deleted local file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")
            raise

def execute(directory: str) -> None:
    """Main execution function"""
    try:
        logger.info("Starting upload process...")
        files_to_upload = list_objects(directory)
        if files_to_upload:
            upload_files_to_s3(files_to_upload)
            delete_local_files(files_to_upload)
        else:
            logger.info("No files found to upload.")
        logger.success("Upload process completed.")
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise

if __name__ == "__main__":
    # Directory to upload
    directory_to_upload = PASTA
    try:
        execute(directory_to_upload)
    except Exception as e:
        logger.error(f"Error in main: {e}")