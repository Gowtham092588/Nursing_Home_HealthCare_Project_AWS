import os
import boto3
import json
import logging
import time
import traceback

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Config

AWS_REGION = "us-east-2"
S3_DATA_BUCKET = "healthcare-proj-data-bucket"
S3_OUTPUT_PREFIX = "raw/"


S3_ERROR_BUCKET = "healthcare-proj-error-bucket"
S3_ERROR_PREFIX = "errors/"

GOOGLE_SECRET_NAME = "healthcare/google-drive-credentials"

# logging info to capture logs automatically

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# connecting to AWS s3


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

# Reading the Google Credentials


def read_google_credentials():

    try:
        logger.info("Reading Google credentials from Secrets Manager")

        secret_client = boto3.client("secretsmanager", region_name=AWS_REGION)

        response = secret_client.get_secret_value(SecretId=GOOGLE_SECRET_NAME)

        secret = json.loads(response["SecretString"])

        logger.info("Successfully retrieved Google credentials")

        return secret

    except Exception as e:

        logger.error(
            f"Failed to retrieve secret: {e}",
            exc_info=True
        )

        raise

# Authenticate the Credentials


def authenticate_credentials(secret):

    try:
        logger.info("Accessing Google Drive using credentials")

        credentials = service_account.Credentials.from_service_account_info(
            secret, scopes=["https://www.googleapis.com/auth/drive.readonly"])

        service = build("drive", "v3", credentials=credentials)

        logger.info("Google Drive authentication successful")

        return service

    except Exception as e:

        logger.error(
            "Google Drive authentication failed",
            exc_info=True
        )

        raise

# Listing the files


def list_files(service):

    try:
        logger.info("Listing the files inside the google drive")

        results = service.files().list(q="mimeType='text/csv'",  pageSize=100,
                                       fields="files(id,name,mimeType)").execute()

        files = results.get("files", [])

        return files

    except Exception as e:

        logger.error(
            "Failed to list Google Drive files",
            exc_info=True
        )

        raise

# Downloading the files


def download_files(service, file_id, file_name):

    try:
        logger.info(f"Downloading {file_name}")

        file_path = f"/tmp/{file_name}"

        request = service.files().get_media(
            fileId=file_id
        )

        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(
                f, request, chunksize=50*1024*1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()

                if status:
                    logger.info(
                        f"Download progress: {int(status.progress()*100)}%")

            logger.info(f"Download completed: {file_name}")

        return file_path

    except Exception:

        logger.error(
            f"Failed downloading {file_name}",
            exc_info=True
        )

        raise


def upload_files_s3(s3_client, file_path, file_name):

    try:

        s3_key = f"{S3_OUTPUT_PREFIX}{file_name}"

        logger.info(
            f"Uploading {file_name} to S3 Data Bucket"
        )

        s3_client.upload_file(
            file_path,
            S3_DATA_BUCKET,
            s3_key
        )

        logger.info(
            f"Successfully uploaded {file_name} to s3://{S3_DATA_BUCKET}/{s3_key}"
        )

    except Exception as e:

        logger.error(
            f"Failed uploading {file_name} to S3",
            exc_info=True
        )

        raise


def error_handler(s3_client, file_path, file_name, stage, error):

    try:

        logger.error(f"{stage} failed for {file_name}", exc_info=True)

        if file_path and os.path.exists(file_path):

            error_key = f"{S3_ERROR_PREFIX}{stage}{file_name}"

            logger.info(f"Uploading {file_name} to S3 Error Bucket")

            s3_client.upload_file(file_path, S3_ERROR_BUCKET, error_key)

            logger.info(f"Failed file uploaded to {error_key}")

        log_message = (
            f"Stage: {stage}\n"
            f"File: {file_name}\n"
            f"Error: {str(error)}\n\n"
            f"{traceback.format_exc()}"
        )

        log_key = (
            f"{S3_ERROR_PREFIX}{stage}/{file_name}.log"
        )

        s3_client.put_object(
            Bucket=S3_ERROR_BUCKET,
            Key=log_key,
            Body=log_message
        )

        logger.info(
            f"Error log uploaded to {log_key}"
        )

    except Exception:

        logger.error(
            "Error Handler failed",
            exc_info=True
        )

        raise


def main():

    start_time = time.time()

    total_files = 0
    successful_files = 0
    failed_files = 0

    failed_file_list = []

    # Create AWS connection
    s3_client = get_s3_client()

    # Get Google credentials
    secret = read_google_credentials()

    # Authenticate Google Drive
    service = authenticate_credentials(secret)

    # Get files
    files = list_files(service)

    for file in files:

        total_files += 1

        file_id = file["id"]
        file_name = file["name"]

        file_path = None

        try:

            logger.info(
                f"Processing {file_name}"
            )

            # Download from Google Drive
            file_path = download_files(
                service,
                file_id,
                file_name
            )

            # Upload to S3
            upload_files_s3(
                s3_client,
                file_path,
                file_name
            )

            successful_files += 1

        except Exception as e:

            failed_files += 1

            failed_file_list.append(file_name)

            error_handler(
                s3_client,
                file_path,
                file_name,
                "google_drive_to_s3",
                e
            )

            continue

    end_time = time.time()

    logger.info("====================")
    logger.info("ETL JOB SUMMARY")
    logger.info("====================")

    logger.info(
        f"Total Files: {total_files}"
    )

    logger.info(
        f"Successful Files: {successful_files}"
    )

    logger.info(
        f"Failed Files: {failed_files}"
    )

    logger.info(
        f"Execution Time: {end_time-start_time:.2f} seconds"
    )

    if failed_file_list:

        logger.info(
            "Failed Files:"
        )

        for failed_file in failed_file_list:
            logger.info(failed_file)


if __name__ == "__main__":
    main()
