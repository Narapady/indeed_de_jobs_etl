import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = "indeed-de-jobs"
REGION = "us-west-2"


def create_bucket(bucket_name, region=None) -> bool:
    """Create an S3 bucket in a specified region"""
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            )
            location = {"LocationConstraint": region}
            response = s3_client.list_buckets()
            if bucket_name != response["Buckets"][0]["Name"]:
                s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main() -> None:
    file_name_raw = Path.cwd() / "dataset" / "indeed_de_jobs.csv"
    file_name_clean = Path.cwd() / "dataset" / "indeed_de_jobs_cleaned.csv"
    if create_bucket(BUCKET_NAME, REGION):
        upload_file(str(file_name_raw), BUCKET_NAME)
        upload_file(str(file_name_clean), BUCKET_NAME)


if __name__ == "__main__":
    main()
