import os
import sys
import time
import logging
import threading
from turtle import down

import boto3

from botocore.exceptions import ClientError
from boto3.session import Session

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] - %(asctime)s: %(message)s",
    datefmt="%d-%m-%Y %I:%M:%S"
)
logger = logging.getLogger("AWS S3 Single Downloader")
logger.setLevel("INFO")

# AWS Keys
BUCKET_NAME = "kamleshtest1"

S3_CLIENT = boto3.client('s3')
S3_RESOURCE = boto3.resource('s3')


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            S3_CLIENT.create_bucket(Bucket=bucket_name)
        else:
            S3_CLIENT = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            S3_CLIENT.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_existing_buckets():
    # Retrieve the list of existing buckets
    response = S3_CLIENT.list_buckets()
    return [bucket for bucket in response['Buckets']]


def get_all_bucket_objects(bucket_name):
    """ Returns bucket objects.

    Returns:
        list: Bucket objects
    """
    return [s3_file for s3_file in S3_RESOURCE.Bucket(bucket_name).objects.all()]


def download_single_file(file_name, bucket_name, object_name=None, directory=None):
    # If S3 object_name was not specified, use file_name.
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Set directory if not None.
    if directory is not None:
        file_path = f"{directory}/{file_name}"
    else:
        file_path = f"{file_name}"

    # Download the file.
    try:
        response = S3_CLIENT.download_file(bucket_name, object_name, file_path)
        logger.info(f"downloaded {object_name}")
    except ClientError as error:
        logging.error(error)
        return False
    return True


def download_all_files(prefix, local, bucket, client=S3_CLIENT):
    """ Downloads all files in dir from S3.

    Args:
        prefix (str): pattern to match in s3
        local (str): local path to folder in which to place files
        bucket (str): s3 bucket with target contents
        client (client, optional): initialized s3 client object.
    """

    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)
        logger.info(f"downloaded - {k}")


# Start downloading
logger.info("Downloading...")
start_timer = time.perf_counter()

all_file_objects = get_all_bucket_objects(BUCKET_NAME)
all_file_names = [file.key for file in all_file_objects]

download_all_files(
    prefix="",
    local="downloads",
    bucket=BUCKET_NAME,
)


end_timer = time.perf_counter()
logger.info(f"Downloaded in {end_timer - start_timer:0.2f} seconds.")