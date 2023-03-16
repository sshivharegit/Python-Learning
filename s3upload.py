import json
import logging
import os
import shutil
from datetime import datetime
from time import sleep
from urllib.request import urlopen

import boto3
import requests
from botocore.exceptions import ClientError
import subprocess

timestamp = datetime.now().replace(microsecond=0).isoformat()


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



def main():
    ET_BUCKET = "satyam-aws-learning"
    try:
        dt_now = datetime.utcnow()
        year = str(dt_now.year).zfill(4)
        month = str(dt_now.month).zfill(2)
        day = str(dt_now.day).zfill(2)
        hour = str(dt_now.hour).zfill(2)

        storage_path = (
            "year="
            + year
            + "/"
            + "month="
            + month
            + "/"
            + "day="
            + day
            + "/"
            + "hour="
            + hour
            + "/"
        )
        """pushing files into s3"""
        if os.path.exists("/Users/asthashrivastava/Desktop/Work/Python/Python-Learning/abc.json"):
            with open("/Users/asthashrivastava/Desktop/Work/Python/Python-Learning/abc.json", "rb"):
                bucket_key = "data/json/et_domain/" + storage_path + "abc.json"
            upload_file("/Users/asthashrivastava/Desktop/Work/Python/Python-Learning/abc.json", ET_BUCKET, bucket_key)
            print(
                "{}: Uploading file to s3 is complete".format(
                    datetime.now().isoformat()
                )
            )
        else:
            print(
                "{}: File /Users/asthashrivastava/Desktop/Work/Python/Python-Learning/abc.json does not exist".format(
                    datetime.now().isoformat()
                )
            )
    except Exception as e:
        print("{}: Exception occured: {}".format(datetime.now().isoformat(), e))

if __name__ == "__main__":
    main()
