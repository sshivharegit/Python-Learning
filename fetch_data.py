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

ET_URL_BASE = "https://rules.emergingthreatspro.com/5315783806243228/reputation"
ET_URL_IP = f"{ET_URL_BASE}/detailed-iprepdata.txt"
ET_URL_DOMAIN = f"{ET_URL_BASE}/detailed-domainrepdata.txt"
timestamp = datetime.now().replace(microsecond=0).isoformat()

def download_data():
    print("{}: Downloading files".format(datetime.now().isoformat()))
    response = requests.get(ET_URL_IP)
    open("/tmp/et_ip.txt", "wb").write(response.content)

    response = requests.get(ET_URL_DOMAIN)
    open("/tmp/et_domain.txt", "wb").write(response.content)

    file_json = open("/tmp/et_ip.json", "w")
    with open("/tmp/et_ip.txt", "r") as data:
        lines = data.readlines()
        del lines[0]
        ip_dict = {}
        for line in lines:
            ip = line.split(",")[0]
            category = int(line.split(",")[1])
            score = int(line.split(",")[2])
            first_seen =  line.split(",")[3]
            last_seen = line.split(",")[4]
            ports = (line.split(",")[5]).split()
            ip_dict["IP_ADDRESS"] = ip
            ip_dict["CATEGORY"] = category
            ip_dict["SCORE"] = score
            ip_dict["FIRST_SEEN"] = first_seen
            ip_dict["LAST_SEEN"] = last_seen
            ip_dict["PORTS"] = ports
            ip_dict["timestamp"] = timestamp

            file_json.write(json.dumps(ip_dict))
            file_json.write("\n")

    file_json1 = open("/tmp/et_domain.json", "w")
    with open("/tmp/et_domain.txt", "r") as data2:
        lines2 = data2.readlines()
        del lines2[0]
        domain_dict = {}
        for line2 in lines2:
            domain = line2.split(",")[0]
            category = int(line2.split(",")[1])
            score = int(line2.split(",")[2])
            first_seen = line2.split(",")[3]
            last_seen = line2.split(",")[4]
            ports = (line2.split(",")[5]).split()
            domain_dict["Domain"] = domain
            domain_dict["CATEGORY"] = category
            domain_dict["SCORE"] = score
            domain_dict["FIRST_SEEN"] = first_seen
            domain_dict["LAST_SEEN"] = last_seen
            domain_dict["PORTS"] = ports
            domain_dict["timestamp"] = timestamp

            file_json1.write(json.dumps(domain_dict))
            file_json1.write("\n")


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

def check_version(bucket):
    available_version = requests.get(ET_URL_AVAILABLE_VERSION)
    available_version = available_version.text.rstrip()
    s3_upload = False
    file_key=  'data/version.txt'
    s3 = boto3.client('s3')
    result = s3.list_objects(Bucket=bucket, Prefix=file_key)
    obj = boto3.resource('s3').Object(bucket, file_key)
    if 'Contents' in result:
        body = obj.get()['Body'].read().decode('utf-8')
        if body != available_version:
            s3_upload = True
            with open('version.txt', 'w') as infile:
                infile.write(available_version) 
            print(
                "{}: Received a new version {}".format(
                    datetime.now().isoformat(), available_version
                )
            )
        else:
            return False
    else:
        with open('version.txt', 'w') as infile:
            infile.write(available_version)
        s3_upload = True

    if s3_upload == True:
        s3.upload_file('version.txt', bucket, file_key)
        print(
                "{}: Version.txt has been Updated in S3".format(
                            datetime.now().isoformat()))
    return True

def run_gluejob_and_crawler(glue_job, data_bucket, glue_crawler, part_year, part_month, part_day, part_hour):
    # start handling glue etl job run abd crawler
    glue = boto3.client(service_name='glue', region_name='us-west-2',
          endpoint_url='https://glue.us-west-2.amazonaws.com')
    myNewJobRun = glue.start_job_run(JobName=glue_job,
                    Arguments = {
                        '--BUCKET': data_bucket,
                        '--YEAR': part_year,
                        '--MONTH': part_month,
                        '--DAY': part_day,
                        '--HOUR': part_hour })
    status = glue.get_job_run(JobName=glue_job, RunId=myNewJobRun['JobRunId'])
    ms = "{}: Glue job is running ... "
    print(ms.format(datetime.now().isoformat()))
    if status:
        state = status['JobRun']['JobRunState']
        while state not in ['SUCCEEDED']:
            sleep(30)
            status = glue.get_job_run(JobName=glue_job, RunId=myNewJobRun['JobRunId'])
            state = status['JobRun']['JobRunState']
            if state in ['STOPPED', 'FAILED', 'TIMEOUT']:
                ms = "{}: Failed to execute glue job: " + status['JobRun']['ErrorMessage'] + '. State is : ' + state
                print(ms.format(datetime.now().isoformat()))
                break
    ms = "{}: Glue job is finished."
    print(ms.format(datetime.now().isoformat()))
    glue.start_crawler(Name=glue_crawler)
    ms = "{}: Crawler is run."
    print(ms.format(datetime.now().isoformat()))

def fetch_url_content_as_int(url: str):
    """Download the content from the URL provided and convert it to an int."""
    content_as_binary = urlopen(url).read()
    content_as_string = content_as_binary.decode("utf-8").strip()
    return int(content_as_string)


def main():
    ET_BUCKET = os.environ["ET_BUCKET"]
    check_result = check_version(ET_BUCKET)
    if check_result == True:
        try:
            download_data()
            if os.path.exists("/tmp/et_ip.txt"):
                os.remove("/tmp/et_ip.txt")
            if os.path.exists("/tmp/et_domain.txt"):
                os.remove("/tmp/et_domain.txt")

            
            ET_GLUE_JOB = os.environ["ET_GLUE_JOB"]
            ET_GLUE_CRAWLER = os.environ["ET_GLUE_CRAWLER"]
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
            if os.path.exists("/tmp/et_ip.json"):
                with open("/tmp/et_ip.json", "rb"):
                    bucket_key = "data/json/et_ip/" + storage_path + "detailed-iprepdata.json"
                upload_file("/tmp/et_ip.json", ET_BUCKET, bucket_key)
                print(
                    "{}: Uploading file to s3 is complete".format(
                        datetime.now().isoformat()
                    )
                )
                os.remove("/tmp/et_ip.json")
            else:
                print(
                    "{}: File /tmp/et_ip.json does not exist".format(
                        datetime.now().isoformat()
                    )
                )

            if os.path.exists("/tmp/et_domain.json"):
                with open("/tmp/et_domain.json", "rb"):
                    bucket_key = "data/json/et_domain/" + storage_path + "detailed-domainrepdata.json"
                upload_file("/tmp/et_domain.json", ET_BUCKET, bucket_key)
                print(
                    "{}: Uploading file to s3 is complete".format(
                        datetime.now().isoformat()
                    )
                )
                os.remove("/tmp/et_domain.json")
            else:
                print(
                    "{}: File /tmp/et_domain.json does not exist".format(
                        datetime.now().isoformat()
                    )
                )

            run_gluejob_and_crawler(ET_GLUE_JOB,ET_BUCKET, ET_GLUE_CRAWLER, year, month, day, hour)

            message = "scheduled-job.run:1|c|#" + os.environ['SERVICE_TAGS'] + ",op:done"
            subprocess.run(["./datadog_submit.sh", message])
        except Exception as e:
            print("{}: Exception occured: {}".format(datetime.now().isoformat(), e))
    else:
        print(
            "{}: File version has not been updated yet".format(
                datetime.now().isoformat()
            )
        )


if __name__ == "__main__":
    main()
    
