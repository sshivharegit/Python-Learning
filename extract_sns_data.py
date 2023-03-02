import json
import urllib.parse
import boto3
import os
from datetime import datetime
import re
import quopri
import email


def lambda_handler(event, context):

    # Initiate boto3 client
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Get s3 object contents based on bucket name and object key; in bytes and convert to string
    sns_data = s3.get_object(Bucket=bucket, Key=key)
    contents = sns_data['Body'].read().decode("utf-8")
    
    msg = email.message_from_string(contents)
    payload = msg.get_payload()
    for i in payload:
        bc = i.as_string()
    payload_str = ''.join(bc)
    payload_bytes = bytes(payload_str, 'utf-8')
    decoded_payload = quopri.decodestring(payload_bytes)
    payload_str = decoded_payload.decode('utf-8')
    string = urllib.parse.unquote(payload_str).replace('"','').replace(' ','').replace('&amp','')

    content = re.findall('white>(.*)</span>', string)
    print(content)