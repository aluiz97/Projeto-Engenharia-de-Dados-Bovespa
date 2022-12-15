# %%
import boto3
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv
from os import getenv
import time

# %%
load_dotenv('/home/ubuntu/.env')

# %%
s3_client = boto3.client(
    's3',
    aws_access_key_id = getenv('AWS_ID'),
    aws_secret_access_key = getenv('AWS_KEY')
)

# %%

def bucket(name: str):

    try:
        s3_client.create_bucket(Bucket=name)

    except ClientError as e:
        logging.error(e)
        return False
    
    return True

def create_folders(bucket_name:str, folders: list):

    for i in range(len(folders)):

        s3_client.put_object(Bucket=bucket_name, Key=(folders[i]+'/'))

def upload_file(bucket:str, folder:str, file_name:str, file_saved):

  client.put_object(Body=file_saved, Bucket=bucket, Key=f'{folder}/{file_name}.json')
