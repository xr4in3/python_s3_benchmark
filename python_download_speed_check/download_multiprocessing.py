import boto3
from boto3.session import Session
import botocore
import os
import shutil
from dotenv import load_dotenv, find_dotenv
import time
import multiprocessing

load_dotenv(find_dotenv())
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("S3_BUCKET")

session = Session(
    aws_access_key_id=access_key, aws_secret_access_key=secret_key
)
s3 = session.resource("s3")
bucket = s3.Bucket(bucket_name)
client = boto3.client("s3")
start = time.time()

filenames = [x.key for x in bucket.objects.all()]


def download_files(s3_file):
    try:
        with open(f"download/{s3_file}", "wb") as data:
            client.download_fileobj(bucket_name, s3_file, data)
    except FileNotFoundError as e:
        print(e)


if __name__ == "__main__":
    p = multiprocessing.Pool(processes=os.cpu_count())
    start = time.time()
    p.map(download_files, filenames)
    end = time.time()
    with open("timers.txt", "a") as f:
        f.write("Multiprocessing:" + " " + str(int(end - start)) + "\n")

    shutil.rmtree("download")
    os.mkdir("./download")
