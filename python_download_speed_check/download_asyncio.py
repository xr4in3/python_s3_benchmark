import boto3
from boto3.session import Session
import botocore
import os
import shutil
from dotenv import load_dotenv, find_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import asyncio
import aiofiles as aiof
import aiobotocore

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


filenames = [x.key for x in bucket.objects.all()]

async def download_files():

    session = aiobotocore.get_session(loop=loop)
    async with session.create_client(
        "s3",
        region_name="eu-central-1",
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
    ) as client:
      
        start = time.time()

        for s3_file in filenames:
            try:
                async with aiof.open(f"download/{s3_file}", "wb") as data:
                    response = await client.get_object(
                        Bucket=bucket_name, Key=s3_file
                    )
                    async with response["Body"] as stream:
                        content = await stream.read()
                        await data.write(content)

            except FileNotFoundError as e:
                print(e)
        end = time.time()
        with open("timers.txt", "a") as f:
            f.write("Asyncio:" + " " + str(int(end - start)) + "\n")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    asyncio.get_event_loop().run_until_complete(download_files())

    shutil.rmtree("download")
    os.mkdir("./download")
