import boto3
import os

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("<>"),
    aws_secret_access_key=os.getenv("<>"),
)
# Store bucket name
bucket_name = "adamm-bucket"

# Store contents of bucket
objects_list = s3.list_objects_v2(Bucket=bucket_name).get("Contents")

# Iterate over every object in bucket
for obj in objects_list:
    #  Store object name
    obj_name = obj["Key"]
    
    print(f"File name: {obj_name}\n--------------")