import boto3
import os

s3 = boto3.client('s3')

#Script list objects from S3 and return content

# Set your bucket name
bucket_name = "adamm-bucket"

# Store contents of bucket
objects_list = s3.list_objects_v2(Bucket=bucket_name).get("Contents")

# Iterate over every object in bucket
for obj in objects_list:
    #  Store object name
    obj_name = obj["Key"]
    
    if(obj_name == "uscities_test.csv"):
        # Read an object from the bucket
        response = s3.get_object(Bucket=bucket_name, Key=obj_name)
    
        # Read the object’s content as text
        object_content = response["Body"].read().decode("utf-8")
    
        # Print all the contents
        print(f"Content of {obj_name}\n--------------")
        print(object_content, end="\n\n")

    else:
        pass

    
