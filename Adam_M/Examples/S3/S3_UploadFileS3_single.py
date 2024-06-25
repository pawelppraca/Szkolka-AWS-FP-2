#manual run in terminal
#aws s3 cp file.csv s3://adamm-bucket/

import boto3
from botocore.exceptions import ClientError

#Script to upload certain file from local folder on S3 bicket


#--function
def UploadFileCSVToS3(LocFileName,S3FileName,S3BucketName):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(LocFileName,S3BucketName,S3FileName)
    

#-main
S3Bucket = 'adamm-bucket'
S3File = 'RetailDataAnalytics/Sales_upload2.csv'
LocalFiles = 'D:\Docs\documents\python\csv\Sample_RetailDataAnalytics\small\Sales.csv'

try:
    response = UploadFileCSVToS3(LocalFiles,S3File,S3Bucket)
    print("SUCCESS upload")
except ClientError as e:
    print("File was not uploaded")

