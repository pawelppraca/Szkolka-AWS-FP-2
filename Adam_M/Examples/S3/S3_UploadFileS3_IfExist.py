
import os
import boto3
from botocore.exceptions import ClientError
from requests import session

#Script checks if files from local folder (LocalPath) exist in S3 bucket, if yes then upload, otherwise skip
#here list of local files is loaded in list which is converted to set then we compare sets and take only files that are set in files_list_pick


#--function
#upload file on S3 from local folder
def upload_file_csv_to_s3(LocFileName,S3FileName,S3BucketName):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(LocFileName,S3BucketName,S3FileName)
    
#check if file exits
def s3_check_if_file_exist(S3Bucket,S3FileCheck):

    session = boto3.client('s3')
    response = session.list_objects_v2(Bucket=S3Bucket)

    if 'Contents' in response:
        existing_keys = {item['Key'] for item in response['Contents']}
        return {key: key in existing_keys for key in S3FileCheck}
    else:
        return {key: False for key in S3FileCheck}

    

#-main body
# get list of files from local folder
LocalPath = "D:/Docs/documents/python/csv/Sample_RetailDataAnalytics/small"
files_list = os.listdir(LocalPath)
#select certain file and convert list to set, get item which exists in both sets then convert back to list
files_Set = set(files_list)
files_list_pick ={'Sales.csv','Store_Info.csv'} #file we want to upload
files_final_list = files_Set & files_list_pick



#set bucket and files
S3Bucket = 'adamm-bucket'
#S3File = 'RetailDataAnalytics/Sales_upload.csv'
#S3FileCheck = ['RetailDataAnalytics/Sales_upload.csv','RetailDataAnalytics/Stores.csv']
#LocalFiles = ['D:/Docs/documents/python/csv/Sample_RetailDataAnalytics/small/Sales.csv']
S3FileFolder = 'RetailDataAnalytics/'

#prepare files + path for S3
S3FileCheck = [S3FileFolder + pre for pre in files_final_list]


result = s3_check_if_file_exist(S3Bucket, S3FileCheck)

for key, exists in result.items():
    if( exists == True):
        print("Key ",key," exists: ",exists)    
    else:
        try:
            #print(LocalPath + key.replace("RetailDataAnalytics/",""),"-",str(key),"-",S3Bucket)
            response = upload_file_csv_to_s3(LocalPath + key.replace("RetailDataAnalytics/","/"),str(key),S3Bucket)
        except ClientError as e:
            print("Error. File was not uploaded")




#article
#https://towardsthecloud.com/aws-sdk-key-exists-s3-bucket-boto3


#manual run in terminal
#aws s3 cp file.csv s3://adamm-bucket/


