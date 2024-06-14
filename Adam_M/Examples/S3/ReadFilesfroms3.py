import boto3
import sys
import os
import csv
import io


#script does search in S3 bucket and RetailDataAnalytics subfolder and return csv files

"""Accessing the S3 buckets using boto3 client"""
s3_client =boto3.client('s3')
s3_bucket_name='adamm-bucket'

""" Getting data files from the AWS S3 bucket and search objects indicates in filter prefix"""
s3 = boto3.resource('s3')
my_bucket=s3.Bucket(s3_bucket_name)
bucket_list = []
for file in my_bucket.objects.filter(Prefix = 'RetailDataAnalytics/'):
    file_name=file.key
    if file_name.find(".csv")!=-1:
        bucket_list.append(file.key)
length_bucket_list=print(len(bucket_list))
print(bucket_list[0:10])

#article
#https://www.sqlservercentral.com/articles/reading-a-specific-file-from-an-s3-bucket-using-python