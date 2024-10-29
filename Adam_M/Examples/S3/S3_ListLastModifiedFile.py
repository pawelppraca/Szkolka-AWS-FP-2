import boto3

s3 = boto3.resource('s3')
my_bucket = s3.Bucket('adamm-bucket')
files_def = my_bucket.objects.filter(Prefix="sparklogs/logs/")
files = [obj.key for obj in sorted(files_def, key=lambda x: x.last_modified, 
    reverse=True)][0]
files_date = [obj.last_modified for obj in sorted(files_def, key=lambda x: x.last_modified, 
    reverse=True)][0]
print("File: ",files,'\t',files_date)