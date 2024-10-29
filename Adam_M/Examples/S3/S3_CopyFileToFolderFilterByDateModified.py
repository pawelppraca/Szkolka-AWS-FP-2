import boto3
import datetime
#bucket Name
bucket_name = 'adamm-bucket'
#folder Name
folder_name = 'copyfiles/source/'

folder_list = { 'a','b' }

#set particular date format that we want to filter
file_date_starttime = "2024-10-28 14:12:00.000000+00:00"
file_date_endtime =  "2024-10-28 14:15:05.000000+00:00"

#--method to move file
def copy_file(bucket_name_VAR, row_folder_name_VAR, file_VAR):
    session = boto3.session.Session()
    s3_resource = session.resource('s3')

    source_path = {"Bucket": bucket_name_VAR, "Key": file_VAR}
    s3_resource.meta.client.copy(source_path, bucket_name_VAR, file_VAR.replace("copyfiles/source/","copyfiles/target/") )
    print(source_path)
    #s3_client.Object(bucket_name_VAR, file_VAR).delete()
    

#------------main part

session = boto3.session.Session()
s3_resource = session.resource('s3')
for row_rolder in folder_list:
    row_folder_name = folder_name+row_rolder+"/"
    
    result = s3_resource.meta.client.list_objects(Bucket=bucket_name, Prefix=row_folder_name)
    if "Contents" in result:
        for obj in result['Contents']:
            if str(obj["LastModified"]) >= str(file_date_starttime) and str(obj["LastModified"]) <= str(file_date_endtime):
                full_s3_file = "s3://" + bucket_name + "/" + obj["Key"]
                print(full_s3_file)
                print(str(obj["LastModified"]),"\n")
                copy_file(bucket_name, row_folder_name, obj["Key"])
    else:
        print(f"{row_folder_name} not exists")
    
    #print(row_rolder,row_folder_name)        

