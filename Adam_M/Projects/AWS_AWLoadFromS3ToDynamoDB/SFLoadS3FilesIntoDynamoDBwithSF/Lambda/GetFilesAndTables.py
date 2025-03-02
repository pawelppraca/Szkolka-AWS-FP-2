import boto3
import json

#Lambda to get files and tables names

s3_client = boto3.client('s3', region_name="eu-west-1")


# example of event for VSC
event_input = {
      "s3path": "Loads/AW/Files/",
      "filename": "Header",
      "tablename": "adamm_Header",
      "colnames": ["SalesOrderID","OrderDate"]
}
#multi groups for step fucntion
"""{
  "fileLists": [
    {
      "s3path": "Loads/AW/Files/",
      "filename": "Header",
      "tablename": "adamm_Header",
      "colnames": ["SalesOrderID","OrderDate"]
    }
    ,{
      "s3path": "Loads/AW/Files/",
      "filename": "Items",
      "tablename": "adamm_Items",
      "colnames": ["SalesOrderDetailID","SalesOrderID"]
    }

  ]
}
"""

def lambda_handler(event, context):
    print("eventbody: ",event)
    bucket = 'adamm-bucket'
    s3path = event['s3path']                                     #get location of s3 files and set final path
    filename = event['filename']
    prefix_full = s3path+filename+"/"
    tablename = event['tablename']                               #get file name and table (you can decide if names should be unique)
    columnname = event['colnames']
    print("pref: ",prefix_full)

    #List objects in S3
    response = s3_client.list_objects_v2(
          Bucket=bucket,
          Prefix=prefix_full
      )
    files = [ obj['Key'] for obj in response.get('Contents', [])                 #get object name(key) 
             if obj['Key'].endswith('.csv')]                                     #filter by only csv files
    
    return {
        'files': files,
        'tablename': tablename,
        'colnames': columnname
    }


#manual check
result = lambda_handler(event_input, '1')
print(result)
#print(event_input)

