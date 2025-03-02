import boto3
import json
from decimal import Decimal
import pandas as pd
import io

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket = 'adamm-bucket'
    print("ev:",event)
    
    fileslist =  event['files']
    tables = event['tablename'] # List of table names
    col_to_return = event['colnames']
    
    
    # Get the object from S3
        
    #for row_key in fileslist:
    print("bucket: ",bucket)
    
    response = s3_client.get_object(Bucket=bucket, Key=fileslist)

    body = response['Body']
    df = pd.read_csv(io.BytesIO(body.read()))
    df = df[col_to_return]
      
      # Load to each table in the group
    table = dynamodb.Table(tables)
          
    from decimal import Decimal        
    with table.batch_writer() as batch:
      for index, row in df.iterrows():
          batch.put_item(Item=
          json.loads(row.to_json(), parse_float=Decimal)
          )
    
    
    return {
        'statusCode': 200,
        'body': f'Successfully loaded {fileslist} to {len(tables)} tables'
        #'body': f'Successfully loaded - dev {event}, {fileslist}'
    }


ev = {
    "colnames": [
      "SalesOrderID",
      "OrderDate"
    ],
    "files": "Loads/AW/Files/Header/Header.csv",
    "tablename": "adamm_Header"
  }
res = lambda_handler(ev,"1")
print("result:",res)