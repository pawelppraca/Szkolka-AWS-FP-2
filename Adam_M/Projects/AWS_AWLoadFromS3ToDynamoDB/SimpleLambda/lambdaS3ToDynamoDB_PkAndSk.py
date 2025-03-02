import json
#import os
import io
import boto3
import pandas as pd
#--------------------------------------------------------------------------------------------------
#script to read files from particular s3 location (requires json config file with metadata to load), 
#next import to pandas and store in dynamodb. 
#to store in dynamo db you need to provide metadata (columns datatypes) in config json file, otherwise it will be skipped
#using batch_writer which is recommended if we want to load more data. Good for insert and delete, but WON'T work for updates
#also when using put_item it will overwrite existing value
#we use partition key and sort key (2 different attributes). if we load data, next change value in sort key, then we will get another row
#f.i. pk=1, s=100, -> sk value is changed to 200, then we get new row and have:  pk    sk
#                                                                                 1   100
#                                                                                 1   200
#--------------------------------------------------------------------------------------------------

# #remember to add pandas in layers so that this library can be used

#set s3
#aws_session = boto3.Session()
#client = aws_session.client('s3', region_name="eu-west-1")
client = boto3.client('s3', region_name="eu-west-1")
my_bucket = "adamm-bucket"
#set dynamo
dynamodb_r = boto3.resource('dynamodb', region_name='eu-west-1')
dynamodb_c = boto3.client('dynamodb', region_name='eu-west-1')
#set config 
prefix = 'Loads/AW/Files/'
cfg_column_name_file = "Loads/AW/Config/column_list.json"                   # column and data types config

def lambda_handler(event, context):
    # main part
    processdata()
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda script completed!')
    }

#list of tables and columns to return
def get_column_names(TableName, mode):
    json_response = client.get_object(Bucket=my_bucket, Key=cfg_column_name_file)
    s3_object_body = json_response.get('Body')

    # Read the data in bytes format
    content = s3_object_body.read()

    json_dict = json.loads(content)
    #print(json_dict)
    ColumnsToDrop_List = []
    
    for row_column in json_dict:
        if row_column["name"] == TableName:
        #print(row_column["column"])
            ColumnsToDrop_List.append(row_column["column"])

    print(ColumnsToDrop_List[0]) # to return list of columns from json config
    if mode == 1:
        columnlist_return = json_dict
    else:
        columnlist_return = ColumnsToDrop_List[0]
    return columnlist_return


def get_data_frame(fileToRead, col_to_return):                          #read file and transform to pandas df
    csv_obj = client.get_object(Bucket=my_bucket, Key=fileToRead) #Key="Loads/AW/Header/*.csv"
    body = csv_obj['Body']
    df = pd.read_csv(io.BytesIO(body.read()))
    df = df[col_to_return]
    print(f"File: {fileToRead}")
    #print(df.head(2))
    return df

def processdata():                                                      #main part to process data - load from s3, transform and store in dynamodb
    spl_str="/"                                                         # char used to split and exclude that is after
    username="adamm_"

    response = client.list_objects_v2(Bucket=my_bucket, Prefix=prefix)  #if you add Delimiter="/" then it will run only in this folder
    files = response.get("Contents")
    for file in files:
        if file['Key'].endswith('.csv'):                                                        #filter only csv files
            tbl_name = username+file['Key'].replace(prefix,"").split(spl_str)[0]                #get folder name, set as table name + username
            tbl_name_orig = file['Key'].replace(prefix,"").split(spl_str)[0]
            col_to_return = get_column_names(tbl_name_orig,0)                                   #get column names for each table
            get_df = get_data_frame(file['Key'], col_to_return)                                 #get content file into pandas df
            
            print(tbl_name)
            col_to_return_full = get_column_names(tbl_name_orig,0)     
            get_table = check_or_create_table(tbl_name,col_to_return_full)                      #check if table exist in DynamoDB, if not then create
            insert_data(get_table,get_df,col_to_return_full)                                    #insert data into dynamodb

  

def check_or_create_table(dynamo_tbl_name,col_to_return_full):
    print("DynamoDB: Creating table")

    get_table = dynamodb_r.Table(dynamo_tbl_name)
    response_check_table = dynamodb_c.list_tables()

    if dynamo_tbl_name in response_check_table['TableNames']:                      #check if table exists
        print(f"Table {dynamo_tbl_name} exists")
        return get_table
    else:
        print(f"Table {dynamo_tbl_name} not exists")
        #creating tables
        table = dynamodb_r.create_table(
                    TableName="adamm_Header",
                    KeySchema=[
                        {
                            'AttributeName': 'SalesOrderID',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'OrderDate',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'SalesOrderID',
                            'AttributeType': 'N'
                        },
                        {
                            'AttributeName': 'OrderDate',
                            'AttributeType': 'S'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                )
        table.wait_until_exists()
        #-----------------items
        #   SalesOrderID  SalesOrderDetailID
        table2 = dynamodb_r.create_table(
                    TableName="adamm_Items",
                    KeySchema=[
                        {
                            'AttributeName': 'SalesOrderDetailID',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'SalesOrderID',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'SalesOrderDetailID',
                            'AttributeType': 'N'
                        },
                        {
                            'AttributeName': 'SalesOrderID',
                            'AttributeType': 'N'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
        table2.wait_until_exists()
        print("Tables were created")        # Wait until the table exists.
        return table


def insert_data(table, df, columns):
    print(f"DynamoDB inserting data in {table}")
    from decimal import Decimal        
    with table.batch_writer() as batch:
        for index, row in df.iterrows():
                
                batch.put_item(Item=
                    json.loads(row.to_json(), parse_float=Decimal)
                    )
                
               

    """with batch_write_test_table.batch_writer() as batch:
    for no in range(1, 6):
        batch.put_item(Item={'PK': 'hi', 'SK': f'user-{no}'})"""
    print("DynamoDB insert completed")

"""with my_table.batch_writer () as batch: #Add items to the table
    batch.put_item(
        Item={
            'movie_title' : 'Titanic',
            'director_name' : 'James Cameron'
            }
        )
"""
#---------

"""prefix = 'Loads/AW/' 
spl_str="/"
tests = "Loads/AW/Header/Header.csv"
print(tests.replace(prefix,"").split(spl_str)[0])
"""

#---------------------only for VSC
lambda_handler('1','2')