import boto3
dynamodb_r = boto3.resource('dynamodb', region_name='eu-west-1')

#--------------------header
#"SalesOrderID","OrderDate"
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
print("Tables were created")


"""         Python                                  DynamoDB
            ------                                  --------
            None                                    {'NULL': True}
            True/False                              {'BOOL': True/False}
            int/Decimal                             {'N': str(value)}
            string                                  {'S': string}
            Binary/bytearray/bytes (py3 only)       {'B': bytes}
            set([int/Decimal])                      {'NS': [str(value)]}
            set([string])                           {'SS': [string])
            set([Binary/bytearray/bytes])           {'BS': [bytes]}
            list                                    {'L': list}
            dict                                    {'M': dict}"""