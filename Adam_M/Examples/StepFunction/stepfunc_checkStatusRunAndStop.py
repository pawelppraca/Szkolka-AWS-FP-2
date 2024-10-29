import boto3
import json
import datetime

#-----------script check if there is running certain step function, if yes then stops it-------------

client = boto3.client("stepfunctions", region_name="eu-west-1")

#set arn of step function you want to check
state_mac = "arn:aws:states:eu-west-1:927230372648:stateMachine:adamm-stepfunct-test"

#start - method to check status of step functions
def sf_check_status(response,param_idle,currenttime):

    #prepare empty list which we will use later on to put running ARNs
    sf_to_stop = []
    for response_item in response['executions']:
        response_startDate = response_item['startDate'].replace(tzinfo=None) #remove timezone offset from json format
        response_time_diff = currenttime-response_startDate
        response_time = round(response_time_diff.total_seconds(),2) # get seconds format
        #if (((response_item['status'] == "RUNNING" ) and (response_time > param_idle))):
        if  (response_time > param_idle):
            print(response_item['startDate'],response_item['executionArn'],response_time)
            sf_to_stop.append(response_item['executionArn'])
#        else:
            #print("no long running sf",response_time,response_item['startDate'],currenttime)
#            pass
    return sf_to_stop
#ends

response = client.list_executions(
    stateMachineArn=state_mac,
    statusFilter='RUNNING',
    maxResults=123
)

param_idle = 30 #param which indciates how long job is running

currenttime = datetime.datetime.now()

result_sf_check_status = sf_check_status(response,param_idle,currenttime) #call method to check step funct status
print("result: ",result_sf_check_status)

#temp = ['arn:aws:states:eu-west-1:927230372648:execution:adamm-stepfunct-test:20aae28a-1c83-4e39-b66b-53d1c074f5c6','nextarn']
if not result_sf_check_status:
    print("no sf to stop")
else:    
    for result_arn in result_sf_check_status:
        print("kill: ",result_arn)
        response_sf_check_status = client.stop_execution(executionArn=result_arn,error='Manual stop')
        print(response_sf_check_status)
