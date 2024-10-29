import json

#script reads json file and returns 3 objects 

fileName = 'D:/Docs/documents/python/json/USA_Dunkin_Donuts_Stores/archive/dunkinDonuts_small.json'

# Opening JSON file
f = open(fileName)
 
# returns JSON object as 
# a dictionary
data = json.load(f)

#print in human format
for row_json in data['data']:
    print(row_json['recordId'],row_json['city'],row_json['state'])

# Closing file
f.close()

#output
#354702 South San Francisco CA



#---------------------
##print in easier to read format with indent of 4 spaces
#for row_json in data['data']:
#    print(json.dumps(row_json, indent=4))
## Closing file
#f.close()

