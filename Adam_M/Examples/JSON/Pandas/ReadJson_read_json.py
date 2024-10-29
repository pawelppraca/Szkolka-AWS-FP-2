import json

#script reads from json, puts into dictionary and returns only row where conditions are met (line 17)
 
fileName = 'D:/Docs/documents/python/json/scaler/names.json'


# Opening JSON file
f = open(fileName)
 
# returns JSON object as 
# a dictionary
data = json.load(f)


#filter and return only when math > 75
filter_data = [ person for person in data if person["math"]>75 ]
print(filter_data)


# Closing file
f.close()