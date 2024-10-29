import json

#fileName = 'D:/Docs/documents/python/json/USA_Dunkin_Donuts_Stores/archive/dunkinDonuts_small.json'

#script read from variable with json format and return only hashKey

fileName = """{
    "data": [
        {
            "hashKey": -918298,
            "geoJson": {
                "coordinates": [
                    37.649585,
                    -122.40607
                ],
                "type": "Point"
            },
            "geohash": -9182987302055287277,
            "recordId": "354702"
        }
    ]
}"""            

json_dict = json.loads(fileName)
for row_json in json_dict['data']:
    print(row_json['hashKey'])


#filtered_data = {key: json_dict["data"]["hashKey"] for key in json_dict["data"]["hashKey"]} 
#print(filtered_data)     
#print(json_dict['data']['hashKey'])