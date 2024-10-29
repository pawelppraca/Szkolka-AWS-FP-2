import pandas as pd


#read from json file using pandas and return top 1 item

fileName ='D:/Docs/documents/python/json/USA_Dunkin_Donuts_Stores/archive/dunkinDonuts_small.json'

#read_json
df_read_json = pd.read_json(fileName)
print("DataFrame using pd.read_json() method:")
print(df_read_json)

#data frame and top 1
df = pd.DataFrame(df_read_json)
print("\nDataFrame using dataframe method:")
#print(df.head(1))
df2 = df_read_json["data"]
