import pandas as pd

#Script to read json file, 1st prints full content and 2nd prints only content where certain criteria are met (line 17)
 
#file
fileLocation = 'D:/Docs/documents/python/json/scaler/names.json'

df = pd.read_json(fileLocation)
dataf = pd.DataFrame(df)

#print everything
print("All data")
print(dataf)

print("\n")
#filter data by math and name
filter_data = dataf[(dataf['math'] > 75) & (dataf['name'] == 'James')].to_json(index=False, orient="records")
print("Filter by math and name")
print(filter_data)

print("\n")
#returns info about dataframe including column or datatype
print(dataf.info())
