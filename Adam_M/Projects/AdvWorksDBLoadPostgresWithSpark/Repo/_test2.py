
from datetime import datetime

# Get the current date and format it
#c = datetime.now().strftime("%Y%m%d%H%M%S")
#print(c)

"""from multiprocessing import Pool

def f(x):
    return x*x
lst = [1, 2, 3]

if __name__ == '__main__':
    with Pool(5) as p:
        print(p.map(f,lst ))"""

import sys
from pyspark.sql import SparkSession


"""spark = SparkSession.builder.appName('SParkByExample').getOrCreate()
#collection
df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("stach", 13),
    ],
    ["first_name", "age"],
)
df.show()"""

"""#---nested dictionary
listlist = []
ratings1 = {
            "Arkadiusz": ["c1","c2","c3"],
            "Agness": ["d1","d2","d3","d5"]           
           }    
for imie in ratings1:
    if imie == "Arkadiusz":
        print(ratings1[imie])
        print(imie," ocena",ratings1[imie])
    #for get_c in ratings1[imie]:
    #    print("get:",get_c)
    #listlist = ratings1[imie]
    #print(listlist)
    """

"""import json
fileName ='D:/Docs/documents/python/projects/AWDBLoadPostgresWithSpark/Repo/ConfigData/extract_columns_dropList.json'

f = open(fileName)"""
 
# returns JSON object as 
# a dictionary
"""data = json.load(f)
#filter_data = [ row_table for row_table in data if row_table["name"] == "Sales.SalesOrderHeader" ]
#print(data)
for row_table in data:
    if row_table["name"] == "Sales.SalesOrderHeader":
        print(row_table["name"],row_table["column"])
    else:
        print(row_table["name"],row_table["column"])

list2 = ['Employee ID','Employee NAME','Company Name'] 
print(list2)"""

#-----------------#
"""def ColumnsToDropGetList(Var_ColumnTable,VAR_ColumnsToDrop_list):
    ColumnsToDrop_List = []
    
    for row_column in VAR_ColumnsToDrop_list:
      if row_column["name"] == Var_ColumnTable:
          print(row_column["column"])
          ColumnsToDrop_List.append(row_column["column"])
    
    return ColumnsToDrop_List[0]

#extract_ColumnsToDrop = FullFolderPath+"/Repo/ConfigData/"+FileListOfColumnsToBeDropped
extract_ColumnsToDrop_open = open(fileName)
extract_ColumnsToDrop_list = json.load(extract_ColumnsToDrop_open)

ColumnsToDropGetList_result = ColumnsToDropGetList("Sales.SalesOrderHeader",extract_ColumnsToDrop_list)
print(ColumnsToDropGetList_result)"""

#----------------------------

"""LoadTruncate_list = []
list2  = ['Sales.SalesOrderHeader', 'Sales.SalesOrderDetail']
for l_row in list2:
    LoadTruncate_list.append(l_row+",")

LoadTruncate_final = "".join(LoadTruncate_list)
LoadTruncate_final = "TRUNCATE TABLE \n"+LoadTruncate_final
LoadTruncate_final = LoadTruncate_final.rstrip(',')+";"
print(LoadTruncate_final)    

"""
"""Temp_var = "Stg.SalesOrderHeader"
LoadTruncate_list = []
list2  = ['Sales.SalesOrderHeader_20241006161853_1.csv','Sales.SalesOrderHeader_20240925213841_1.csv']
for l_row in list2:
    LoadTruncate_list.append("'"+l_row+"',")

LoadTruncate_final = "".join(LoadTruncate_list)
LoadTruncate_final = LoadTruncate_final.rstrip(',')
LoadTruncate_final = f"DELETE FROM {Temp_var} where LoadFileName in ( {LoadTruncate_final} )"+";"
print(LoadTruncate_final)    

import random
def rand_unique(total, amount):
    print(random.sample(range(total+1),amount))

rand_unique(30,3)"""

"""
import pandas as pd
import random
VAR_trans_Dictlist_final = "D:/Docs/documents/python/projects/AWDBLoadPostgresWithSpark/Files/TransactionData/Purchasing.ShipMethod/part-00000-3ff8d5cf-9d07-41e0-b767-618f0eeafbea-c000.csv"

trans_getpandas = pd.read_csv(VAR_trans_Dictlist_final, header=0, dtype=str)   #skip header
#print(trans_getpandas)
trans_getpandas_df = pd.DataFrame(trans_getpandas)
print(trans_getpandas_df)
trans_data =  trans_getpandas_df.values.tolist()
print(trans_data)
"""

"""from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,struct

spark = SparkSession.builder.appName('SParkByExample').getOrCreate()
df = spark.createDataFrame([('Alice', 1)], ['name', 'age'])

df.withColumn("salary", lit(34000))\
       .withColumn('points3', lit(3)).show()


"""
#---

"""import pandas as pd

df = pd.DataFrame({ 'X':['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o'], 'Y':[1,2,2,3,4,1,2,2,1,2,1,2,1,2,3] })

decompose = lambda x: [10**i for i,a in enumerate(str(x)[::-1]) for _ in range(int(a))]

df['Y'] = df['Y'].apply(decompose)
out = df.explode('Y')
out2 = out
print(out)
print(out2)"""

#------------
"""import datetime
x = datetime.datetime(2020, 5, 17)
print(x)
x2 = x.strftime("%Y-%m-%d 00:00:00")     #set date column format
print(x2)
x3 = x.strftime("%Y%m%d0000000")     #set string format


x3_int = int(x3)
print(x3_int, type(x3_int))
"""
#----------
"""import string
import random
letters = string.ascii_letters

one_letter = random.choice(letters)
fifty_random_letters_list1 = random.choices(letters, k=4)
fifty_random_letters_list2 = random.choices(letters, k=4)
fifty_random_letters_string = ''.join(fifty_random_letters_list1)+'-'+''.join(fifty_random_letters_list2)

print(fifty_random_letters_string)

"""
"""import numpy as np
import pandas as pd
day_list = ['12','43','9']
df = pd.DataFrame({
        'A':["str","s2tr","s5tr","st5r","st3r","strd"],
        'B':[4,5,4,5,5,4],
})

df['ProductID'] = np.random.choice(day_list, size=len(df))
print(df)
"""
#----------------
import pandas as pd

VAR_trans_Dictlist_final = 'Production.Product'
trans_Dictlist_final = 'D:\Docs\documents\python\projects\AWDBLoadPostgresWithSpark\Files\TransactionData\Production.Product\part-00000-78d1177d-ecc2-4a34-9339-539d5e9bbedb-c000.csv'

def trans_LoadCSVToDict(VAR_trans_Dictlist_final,VAR_IsList):
    
    #file_data = open(VAR_trans_Dictlist_final, "r")
    #trans_data = list(csv.reader(VAR_trans_Dictlist_final, delimiter=","))
    trans_getpandas = pd.read_csv(VAR_trans_Dictlist_final, header=None, skiprows=1,  dtype=str)   #skip header and set as string
    trans_getpandas_df = pd.DataFrame(trans_getpandas)
    if VAR_IsList == "1":
       trans_data =  trans_getpandas_df.values.tolist()
    else:
       trans_data =  trans_getpandas_df
    return trans_data       

rr = trans_LoadCSVToDict(trans_Dictlist_final,"0")
print(rr)
print(type(rr))

df = pd.DataFrame({
        'A':list('abcdef'),
        'B':[4,5,4,5,5,8],
})

import numpy as np
#day_list = pd.to_datetime(['2015-01-02','2016-05-05','2015-08-09'])
"""arr_set = pd.DataFrame([21,34,12,43,56,3,89])
print(type(arr_set.to_numpy()), arr_set.to_numpy())
arr_set2 = pd.to_numeric(list(arr_set))
print(type(arr_set2), arr_set2)
arr_data = pd.to_numeric([21,34,12,43,56,3,89])
"""
#arr_data = pd.to_numeric([21,34,12,43,56,3,89])
arr_data1 = pd.DataFrame([21,34,12,43,56,3,89])
print(type(arr_data1),arr_data1)
arr_data2 = np.array(arr_data1).reshape(-1)
print(type(arr_data2),arr_data2)
df["rand_day_test"] = np.random.choice(arr_data2,size=len(df))

#arr_data = pd.to_numeric(list(rr))
arr_data = np.array(rr).reshape(-1)
#arr_data = pd.to_numeric(rr)

#alternative
#day_list = pd.DatetimeIndex(['2015-01-02','2016-05-05','2015-08-09'])

#arr = arr_data.to_numpy()
print(type(arr_data), arr_data)
#print(type(rr), rr)
#df["rand_day"] = np.random.choice(rr.shape)
#df["rand_day"] = np.random.choice(day_list, size=len(df))
df["rand_day"] = np.random.choice(arr_data,size=len(df))
print (df)
