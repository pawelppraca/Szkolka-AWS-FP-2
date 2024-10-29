
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
Temp_var = "Stg.SalesOrderHeader"
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

rand_unique(30,3)