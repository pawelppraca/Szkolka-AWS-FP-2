# Python program using astype
# to convert a datatype of series
 
# importing pandas module  
import pandas as pd 
   
# reading csv file from url  
data = pd.read_csv("D:/Docs/documents/python/csv/various/nba.csv") 
    
# dropping null value columns to avoid errors 
data.dropna(inplace = True) 
   
# storing dtype before converting 
before = data.dtypes 
   
# converting dtypes using astype 
data["Salary"]= data["Salary"].astype(int) 
data["Number"]= data["Number"].astype(str) 
   
# storing dtype after converting 
after = data.dtypes 
   
# printing to compare 
print("BEFORE CONVERSION\n", before, "\n") 
print("AFTER CONVERSION\n", after, "\n")