# importing pandas package
import pandas as pd
 
# making data frame from csv file
data = pd.read_csv('D:/Docs/documents/python/csv/various/nba.csv', index_col ="Name")
print("\n")

# retrieving row by loc method
first = data.loc["Avery Bradley"]
second = data.loc["R.J. Hunter"]
 
 
print(first, "\n\n\n", second)

# retrieving rows by iloc method, 4th line 
row2 = data.iloc[4] 

print("\n\n\n", row2)