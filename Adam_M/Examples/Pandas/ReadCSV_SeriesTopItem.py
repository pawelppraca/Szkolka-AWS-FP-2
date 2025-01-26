import pandas as pd 
  
# making data frame 
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv") 
  
# number of rows to return 
n = 9
  
# creating series 
series = data["Name"] 
  
# returning top n rows 
top = series.head(n = n) 
  
# display 
print(top) 