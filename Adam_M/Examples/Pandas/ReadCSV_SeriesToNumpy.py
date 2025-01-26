# importing pandas 
import pandas as pd  
  
# reading the csv   
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv")  
     
data.dropna(inplace = True) 
  
# creating series form weight column 
gfg = pd.Series(data['Weight'].head()) 
print(gfg)

# using to_numpy() function 
print(type(gfg.to_numpy())) 

# providing dtype 
print(gfg.to_numpy(dtype ='float32')) 