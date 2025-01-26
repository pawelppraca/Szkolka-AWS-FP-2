



# importing pandas module 
import pandas as pd 
  
# making data frame 
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv") 
  
# calling head() method  
# storing in new variable 
data_top = data.head() 
  
# display 
print(data_top)

#------------
#bottom 
data_bottom = data.tail() 
print(data_bottom)

#--------------
#tail with n param
# number of rows to return 
n = 3
# creating series 
series = data["Salary"] 
# returning top n rows 
bottom = series.tail(n = n) 
print(bottom)