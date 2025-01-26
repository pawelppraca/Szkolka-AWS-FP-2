# importing pandas module  
import pandas as pd  
     
# making data frame  
df = pd.read_csv("D:/Docs/documents/python/csv/various/nba.csv")  

#get top 10 items   
ser = pd.Series(df['Name']) 
data = ser.head(10)
print(data)


#-------------------------
#Now we access the element of series using index operator [ ] - betwen 3 and 6
print(data[3:6])

#output
#Name: Name, dtype: object
#3      R.J. Hunter
#4    Jonas Jerebko
#5     Amir Johnson
#Name: Name, dtype: object

#-----------------------
#Now we access the element of series using .loc[] function.
print(data.loc[3:6])