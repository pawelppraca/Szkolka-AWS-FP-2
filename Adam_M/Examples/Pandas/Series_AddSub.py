# importing pandas module  
import pandas as pd  
 
# creating a series
data = pd.Series([5, 2, 3,7], index=['a', 'b', 'c', 'd'])
 
# creating a series
data1 = pd.Series([1, 6, 4, 9], index=['a', 'b', 'd', 'e'])
 
print(data, "\n\n", data1)

#output
#a    5
#b    2
#c    3
#d    7

# a    1
#b    6
#d    4
#e    9

#-----------
#Now we add two series using .add() function.

print("\n",data.add(data1, fill_value=0))

#output
#a     6.0
#b     8.0
#c     3.0
#d    11.0
#e     9.0

#------------
# subtracting two series using
# .sub
print(data.sub(data1, fill_value=0))

#output
#a    4.0
#b   -4.0
#c    3.0
#d    3.0
#e   -9.0