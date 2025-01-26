#Accessing element of Series
# import pandas and numpy 
import pandas as pd
import numpy as np
 
# creating simple array
data = np.array(['g','e','e','k','s','f', 'o','r','g','e','e','k','s'])
ser = pd.Series(data)
  
  
#retrieve the first 5 element
print(ser[:5])


#------------------------
#-Accessing Element Using Label (index) :
#In order to access an element from series, we have to set values by index label. A Series is like a fixed-size dictionary in that you can get and set values by index label.
#Accessing a single element using index label.

 
# creating simple array
data2 = np.array(['g','e','e','k','s','f', 'o','r','g','e','e','k','s'])
ser2 = pd.Series(data2,index=[10,11,12,13,14,15,16,17,18,19,20,21,22])
  
  
# accessing a element using index element
print(ser2[16])

