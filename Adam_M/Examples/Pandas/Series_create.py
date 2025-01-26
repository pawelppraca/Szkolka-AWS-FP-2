import pandas as pd 
import numpy as np

# Creating empty series 
ser = pd.Series() 
print("Pandas Series: ", ser) 

# simple array 
data = np.array(['g', 'e', 'e', 'k', 's']) 
  
ser = pd.Series(data) 
print("Pandas Series:\n", ser)