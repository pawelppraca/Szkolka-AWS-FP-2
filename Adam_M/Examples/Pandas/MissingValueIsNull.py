
#Checking for missing values using isnull()

# importing pandas as pd
import pandas as pd
 
# importing numpy as np
import numpy as np
 
# dictionary of lists
dict = {'First Score':[100, 90, np.nan, 95],
        'Second Score': [30, 45, 56, np.nan],
        'Third Score':[np.nan, 40, 80, 98]}
 
# creating a dataframe from list
df = pd.DataFrame(dict)
 
# using isnull() function  
print(df.isnull())

#output
#   First Score  Second Score  Third Score
#0        False         False         True
#1        False         False        False
#2         True         False        False
#3        False          True        False