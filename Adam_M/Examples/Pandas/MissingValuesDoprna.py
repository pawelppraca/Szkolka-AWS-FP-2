# importing pandas as pd
import pandas as pd
 
# importing numpy as np
import numpy as np
 
# dictionary of lists
dict = {'First Score':[100, 90, np.nan, 95],
        'Second Score': [30, np.nan, 45, 56],
        'Third Score':[52, 40, 80, 98],
        'Fourth Score':[np.nan, np.nan, np.nan, 65],
        '5h Score':[100, 90, 34, 23]}
 
# creating a dataframe from dictionary
df = pd.DataFrame(dict)
 
#Now we drop rows with at least one Nan value (Null value). 
# using dropna() function  
print(df.dropna())

#output
#   First Score  Second Score  Third Score  Fourth Score
#3         95.0          56.0           98          65.0