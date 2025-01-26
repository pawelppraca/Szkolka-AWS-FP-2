#Convert DataFrame to Numpy Array

import pandas as pd
 
# initialize a dataframe
df = pd.DataFrame(
    [[1, 2, 3],
     [4, 5, 6],
     [7, 8, 9],
     [10, 11, 12]],
    columns=['a', 'b', 'c'])
 
#df
print(df)
# convert dataframe to numpy array
arr = df.to_numpy()
 
print('\nNumpy Array\n----------\n', arr)
print(type(arr))

#----------------------------
#convert particular column into numpy array
# convert dataframe to numpy array
arr2 = df[['a', 'c']].to_numpy()
 
print('\nNumpy Array\n----------\n', arr2)
print(type(arr2))
print('Numpy Array Datatype :', arr2.dtype)