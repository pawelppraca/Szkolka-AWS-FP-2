# importing pandas module
import pandas as pd
 
# making data frame from csv file
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv", index_col ="Name" )
 
# dropping passed columns
data.drop(["Team", "Weight"], axis = 1, inplace = True)
 
# display
print(data)


#------------
# dropping passed values - row
data.drop(["Avery Bradley", "John Holland", "R.J. Hunter",
                            "R.J. Hunter"], inplace = True)
 
# display
print(data)
