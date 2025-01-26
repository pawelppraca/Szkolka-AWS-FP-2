# importing pandas
import pandas as pd
 
# reading the csv
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv")
 
data.dropna(inplace=True)
 
# creating DataFrame from weight column
df = pd.DataFrame(data['Weight'].head())
 
# using to_numpy() function
print(df.to_numpy())

# Validating the type of the array 
print(type(df.to_numpy()))