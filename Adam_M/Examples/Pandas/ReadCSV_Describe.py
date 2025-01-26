

#We can easily learn about several statistical measures, including mean, median, standard deviation, quartiles, and more, by using describe() on a DataFrame.

import pandas as pd
# reading and printing csv file
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv")
print(data.head())


print(data.describe())