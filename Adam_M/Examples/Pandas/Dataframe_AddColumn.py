

#Adding New Column to Existing DataFrame in Pandas
#by passing address list as parameter
import pandas as pd

# Define a dictionary containing Students data
data = {'Name': ['Pandas', 'Geeks', 'for', 'Geeks'],
        'Height': [1, 2, 3, 4],
        'Qualification': ['A', 'B', 'C', 'D']}

# Convert the dictionary into DataFrame
df = pd.DataFrame(data)

# using assign() and passing address list as parameter
df2 = df.assign(address = ['NewYork', 'Chicago', 'Boston', 'Miami'])

print(df2)
print("\n")
#------------------------
#By passing Dictionary
data2 = {'Name': ['Pandas', 'Geeks', 'for', 'Geeks'],
        'Height': [1, 2, 3, 4],
        'Qualification': ['A', 'B', 'C', 'D']}

# Convert the dictionary into DataFrame
df3 = pd.DataFrame(data2)
# address dictionary with names as keys & addresses as values
address2 = {'Pandas': 'NewYork', 'Geeks': 'Chicago', 
            'for': 'Boston', 'Geeks_2': 'Miami'}

# Add the 'Address' column by mapping the 'Name' column
# to the address dictionary
df3['Address'] = df3['Name'].map(address2)

print(df3)