import pandas as pd

#script to read csv file and add new column with same value and store as a new files
 
# Step 1: Read the CSV file into a DataFrame
Source_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/People.csv'
Target_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/People_upd.csv'
df = pd.read_csv(Source_csv_file_path)
 
# Step 2: Define the values for the new "City" column
#new_city_values = ['New York', 'Los Angeles', 'Chicago'] #with different values, but number of items has to match
new_city_values = 'Chicago' #set default value

# Step 3: Add the new "City" column to the DataFrame
df['City'] = new_city_values
 
# Step 4: Write the DataFrame back to the CSV file
df.to_csv(Target_csv_file_path, index=False)