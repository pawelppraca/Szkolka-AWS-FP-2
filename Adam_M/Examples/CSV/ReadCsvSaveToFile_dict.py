import csv


#script reads csv file and puts into dictionary(dict) and next print content and wrote into new file
 
#declare source/target file
Source_csv_filename = 'People.csv'
Target_csv_filename = 'People_upd.csv'

#declare source/target folder
Source_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/'+Source_csv_filename
Target_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/'+Target_csv_filename


# Open the CSV file for reading
with open(Source_csv_file_path, mode='r') as fileRead:
    # Create a CSV reader with DictReader
    csv_reader = csv.DictReader(fileRead)
 
    # Initialize an empty list to store the dictionaries
    data_list = []
    
    # Iterate through each row in the CSV file
    for row in csv_reader:
        # Append each row (as a dictionary) to the list
        data_list.append(row)


# Print the list of dictionaries
for data in data_list:
    print(data)

with open(Target_csv_file_path, 'w', newline='') as fileSave:
    writer = csv.DictWriter(fileSave, fieldnames = data_list[0]) #define and set header from 1st row
    writer.writeheader() #set and write header
    writer.writerows(data_list) #set and write data

#close files
fileRead.close()
fileSave.close()    
