import csv

#script to open csv file and write to new file. this can be used if we want to make adjustment in file and store it as a new one

#declare source/target file
Source_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/People.csv'
Target_csv_file_path = 'D:/Docs/documents/python/csv/AddnewColumnCSV/People_upd.csv'

#open, read file and get the data
with open(Source_csv_file_path,'r') as readFile:
    reader = csv.reader(readFile)
    lines = list(reader) #to store data as a list

#open and 
with open(Target_csv_file_path,'w',newline='') as writeFile: #newline adds new line every each line
    writer = csv.writer(writeFile)
    writer.writerows(lines)
    
readFile.close()
writeFile.close()

