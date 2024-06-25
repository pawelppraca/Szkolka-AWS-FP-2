import os

#script returns objects in folder defined in path

path = "D:/Docs/documents/python/csv/Sample_RetailDataAnalytics/small"
dir_list = os.listdir(path)
print("Files and directories in '", path, "' :")
# prints all files
print(dir_list)