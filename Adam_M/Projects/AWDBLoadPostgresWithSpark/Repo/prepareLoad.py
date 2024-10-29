from ConfigData.configdata import * #require to import config file
from ConfigData.configfunctions import * #require to import config functions 


#move folder/file to load folder

#get a list of table names from config file that we will loaded from SQL --------------------------------
move_CSVList = FullFolderPath+"/Repo/ConfigData/"+FileExtractSQLToCSVList

tablelist_result_result = []
tablelist_result_result = CollectListOfTables(move_CSVList)

#check if target folder exists if not then creates otherwise move csv files
for createFolder_row in tablelist_result_result:
    FolderCheckIfExistsCreates(FullFolderPath+"/"+FolderLoadCSV+"/"+createFolder_row,"Target")
    FilesMoves(FullFolderPath+"/"+FolderExtractSQLToCSV+"/"+createFolder_row,FullFolderPath+"/"+FolderLoadCSV+"/"+createFolder_row,createFolder_row)




#print(tablelist_result)