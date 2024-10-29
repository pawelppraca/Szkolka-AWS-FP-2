import shutil #lib require to manage folders
from pathlib import Path
import os
import glob
from datetime import datetime
import csv

#method to remove non empty folder
def DeleteNonEmtpyFolder(Var_DeleteNEFolder):
    Delete_dirpath = Path(Var_DeleteNEFolder) 
    if Delete_dirpath.exists() and Delete_dirpath.is_dir():
        shutil.rmtree(Delete_dirpath)
        print(Var_DeleteNEFolder,' folder has been deleted')    
    else:
        print(Var_DeleteNEFolder," not exists")
#ends

#method to check if folder exists and creates
def FolderCheckIfExistsCreates(VAR_CreateFolder,VAR_SRC):
    if not os.path.exists(VAR_CreateFolder):
      os.makedirs(VAR_CreateFolder)
      print(VAR_SRC," ",VAR_CreateFolder," folder has been created")
    else:
      print(VAR_SRC," ",VAR_CreateFolder,"folder already exists")
#end

#method to check if folder exists
def FolderCheckIfExists(VAR_CheckFolder,VAR_Source):
    IsFolderExists = True
    if not os.path.exists(VAR_CheckFolder):
      print(VAR_Source," ",VAR_CheckFolder," folder does not exist")
      IsFolderExists = False
    return IsFolderExists
#end


#method to move csv file 
def FilesMoves(VAR_SourceFilesList,VAR_TargetFilesList,VAR_FileName):
    
    IsSourceFolderExists = FolderCheckIfExists(VAR_SourceFilesList,"Source")     #check if source folder exists, if not then skip move action
    if IsSourceFolderExists == True:
        file_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        ext = "/*.csv"                                                      #define what extension we are about to move
        fct = 1                                                             #set counter required for multiple files
        files_moves_list = glob.glob(VAR_SourceFilesList + ext)
        for file_row in files_moves_list:
            file_row_split = os.path.splitext(file_row)                     #split file name and set new name 
            
            #file_name_split = file_row_split[0]
            file_extension_split = file_row_split[1]
            file_new_base = VAR_FileName+"_"+file_stamp +"_"+str(fct) + file_extension_split
            targetfilename = os.path.join(VAR_TargetFilesList, file_new_base)
            #file_name = os.path.basename(file_row)
            shutil.move(file_row, targetfilename)
            print("Files have been momoved to: ",targetfilename)
            fct += 1                                                        #increase counter
    else:
       print("No file ",VAR_FileName," in source to load")
#end

#method to get list of table names from config file that we will loaded/extracted from/to DB
def CollectListOfTables(VAR_load_CSVList):
  tablelist_result = []
  with open(VAR_load_CSVList, 'r') as move_file:
      tablelist_dict = csv.DictReader(move_file)
      #print( [(row['table'], row['isenabled']) for row in tablelist_dict if row['isenabled'] == "1" ])
      for table_row in tablelist_dict:
          if table_row['isenabled'] == "1":
              #print(table_row['table'])
              tablelist_result.append(table_row['table'])
  move_file.close()
  return tablelist_result
#end

#method get files in folder
def FolderGetFiles(VAR_loadfolder_CSVList, VAR_tablefile):
    
    #set list where we will collect file names
    FolderFile_list = []
    FolderName = VAR_loadfolder_CSVList
    IsSourceFolderExists = FolderCheckIfExists(FolderName,"Load")     #check if folder exists, if not then skip move action
    if IsSourceFolderExists == True:
      for file_row in os.listdir(FolderName):
        #print(FolderName,"-",file_row)
        FolderFile_list.append(file_row)
        
    else:
      print(FolderName," not exists")
    
    #print(FolderFile_list)
    return FolderFile_list   
#end

#method to prepare list of columns to be dropped
def ColumnsToDropGetList(Var_ColumnTable,VAR_ColumnsToDrop_list):
    ColumnsToDrop_List = []
    
    for row_column in VAR_ColumnsToDrop_list:
      if row_column["name"] == Var_ColumnTable:
          print(row_column["column"])
          ColumnsToDrop_List.append(row_column["column"])
    
    return ColumnsToDrop_List[0] # to return only one square bracket

#method to split file name and extenstion
def FileSplitNameAndExt(VAR_FileSplit_source_schema,VAR_FileSplit_table_name):
   FileSplit_value = VAR_FileSplit_source_schema+"."+VAR_FileSplit_table_name.split('.')[1]
   return FileSplit_value