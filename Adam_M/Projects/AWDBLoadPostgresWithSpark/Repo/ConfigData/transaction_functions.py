from ConfigData.configdata import * #require to import config file
from pyspark.sql.functions import *
import os, glob, csv
import random
import pandas as pd

def trans_GetDictFromSQLServer(spark,VAR_GetDict_table, VAR_GetDict_column):
    conc_database = SQL_source_database
    conc_table = VAR_GetDict_table           
    GetDict_ColName =  VAR_GetDict_column            # change
    conc_user = SQL_source_user
    conc_password  = SQL_source_password
    conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={conc_database};encrypt=true;trustServerCertificate=true;"
    conc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    #conc_numPartitions = 5
    #conc_column = Var_Column
    conc_fetchsize = 3000
    conc_batchsize = 3000

    #get data from table
    getDict_query = f"(SELECT {GetDict_ColName}  FROM {conc_table} ) as q"
    getDict_ExtractFromSQLServer_df = spark.read \
    .format("jdbc") \
    .option("url",conc_jdbc_url) \
    .option("driver", conc_driver) \
    .option("dbtable", getDict_query) \
    .option("user", conc_user) \
    .option("password", conc_password) \
    .load()

    #getDict_ExtractFromSQLServer_df.show(2)
    print(getDict_ExtractFromSQLServer_df.count())

    getDict_ExtractFromSQLServer_df = getDict_ExtractFromSQLServer_df.repartition(1)
    #store in file
    SQLServerToCSV_path = FullFolderPath+"/"+Trans_FolderExtractDictTransToCSV+"/"+conc_table
    getDict_ExtractFromSQLServer_df.write.option("header",True).option("delimiter",",").format("csv").mode("overwrite").save(SQLServerToCSV_path)
    
#method to check if folder exists
def trans_FolderCheckIfExists(VAR_CheckFolder,trans_VAR_logger):
    trans_IsFolderExists = True
    if not os.path.exists(VAR_CheckFolder):
      trans_VAR_logger.info(VAR_CheckFolder," folder does not exist")
      trans_IsFolderExists = False
    return trans_IsFolderExists
#end

def trans_FolderGetFiles(VAR_trans_loadfolder_CSVList, trans_VAR_logger):
    
    #set list where we will collect file names
    trans_FolderFile_list = []
    FolderName = VAR_trans_loadfolder_CSVList
    IsSourceFolderExists = trans_FolderCheckIfExists(FolderName,trans_VAR_logger)     #check if folder exists, if not then skip move action
    if IsSourceFolderExists == True:
      for file_row in os.listdir(FolderName):
        if file_row.endswith(".csv"):                                                 #filter only csv files
            #trans_VAR_logger.info(file_row+" exists")
            trans_FolderFile_list.append(file_row)
        
    else:
      trans_VAR_logger.info(FolderName+" not exists")
    
    #print(FolderFile_list)
    return trans_FolderFile_list   
#end


#method to load dict file
def trans_GetDictFiles(spark,VAR_trans_row_getDict,VAR_trans_logger):

    DictFiles_path = FullFolderPath+"/"+Trans_FolderExtractDictTransToCSV+"/"+VAR_trans_row_getDict
    #VAR_trans_logger.info("folder: "+DictFiles_path)
    load_DictFiles_result = trans_FolderGetFiles(DictFiles_path,VAR_trans_logger)           #check if file exist
    #return list of files
    DictFiles_path_list = []
    for item_toLoadPostgreSQL in  load_DictFiles_result:
        """CSVToPostgreSQL_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load(DictFiles_path+"/"+item_toLoadPostgreSQL) \
        .withColumn("SourceTable", lit(VAR_trans_row_getDict))                             #adding column which indicates tbl"""
        DictFiles_path_final = DictFiles_path+"/"+item_toLoadPostgreSQL
        DictFiles_path_list.append(DictFiles_path_final)
        
    return DictFiles_path_list #CSVToPostgreSQL_df
#end

#method to split file name and extenstion
def trans_ReturnFileNameNoExt(VAR_trans_row_getDict):
   FileSplit_value = VAR_trans_row_getDict.split('.')[1]
   return FileSplit_value
#end

#method to open csv file into dict
def trans_LoadCSVToDict(VAR_trans_Dictlist_final):
    
    #file_data = open(VAR_trans_Dictlist_final, "r")
    #trans_data = list(csv.reader(VAR_trans_Dictlist_final, delimiter=","))
    trans_getpandas = pd.read_csv(VAR_trans_Dictlist_final, header=None, delimiter=",")   #skip header
    trans_getpandas_df = pd.DataFrame(trans_getpandas)
    trans_data =  trans_getpandas_df.values.tolist()
    #file_data.close()
    return trans_data

#method to generate new random data for dictionaries
"""def trans_GenRandomData(VAR_trans_GenRandomData):
   VAR_trans_GenRandomData_result = random.choices(VAR_trans_GenRandomData, k=1)
   VAR_trans_GenRandomData_result_str = (', '.join(VAR_trans_GenRandomData_result[0]))    #convert list element to string
   return VAR_trans_GenRandomData_result_str"""