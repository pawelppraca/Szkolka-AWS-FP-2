from ConfigData.configdata import * #require to import config file
from pyspark.sql.functions import *
import os, glob, csv
import random
import pandas as pd
import psycopg2

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
    getDict_query = f"(SELECT cast({GetDict_ColName} as varchar(20)) as {GetDict_ColName} FROM {conc_table} ) as q"
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

#method to open csv file and put into dict
def trans_LoadCSVToDict(VAR_trans_Dictlist_final,VAR_IsList):
    
    #file_data = open(VAR_trans_Dictlist_final, "r")
    #trans_data = list(csv.reader(VAR_trans_Dictlist_final, delimiter=","))
    trans_getpandas = pd.read_csv(VAR_trans_Dictlist_final, header=0, dtype=str)   #skip header and set as string
    trans_getpandas_df = pd.DataFrame(trans_getpandas)
    if VAR_IsList == "1":
       trans_data =  trans_getpandas_df.values.tolist()
    else:
       trans_data =  trans_getpandas_df
    #file_data.close()
    return trans_data

def trans_GenDataToSQLServer(spark,VAR_df_header_final, VAR_GetData_column):
    GenData_database = SQL_source_database
    GenData_table = VAR_GetData_column           
    #GetDict_ColName =  VAR_GetDict_column            # change
    GenData_user = SQL_source_user
    GenData_password  = SQL_source_password
    GenData_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={GenData_database};encrypt=true;trustServerCertificate=true;"
    GenData_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    #conc_numPartitions = 5

    #save data into table
    VAR_df_header_final.write \
        .format("jdbc") \
        .option("url", GenData_jdbc_url) \
        .option("dbtable", GenData_table) \
        .option("user", GenData_user) \
        .option("password", GenData_password) \
        .option("driver",GenData_driver) \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    
#---------------------------- SQL clear stg
def trans_TruncateGenTables(VAR_trans_tablelist,VAR_trans_trans_logger,spark):

    trans_connection = trans_SQLPsycopg2Connection(spark)

    for loadtruncate_row in VAR_trans_tablelist:
       #LoadTruncate_final = f"DELETE FROM {loadtruncate_row} ;"
       trans_connection.prepareCall(f" DELETE FROM {loadtruncate_row} ").execute()

    trans_connection.close()
    VAR_trans_trans_logger.info("Cleared up gen table ")
    
#--end-- SQL clear stg

#----------------------SQLPsycopg2Connection SQL set psycopg2 connection
def trans_SQLPsycopg2Connection(spark):
    trans_SQL_database = SQL_source_database 
    trans_SQL_user = SQL_source_user 
    trans_SQL_pass = SQL_source_password 
    #PPC_conn = psycopg2.connect(f"dbname={PPC_database} user={PPC_user} password={PPC_pass}")
    
    trans_driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    trans_conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={trans_SQL_database};encrypt=true;trustServerCertificate=true;"
    trans_connection = trans_driver_manager.getConnection(trans_conc_jdbc_url, trans_SQL_user, trans_SQL_pass)
    
    return trans_connection
#--end-- SQLPsycopg2Connection    

#---------------------------- SQL merge data into dbo
def trans_MergeGenToDbo(spark):

    trans_connection = trans_SQLPsycopg2Connection(spark)

    trans_connection.prepareCall(f"EXEC gen.Merge_SalesOrder ").execute()

    trans_connection.close()
    
#--end-- SQL merge data into dbo
