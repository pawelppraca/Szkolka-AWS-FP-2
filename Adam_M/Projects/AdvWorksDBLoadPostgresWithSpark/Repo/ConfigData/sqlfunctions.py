
from ConfigData.configdata import * #require to import config file
from ConfigData.configfunctions import * #require to import config functions 
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
import psycopg2 # required for PostgreSQL

#start SQLServerToCSV-------------------------------------
def SQLServerToCSV(spark, Var_Table,Var_Column,Var_FullFolderPath,VAR_getSQLMax,VAR_ColumnsToDropGetList):
#method gets data from SQL server and puts into csv file

    #define connection 
    conc_database = SQL_source_database
    conc_table = Var_Table
    conc_user = SQL_source_user
    conc_password  = SQL_source_password
    conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={conc_database};encrypt=true;trustServerCertificate=true;"
    conc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    conc_numPartitions = 5
    conc_column = Var_Column
    conc_fetchsize = 3000
    conc_batchsize = 3000

    print(f"iteration {conc_table}-----------------------------------")

    #check if maxValue DF is empty, if yes set default, otherwise get MaxValue from dataframe and set for particular table
    if VAR_getSQLMax.rdd.isEmpty() == True:
        lower_bound_s = default_value(None,'datetime')
    else:
        lower_bound_q = VAR_getSQLMax.select(VAR_getSQLMax.MaxColumnValue).filter(VAR_getSQLMax.LoadedFileName==conc_table) 
        if lower_bound_q.rdd.isEmpty() == True: #check again as if init load df is not empty in 1st iteration
            lower_bound_s = default_value(None,'datetime')
        else:
            lower_bound_q = lower_bound_q.collect()
            lower_bound_s = lower_bound_q[0].__getitem__('MaxColumnValue') #get 1st row to value for corresponding MaxColumnValue


    #get max of last extracted data
    """    
    SQLServerToCSV_maxValue_query = f"(SELECT MAX({conc_column}) AS row_num FROM {conc_table}) as my_table"
    SQLServerToCSV_bound_df = spark.read \
    .format("jdbc") \
    .option("url", conc_jdbc_url) \
    .option("dbtable", SQLServerToCSV_maxValue_query) \
    .option("driver", conc_driver) \
    .option("user", conc_user) \
    .option("password", conc_password) \
    .load()
    """
    #set boundary lower and upper
    lower_bound = lower_bound_s
    upper_bound = "9999-12-31 00:00:00"
 
    print("low and up boundary : ",lower_bound,upper_bound)
    
    main_query = f"(SELECT *  FROM {conc_table}  WHERE {conc_column} > '{lower_bound}') as q"
    SQLServerToCSV_df = spark.read \
    .format("jdbc") \
    .option("url",conc_jdbc_url) \
    .option("driver", conc_driver) \
    .option("dbtable", main_query) \
    .option("partitionColumn", conc_column) \
    .option("numPartitions", conc_numPartitions) \
    .option("lowerBound", str(lower_bound)) \
    .option("upperBound", str(upper_bound)) \
    .option("user", conc_user) \
    .option("password", conc_password) \
    .load()
#    .option("fetchsize", conc_fetchsize) \

    #check if dateframe is empty, in case there is nothing to extract
    if SQLServerToCSV_df.rdd.isEmpty() == True:
        print("Nothing to extract for ",conc_table)
    else:
        #getmax value from extracted data which we will put into csv file - dataframe to one value

        SQLServerToCSV_MaxExtractedValue =  SQLServerToCSV_df.agg(max('ModifiedDate')).collect()[0][0]

        #remove column section that we don't want to export
        SQLServerToCSV_df_final = SQLServerToCSV_df.drop(*VAR_ColumnsToDropGetList)
        
        print(conc_table," Max value: ",SQLServerToCSV_MaxExtractedValue)
        SQLServerToCSV_path = Var_FullFolderPath+"/"+FolderExtractSQLToCSV+"/"+conc_table

        SQLServerToCSV_df_final.show(2)

        #SQLServerToCSV_df = SQLServerToCSV_df.repartition(2)
        SQLServerToCSV_df_final.write.option("header",True).option("delimiter",",").format("csv").mode("overwrite").save(SQLServerToCSV_path)
        #SQLServerToCSV_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(SQLServerToCSV_path)

        #method - mark as table load completed from  SQL Server table in checkpoint table
        SQLServerSetCheckPoint(spark,Var_Table,Var_Column, SQLServerToCSV_MaxExtractedValue)

        print("Data was extracted for ",conc_table)
    
#end SQLServerToCSV ---------------------------------------------

#start SQLServerGetListOfTableCheckPoint-------------------------
#method return max value for loads that completed from Checkpoint
def SQLServerGetListOfTableCheckPoint(spark):    
    conc_database = SQL_source_database
    conc_user = SQL_source_user
    conc_password  = SQL_source_password
    conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={conc_database};encrypt=true;trustServerCertificate=true;"
    conc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    

    #get max of last extracted data fromm SQL server
    SQLServerToCSV_getListCP_query = "(select max(MaxValue) as MaxColumnValue, [LoadedFileName] FROM dbo.[CheckPointSet] WHERE IsCompleted=1 group by [LoadedFileName] ) as my_table"
    SQLServerToCSV_getListCP_df = spark.read \
    .format("jdbc") \
    .option("url", conc_jdbc_url) \
    .option("dbtable", SQLServerToCSV_getListCP_query) \
    .option("driver", conc_driver) \
    .option("user", conc_user) \
    .option("password", conc_password) \
    .load()

    return(SQLServerToCSV_getListCP_df)


#end SQLServerGetListOfTableCheckPoint --------------------------

#start SQLServerGetCheckPoint------------------------------------
#method execute sproc to insert new row into checkpoint which will represent new load
def SQLServerSetCheckPoint(spark, Var_Table,Var_Column, MaxValue):
    
    """    
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    connection = driver_manager.getConnection(conc_jdbc_url, SQL_source_user, SQL_source_password)
    res = connection.prepareCall(f" declare @rowCount int; EXEC dbo.SetCheckpoint @FileName = '{Var_Table}' ").execute()
    connection.close()
    """
    conc_database = SQL_source_database
    conc_user = SQL_source_user
    conc_password  = SQL_source_password
    conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={conc_database};encrypt=true;trustServerCertificate=true;"
    conc_driver = SQL_source_driver

    #prepare row to insert as DF
    SetCP_data_current_time = datetime.datetime.now()
    SetCP_Columns = ["LoadedFileName", "MaxValue","IsCompleted","DateStart"]
    SetCP_data = [(Var_Table, MaxValue, 1, SetCP_data_current_time)]
    #SetCP_data = [(Var_Table, "0", 0, SetCP_data_current_time)]
    SetCP_df = spark.createDataFrame(SetCP_data, SetCP_Columns)


    SetCP_df.write.mode("append") \
            .format("jdbc") \
            .option("driver", conc_driver) \
            .option("url", conc_jdbc_url) \
            .option("dbtable", "dbo.CheckPointSet") \
            .option("user", conc_user) \
            .option("password", conc_password) \
            .save()
    
#end SQLServerGetCheckPoint ------------------------------------------   

#method to set default value
def default_value(getvalue,datatype):
    from datetime import datetime

    if getvalue is None:
        match datatype:
            case 'datetime':
                return datetime(day = 1, month = 1, year = 1900, minute = 0, hour = 0)
            case _:
                return 0 # 0 is default if 
    else:
        return getvalue

#---------------------------- PostgreSQL load
def CSVToPostgreSQL(spark, VAR_table_name,VAR_FullFolderPath,VAR_logger):
    
    #set file path
    CSV_FolderName = VAR_FullFolderPath+"/"+VAR_table_name
    #set DB connection
    load_database = PGSQL_source_database
    load_user = PGSQL_source_user
    load_password  = PGSQL_source_password
    load_jdbc_url = f"jdbc:postgresql://localhost:5432/{load_database}"
    load_driver = PGSQL_source_driver

    #call method to set file name - split without schema 
    load_file_name_split = FileSplitNameAndExt(PGSQL_source_schema,VAR_table_name)                    

    #check files to be loaded from load folder and prepare load list
    load_tablelist_result_final = FolderGetFiles(CSV_FolderName, VAR_table_name) 

    #clear data in table
    LoadTruncateStagingTables(load_tablelist_result_final,load_file_name_split,VAR_logger)
    
    #return list of files and load into PostgreSQL
    for item_toLoadPostgreSQL in  load_tablelist_result_final:
        CSVToPostgreSQL_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load(CSV_FolderName+"/"+item_toLoadPostgreSQL) \
        .withColumn("LoadFileName", lit(item_toLoadPostgreSQL))                 #adding column which indicates file name

        #CSVToPostgreSQL_df.show(2)
        
        rowscnt = CSVToPostgreSQL_df.count()
        #print("row cnt: ",rowscnt)

        CSVToPostgreSQL_df.write \
        .format("jdbc") \
        .option("url", load_jdbc_url) \
        .option("dbtable", load_file_name_split) \
        .option("user", load_user) \
        .option("password", load_password) \
        .option("driver",load_driver) \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
        VAR_logger.info("File : "+item_toLoadPostgreSQL+" loaded into: "+load_file_name_split)

        #mark file as loaded into stg PostgreSQL    
        CSVToPostgreSQLMarkAsLoaded(item_toLoadPostgreSQL, VAR_logger)                                      

        #clear cache
        spark.catalog.clearCache()
         # unpersist the DataFrame from memory
        CSVToPostgreSQL_df.unpersist()

#--end CSVToPostgreSQL        


#---------------------------- PostgreSQL clear stg
def LoadTruncateStagingTables(VAR_load_tablelist_result,VAR_LoadTruncate_table_name,VAR_logger):

    LoadTruncate_list = []                                      #prepare list to build truncate statement
    for loadtruncate_row in VAR_load_tablelist_result:
        LoadTruncate_list.append("'"+loadtruncate_row+"',")    #split file name and set with stg schema into list
    LoadTruncate_final = "".join(LoadTruncate_list)
    LoadTruncate_final = LoadTruncate_final.rstrip(',')    #convert list to string   
    LoadTruncate_final = f"DELETE FROM {VAR_LoadTruncate_table_name} where LoadFileName in ( {LoadTruncate_final} )"+";"

    LoadTruncate_conn = PostgreSQLPsycopg2Connection()           #get connection
    LoadTruncate_cursor = LoadTruncate_conn.cursor()
    delete_tbl_data = LoadTruncate_final
    LoadTruncate_cursor.execute(delete_tbl_data)
    LoadTruncate_conn.commit()
    LoadTruncate_cursor.close()
    LoadTruncate_conn.close()
    VAR_logger.info("Cleared up staging table: "+VAR_LoadTruncate_table_name)
    
#--end-- PostgreSQL clear stg

#---------------------- CSVToPostgreSQLMarkAsLoaded mark load as completed into stg in config table
def CSVToPostgreSQLMarkAsLoaded(MarkAsLoadedVAR_filename, VAR_logger):
    CSVToPostgreSQLMarkAsLoaded_conn = PostgreSQLPsycopg2Connection()           #get connection
    CSVToPostgreSQLMarkAsLoaded_cursor = CSVToPostgreSQLMarkAsLoaded_conn.cursor()
    MarkAsLoaded_stmt = f"call stg.LoadFileMark('{MarkAsLoadedVAR_filename}');"
    CSVToPostgreSQLMarkAsLoaded_cursor.execute(MarkAsLoaded_stmt)
    CSVToPostgreSQLMarkAsLoaded_conn.commit()
    CSVToPostgreSQLMarkAsLoaded_cursor.close()
    CSVToPostgreSQLMarkAsLoaded_conn.close()
    VAR_logger.info("File: "+MarkAsLoadedVAR_filename+" has been marked as loaded into stg")
#--end-- CSVToPostgreSQLMarkAsLoaded

#----------------------PostgreSQLPsycopg2Connection PostgreSQL set psycopg2 connection
def PostgreSQLPsycopg2Connection():
    PPC_database = PGSQL_source_database 
    PPC_user = PGSQL_source_user 
    PPC_pass = PGSQL_source_password 
    PPC_conn = psycopg2.connect(f"dbname={PPC_database} user={PPC_user} password={PPC_pass}")
    return PPC_conn
#--end-- PostgreSQLPsycopg2Connection