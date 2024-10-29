#extract data from SQL server into CSV files

from ConfigData.configdata import * #require to import config file
from ConfigData.sqlfunctions import * #require to import sql functions file
from ConfigData.configfunctions import * #require to import config functions 

from pyspark.sql import SparkSession,functions as O
from pyspark import SparkContext, SparkConf, SQLContext

import json # require for columns to drop list

#define spark context
appName = "PySpark SQL Server via JDBC"
master = "local"
conf = SparkConf() \
            .setAppName(appName) \
            .setMaster(master) \
            .set("spark.driver.extraClassPath", "Repo/Jars/mssql-jdbc-12.6.3.jre8.jar") \
            .set("spark.sql.debug.maxToStringFields", 1000) \
            .set("spark.executor.heartbeatInterval", 200000) \
            .set("spark.network.timeout", 300000) \
            .set("spark.driver.cores", "2") \
            .set("spark.executor.cores", "2") \
            .set("spark.driver.memory", "1G")  \
            .set("spark.executor.memory", "1G")  \
            .set("spark.executor.instances", "2") 
            #.set("spark.sql.execution.arrow.pyspark.enabled", "true") \
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession


#get a list of table names from config file that we will loaded from SQL --------------------------------
extract_FileExtractSQLToCSVList = FullFolderPath+"/Repo/ConfigData/"+FileExtractSQLToCSVList
#print(extract_FileExtractSQLToCSVList)
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(extract_FileExtractSQLToCSVList)

#filter and return only tables that are marked as enabled
df2_list = df.select("table","column").sort(O.desc("order")).filter(df.isenabled == '1')
df2_list.show()

#get list of columns to be dropped
extract_ColumnsToDrop = FullFolderPath+"/Repo/ConfigData/"+FileListOfColumnsToBeDropped
extract_ColumnsToDrop_open = open(extract_ColumnsToDrop)
extract_ColumnsToDrop_list = json.load(extract_ColumnsToDrop_open)

#remove old folders
for list_folder in df2_list.collect():
    DeleteNonEmtpyFolder(FullFolderPath+"/"+FolderExtractSQLToCSV+"/"+list_folder["table"])       #remove old folder if exists

#get max value from last completed load
df_getSQLMaxCP = SQLServerGetListOfTableCheckPoint(spark)

#Extract data from SQL to CSV   ------------------------------------------------------------------------
#SQLServerToCSV("Sales.SalesOrderHeader","ModifiedDate",FullFolderPath)
for list_item in df2_list.collect():
    ColumnsToDropGetList_result = ColumnsToDropGetList(list_item["table"],extract_ColumnsToDrop_list) #set list of columns to drop
    SQLServerToCSV(spark, list_item["table"],list_item["column"],FullFolderPath, df_getSQLMaxCP,ColumnsToDropGetList_result)
    #print(list_item["table"],list_item["column"])


spark.stop()


