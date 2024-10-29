from ConfigData.configdata import * #require to import config file
from ConfigData.transaction_functions import * #require to import sql functions file

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark import SparkContext, SparkConf, SQLContext

import json # require for columns to drop list

"""#define spark 
masterset = "local"
# Create Spark Session
spark = SparkSession.builder\
    .master(masterset)\
    .appName("Extract Dict from SQL Server")\
    .config("spark.debug.maxToStringFields", 1000)\
    .config("spark.driver.extraClassPath", "Repo/Jars/mssql-jdbc-12.6.3.jre8.jar") \                                    
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config("spark.executor.heartbeatInterval", 200000) \
    .config("spark.network.timeout", 300000) \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1G")  \
    .config("spark.executor.memory", "1G")  \
    .config("spark.executor.instances", "2") \
    .config("spark.debug.maxToStringFields", 1000)\
    .getOrCreate()
"""
if __name__=='__main__':

    conf = SparkConf().setMaster("local[*]").set("spark.sql.debug.maxToStringFields", 1000) \
        .set("spark.executor.heartbeatInterval", 200000) \
        .set("spark.network.timeout", 300000) \
        .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .set("spark.driver.extraClassPath", "Repo/Jars/mssql-jdbc-12.6.3.jre8.jar") \
        .set("spark.ui.port", 4040) \
        .set("spark.driver.cores", "2") \
        .set("spark.executor.cores", "2") \
        .set("spark.driver.memory", "1G")  \
        .set("spark.executor.memory", "1G")  \
        .set("spark.executor.instances", "2") \
        .setAppName("Extract Dict from SQL Server")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")


    #get a list of table names from config file that we will loaded from SQL --------------------------------
    transaction_FileExtractSQLToCSVList = FullFolderPath+"/Repo/ConfigData/"+Trans_FileExtractDictToCSVList
    #print(extract_FileExtractSQLToCSVList)
    trans_extract_list_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(transaction_FileExtractSQLToCSVList)

    #filter and return only tables that are marked as enabled
    trans_extract_list_df_ready = trans_extract_list_df.select("table","column").filter(trans_extract_list_df.isenabled == '1').sort(desc("order"))
    trans_extract_list_df_ready.show()

    for trans_list_item in trans_extract_list_df_ready.collect():
        trans_GetDictFromSQLServer(spark,trans_list_item["table"],trans_list_item["column"])
        print("go : ",trans_list_item["table"])


    spark.stop()    