from ConfigData.configdata import * #require to import config file
from ConfigData.transaction_functions import * #require to import sql functions file

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext

import logging
import random

#method to generate random values
def trans_GenRandomData(VAR_trans_GenRandomData):
    VAR_trans_GenRandomData_result = random.choices(VAR_trans_GenRandomData, k=1)
    VAR_trans_GenRandomData_result_str = (', '.join(VAR_trans_GenRandomData_result[0]))    #convert list element to string
    return VAR_trans_GenRandomData_result_str



if __name__=='__main__':

    #set format logging
    MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    DATETIME_FORMAT = '%H:%M:%S'
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)
    trans_logger = logging.getLogger()


    #create spark context
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
    transaction_GetDictFiles = FullFolderPath+"/Repo/ConfigData/"+Trans_FileExtractDictToCSVList
    trans_get_list_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(transaction_GetDictFiles)

    #filter and return only tables that are marked as enabled
    trans_get_list_df_ready = trans_get_list_df.select("table","column").filter(trans_get_list_df.isenabled == '1').sort(desc("order"))


    for trans_row_getDict in trans_get_list_df_ready.collect(): 
        trans_FileName_final_pre = trans_GetDictFiles(spark,trans_row_getDict["table"],trans_logger)
        trans_Dictlist_final = trans_FileName_final_pre[0]
        print(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Sales.Customer":
            trans_LoadDict_Customer = trans_LoadCSVToDict(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Person.Address":
            trans_LoadDict_Address = trans_LoadCSVToDict(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Purchasing.ShipMethod":
            trans_LoadDict_ShipMethod = trans_LoadCSVToDict(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Sales.CreditCard":
            trans_LoadDict_CreditCard = trans_LoadCSVToDict(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Sales.SalesPerson":
            trans_LoadDict_SalesPerson = trans_LoadCSVToDict(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Sales.SalesTerritory":
            trans_LoadDict_SalesTerritory = trans_LoadCSVToDict(trans_Dictlist_final)
        
    #remove column name as we get it !!!!!!!!!!!!!

    #generate header dat
    Range = range(1, 100)
    rdd = sc.parallelize(Range). \
         map(lambda x: (x, trans_GenRandomData(trans_LoadDict_Customer), \
                         trans_GenRandomData(trans_LoadDict_ShipMethod), \
                         trans_GenRandomData(trans_LoadDict_CreditCard), \
                         trans_GenRandomData(trans_LoadDict_SalesPerson), \
                         trans_GenRandomData(trans_LoadDict_SalesTerritory), \
                         trans_GenRandomData(trans_LoadDict_Address)))

    df = rdd.toDF(). \
         withColumnRenamed("_1","ID"). \
         withColumnRenamed("_2", "Customer"). \
         withColumnRenamed("_3", "ShipMenthod"). \
         withColumnRenamed("_4", "CreditCard"). \
         withColumnRenamed("_5", "SalesPerson"). \
         withColumnRenamed("_6", "SalesTerritory"). \
         withColumnRenamed("_7", "Address")
    
    df.printSchema()
    df.show(5)
#    spark.stop()


