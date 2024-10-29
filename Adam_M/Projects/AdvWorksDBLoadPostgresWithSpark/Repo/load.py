#load into PostgreSQL from file

from ConfigData.configdata import * #require to import config file
from ConfigData.sqlfunctions import * #require to import sql functions file
from ConfigData.configfunctions import * #require to import config functions 

from pyspark.sql import SparkSession,functions as O
from pyspark import SparkContext, SparkConf, SQLContext

import logging

if __name__=='__main__':

    MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    DATETIME_FORMAT = '%H:%M:%S'
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)
    logger = logging.getLogger()

    #set path where file is stored
    FullFolderPath_final = FullFolderPath+"/"+FolderLoadCSV

    #define spark context for postgres with getOrCreate
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "Repo/Jars/postgresql-42.6.0.jar") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()


    #get a list of table names from config file that we will loaded from SQL --------------------------------
    loadfolder_CSVList = FullFolderPath+"/Repo/ConfigData/"+FileLoadCSVToSQLList

    load_tablelist_result = []
    load_tablelist_result = CollectListOfTables(loadfolder_CSVList)
    print(load_tablelist_result)



    try:
        #Clear staging tables, load data from CSV to PostgreSQL and mark as loaded   
        # ------------------------------------------------------------------------
        
        for list_item in load_tablelist_result:
            #print("before methof call: ",list_item,"-",FullFolderPath_final)
            CSVToPostgreSQL(spark, list_item,FullFolderPath_final,logger)

    except Exception:
            logger.error("Load into stg or truncate did not work!")
            raise    

    spark.stop()