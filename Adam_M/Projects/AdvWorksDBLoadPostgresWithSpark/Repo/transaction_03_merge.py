from ConfigData.configdata import * #require to import config file
from ConfigData.transaction_functions import * #require to import sql functions file

from pyspark.sql import SparkSession
from pyspark import SparkConf

from datetime import datetime
import datetime
import logging

#script to merge transaction data from gen schema into dbo

if __name__=='__main__':

        #set format logging
    MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    DATETIME_FORMAT = '%H:%M:%S'
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)
    trans_logger = logging.getLogger()

    getdatenow = datetime.datetime.now()                            # current date and time

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

    try:

        #put generated transaction data into dbo tab;es
        # ------------------------------------------------------------------------
        trans_MergeGenToDbo(spark)
        trans_logger.info("Transactions have been processed into dbo schema")
    
    except Exception:
        trans_logger.error("Transaction data into dbo schema failed!")
        raise 

    getdateend = datetime.datetime.now()                            # total time
    getdatetotal = getdateend - getdatenow
    minutes = divmod(getdatetotal.seconds, 60)
    trans_logger.info('Total time: ' +str(minutes[0])+ ' minutes '+str(minutes[1]) +' seconds')


