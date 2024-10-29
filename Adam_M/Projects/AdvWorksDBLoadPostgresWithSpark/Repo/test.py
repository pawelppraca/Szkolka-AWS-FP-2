from ConfigData.configdata import * #require to import config file
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime


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

conc_database = SQL_source_database
conc_user = SQL_source_user
conc_password  = SQL_source_password
conc_jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={conc_database};encrypt=true;trustServerCertificate=true;"
conc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
Var_Table = 'Sales.SalesOrderHeader'
Var_Column = 'DateModified'

from ConfigData.configdata import * #require to import config file
from ConfigData.sqlfunctions import * #require to import sql functions file


main_query = f"(SELECT ModifiedDate  FROM {Var_Table}  ) as q"
SQLServerToCSV_df = spark.read \
    .format("jdbc") \
    .option("url",conc_jdbc_url) \
    .option("driver", conc_driver) \
    .option("dbtable", main_query) \
    .option("user", conc_user) \
    .option("password", conc_password) \
    .load()
#    .option("fetchsize", conc_fetchsize) \

    #getmax value from extracted data which we will put into csv file - dataframe to one value
SQLServerToCSV_df2 = SQLServerToCSV_df.show(2)
print(SQLServerToCSV_df2)



spark.stop()

