
from pyspark import SparkContext, SparkConf, SQLContext

appName = "PySpark SQL Server Example - via JDBC"
master = "local"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath", "jars/mssql-jdbc-12.6.3.jre8.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

database = "<DB>"           #define DB name
table = "<schema>.<table>"  #define table
user = "<user>"             #define SQL username
password  = "<pass>"        #define SQL pass

jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://localhost:1434;databaseName={database};encrypt=true;trustServerCertificate=true;") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

jdbcDF.show(3)

spark.stop()