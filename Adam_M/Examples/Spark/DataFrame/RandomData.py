import usedFunctions as uf
import conf.variables as v
from sparkutils import sparkstuff as s

#this is a script to generate random data which next is inserted into DB, you can skip that part with DB

appName = "app1"
spark = s.spark_session(appName)
spark.sparkContext._conf.setAll(v.settings)
sc = s.sparkcontext()
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)

#
## Check if table exist otherwise create it
rows = 0
sqltext  = ""
if (spark.sql(f"""SHOW TABLES IN {v.DB} like '{v.tableName}'""").count() == 1):
    spark.sql(f"""ANALYZE TABLE {v.fullyQualifiedTableName} compute statistics""")
    rows = spark.sql(f"""SELECT COUNT(1) FROM {v.fullyQualifiedTableName}""").collect()[0][0]
    print ("number of rows is ",rows)
else:
    print(f"\nTable {v.fullyQualifiedTableName} does not exist, creating table ")
    sqltext = f"""
    CREATE TABLE {v.DB}.{v.tableName}(
      ID INT
    , CLUSTERED INT
    , SCATTERED INT
    , RANDOMISED INT
    , RANDOM_STRING VARCHAR(50)
    , SMALL_VC VARCHAR(50)
    , PADDING  VARCHAR(4000)
    )
    STORED AS PARQUET
    """
    spark.sql(sqltext)

start = 0
if (rows == 0):
      start = 1
      maxID= 0
else:
      maxID = spark.sql(f"SELECT MAX(id) FROM {v.fullyQualifiedTableName}").collect()[0][0]
start = maxID + 1
end = start + v.rowsToGenerate - 1
print ("starting at ID = ",start, ",ending on = ",end)
Range = range(start, end+1)
## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python lambda function

rdd = sc.parallelize(Range). \
         map(lambda x: (x, uf.clustered(x,v.rowsToGenerate), \
                           uf.scattered(x,v.rowsToGenerate), \
                           uf.randomised(x, v.rowsToGenerate), \
                           uf.randomString(50), \
                           uf.padString(x," ",50), \
                           uf.padSingleChar("x",4000)))
df = rdd.toDF(). \
         withColumnRenamed("_1","ID"). \
         withColumnRenamed("_2", "CLUSTERED"). \
         withColumnRenamed("_3", "SCATTERED"). \
         withColumnRenamed("_4", "RANDOMISED"). \
         withColumnRenamed("_5", "RANDOM_STRING"). \
         withColumnRenamed("_6", "SMALL_VC"). \
         withColumnRenamed("_7", "PADDING")
df.printSchema()
df.write.mode("overwrite").saveAsTable(f"""{v.DB}.ABCD""")
df.createOrReplaceTempView(f"""{v.tempView}""")
sqltext = f"""
      INSERT INTO TABLE {v.fullyQualifiedTableName}
      SELECT
              ID
            , CLUSTERED
            , SCATTERED
            , RANDOMISED
            , RANDOM_STRING
            , SMALL_VC
            , PADDING
      FROM tmp
"""
spark.sql(sqltext)
spark.sql(f"""SELECT MIN(id) AS minID, MAX(id) AS maxID FROM {v.fullyQualifiedTableName}""").show(n=20,truncate=False,vertical=False)
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");uf.println(lst)

#https://www.linkedin.com/pulse/creating-random-test-data-spark-python-code-mich-talebzadeh-ph-d-/
#https://github.com/michTalebzadeh/pilot