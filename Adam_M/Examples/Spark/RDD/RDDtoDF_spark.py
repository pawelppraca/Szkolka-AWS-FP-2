from pyspark.sql import SparkSession

#create RDD passing to parallelize
spark = SparkSession.builder.appName('SParkByExample').getOrCreate()
#collection
depart = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
departColumns= ["dept_name","dept_ID"]
rdd = spark.sparkContext.parallelize(depart)
df = rdd.toDF(departColumns)
df.printSchema()
df.show(truncate=False)

#output
#root
# |-- dept_name: string (nullable = true)
# |-- dept_ID: long (nullable = true)

#+---------+-------+
#|dept_name|dept_ID|
#+---------+-------+
#|Finance  |10     |
#|Marketing|20     |
#|Sales    |30     |
#|IT       |40     |
#+---------+-------+

#https://sparkbyexamples.com/pyspark/convert-pyspark-rdd-to-dataframe/