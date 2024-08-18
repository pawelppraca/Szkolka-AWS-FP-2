from pyspark.sql.types import StructType,StructField,StringType

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()

#create schema
schema = StructType([
    StructField('firstname', StringType(),True),
    StructField('secondname', StringType(),True),
    StructField('lastname',StringType(),True)
])


#Convert empty RDD to Dataframe
df1 = emptyRDD.toDF(schema)
df1.printSchema()

#Create empty DataFrame directly, without RDD
df2 = spark.createDataFrame([], schema)
df2.printSchema()

#Create empty DatFrame with no schema (no columns)
df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

#output
#root
# |-- firstname: string (nullable = true)
# |-- secondname: string (nullable = true)
# |-- lastname: string (nullable = true)

#root
# |-- firstname: string (nullable = true)
# |-- secondname: string (nullable = true)
# |-- lastname: string (nullable = true)

#root

#https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/