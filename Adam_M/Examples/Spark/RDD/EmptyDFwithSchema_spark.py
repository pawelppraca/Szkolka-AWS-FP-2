from pyspark.sql.types import StructType,StructField,StringType


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
#print(emptyRDD)


#create schema
schema = StructType([
    StructField('firstname', StringType(),True),
    StructField('secondname', StringType(),True),
    StructField('lastname',StringType(),True)
])

#create empty datadrema from empty RDD
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()

#output
#root
# |-- firstname: string (nullable = true)
# |-- secondname: string (nullable = true)
# |-- lastname: string (nullable = true)

#https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/
