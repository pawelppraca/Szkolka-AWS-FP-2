from pyspark.sql import SparkSession

# RDD, Resilient Distributed Datasets (RDD) is a fundamental data structure of PySpark, It is an immutable distributed collection of objects. 
# Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

#create empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

#output
#EmptyRDD[188] at emptyRDD


#https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/