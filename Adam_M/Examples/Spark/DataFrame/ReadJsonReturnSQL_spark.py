import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as O

#script to return data from json using spark 

# Initiate the Spark Session
sc = SparkSession.builder.appName('covid-example').getOrCreate()


#JSON
df = sc.read.json('D:/Docs/documents/python/json/nyt2/nyt2.json')

#create a view which next we will be querying
df.createOrReplaceTempView("df")

#Running SQL Commnads
sc.sql("select * from df").show(3)



sc.stop()
