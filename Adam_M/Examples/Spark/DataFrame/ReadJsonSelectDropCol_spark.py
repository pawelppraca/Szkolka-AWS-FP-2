import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as O

#script to return data from json using spark 
#return particular column names
# Initiate the Spark Session
sc = SparkSession.builder.appName('covid-example').getOrCreate()


#JSON
df = sc.read.json('D:/Docs/documents/python/json/nyt2/nyt2.json')


#select and remove 2 columns 
df = df.drop("published_date","amazon_product_url").show(5)


sc.stop()

#Repartitioning
# Dataframe with 10 partitions
#df.repartition(10).rdd.getNumPartitions()

# Dataframe with 1 partition
#df.coalesce(1).rdd.getNumPartitions()