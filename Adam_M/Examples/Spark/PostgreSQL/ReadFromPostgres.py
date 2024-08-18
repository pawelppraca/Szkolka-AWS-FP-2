from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .config("spark.jars", "jars/postgresql-42.6.0.jar") \
    .getOrCreate()
    
 # generally we also put `.master()` argument to define the cluster manager
 # it sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run locally with 4 cores, or
 # “spark://master:7077” to run on a Spark standalone cluster.
 # http://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    
# Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods

df = spark.read.format("jdbc"). \
options(
         url='jdbc:postgresql://localhost:5432/Adventureworks', # jdbc:postgresql://<host>:<port>/<database>
         dbtable='sales.salesorderheader',
         user='postgres',
         password='<provide_pass>', #provide_pass
         driver='org.postgresql.Driver').\
load()


#return top 3 rows in data frame
df.show(3)
print('---------------------------------------------------------------------------------------------------')
#convert dataframe to panda (well formatted)
pandasDF = df.toPandas()
print(pandasDF.head(2))


spark.stop()

#article
#https://mmuratarat.github.io/2020-06-18/pyspark-postgresql-locally