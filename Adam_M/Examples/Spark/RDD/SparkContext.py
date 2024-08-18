from pyspark.sql import SparkSession

# Create SparkSession from builder

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
print(spark.sparkContext)
print("Spark App Name : "+ spark.sparkContext.appName)

# SparkContext stop() method
spark.sparkContext.stop()



#article
#https://sparkbyexamples.com/pyspark/pyspark-sparkcontext-explained/