
from pyspark.sql import SparkSession

# Create SparkSession from builder

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

rdd = spark.sparkContext.range(1, 5)
print(rdd.collect())

# SparkContext stop() method
spark.sparkContext.stop()
