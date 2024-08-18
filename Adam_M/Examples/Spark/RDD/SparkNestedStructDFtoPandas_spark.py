from pyspark.sql.types import StructType,StructField,StringType, IntegerType

# Most of the time data in PySpark DataFrame will be in a structured format meaning one column contains other columns so let’s see how it convert to Pandas. 
# example with nested struct where we have firstname, middlename and lastname are part of the name column.

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataStruct = [(("Stach","Son"),"11239","M",3000),\
              (("Erni","Zwala"),"11459","M",4000),\
              (("Angela","Krys"),"5239","F",3400),\
              (("Mary","Kowal"),"6259","F",6000),
]

schemaStruct = StructType([
    StructField('name', StructType([
        StructField('firstname',StringType(),True),
        StructField('lastname',StringType(),True)
    ])),
    StructField('dob', StringType(),True),
    StructField('gender', StringType(),True),
    StructField('salary', StringType(),True)
])

df = spark.createDataFrame(data = dataStruct, schema = schemaStruct)
df.printSchema()

pandasDF = df.toPandas()
print(pandasDF)

#output
#root
# |-- name: struct (nullable = true)
# |    |-- firstname: string (nullable = true)
# |    |-- lastname: string (nullable = true)
# |-- dob: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- salary: string (nullable = true)

#             name    dob gender salary
#0    (Stach, Son)  11239      M   3000
#1   (Erni, Zwala)  11459      M   4000
#2  (Angela, Krys)   5239      F   3400
#3   (Mary, Kowal)   6259      F   6000

#https://sparkbyexamples.com/pandas/convert-pyspark-dataframe-to-pandas/