import pyspark
from pyspark.sql import SparkSession

#create dafaframe
spark = SparkSession.builder.appName("SparkExamp").getOrCreate()

data = [("James","Sun","1236","M",9000),
        ("Andy","Moon","1536","M",9400),
        ("Sara","Jane","78236","F",7000),
        ("Mary","Day","1606","F",9050),
]
columns = ["firstname","lastname","dob","gender","salary"]

#create DF,set schema and columns
sparkDF = spark.createDataFrame(data = data,schema = columns)
sparkDF.printSchema()
sparkDF.show(truncate=False)

#convert DF to Pandas
pandasDF = sparkDF.toPandas()
print(pandasDF)