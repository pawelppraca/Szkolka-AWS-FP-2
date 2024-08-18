import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as O

#script to return data from csv using spark 
#based on windows function lag which returns data for 7 days before
#and filtered by Busan province and after 10.3.2020 only
# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

# TimeProvince file
tpFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/TimeProvince.csv'

tp = spark.read.load(tpFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")

windowtp = Window().partitionBy(['province']).orderBy('date')
tp.withColumn("confirmed-7days",O.lag("confirmed",7).over(windowtp)).filter((tp.province == "Busan") & (tp.date > '2020-03-10')).show(10)

spark.stop()
#article
#https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20Dataframe%20Complete%20Guide%20(with%20COVID-19%20Dataset).ipynb
