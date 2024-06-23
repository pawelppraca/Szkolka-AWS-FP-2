import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as O

#script to return data from csv using spark 
#based on windows function rank which is partitioned by province and sorted desc by confirmed and released

# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

# TimeProvince file
tpFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/TimeProvince.csv'

tp = spark.read.load(tpFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")

windowtp = Window().partitionBy(['province']).orderBy(O.desc('confirmed'),O.desc('released'))
tp.withColumn("rank",O.rank().over(windowtp)).show(10)

spark.stop()
#article
#https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20Dataframe%20Complete%20Guide%20(with%20COVID-19%20Dataset).ipynb
