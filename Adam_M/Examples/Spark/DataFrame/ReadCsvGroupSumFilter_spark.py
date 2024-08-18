#import findspark
#findspark.init('C:/Users/bokhy/spark/spark-2.4.6-bin-hadoop2.7')
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
# Descending Sort
from pyspark.sql import functions as F

#script to return data from csv using spark

# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

#casefile
caseFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/Case.csv'

cases = spark.read.load(caseFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")

#return grouped columns - province and city
#sum confirmed and get max confirmed
#filter out city with '-'
cases.groupBy(["province","city"]).agg(F.sum("confirmed").alias("TotalConfirmed") ,F.max("confirmed").alias("MaxConfirmed")).filter(cases.city != '-').show(10)

spark.stop()
#article
#https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20Dataframe%20Complete%20Guide%20(with%20COVID-19%20Dataset).ipynb