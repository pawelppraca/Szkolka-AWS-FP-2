import findspark
#findspark.init('C:/Users/bokhy/spark/spark-2.4.6-bin-hadoop2.7')
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
#Data types
from pyspark.sql.types import DoubleType, IntegerType, StringType as F


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
# First few rows in the file
cases = cases.select('province','city','infection_case','confirmed')
cases = cases.withColumn('confirmed', cases['confirmed'].cast(IntegerType()))
cases = cases.withColumn('city', cases['city'].cast(StringType()))

cases.show(10)

spark.stop()