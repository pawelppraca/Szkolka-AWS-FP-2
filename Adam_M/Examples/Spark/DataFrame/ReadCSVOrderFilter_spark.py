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
# First few rows in the file
cases.show(10)

#converts to pandas
#cases.limit(10).toPandas()

#return subset of columns and sort by latitude desc
cases = cases.select('province','city','infection_case','confirmed','latitude')
#filter by confirmed and latitude and sort desc by latitude
cases.sort(F.desc("latitude")).filter((cases.confirmed>10) & (cases.latitude != '-')).show(10)

spark.stop()