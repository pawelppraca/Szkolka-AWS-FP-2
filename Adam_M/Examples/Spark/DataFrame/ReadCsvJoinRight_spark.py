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

#region and cases files
regionFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/Region.csv'
casesFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/Case.csv'

regions = spark.read.load(regionFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")
cases = spark.read.load(casesFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")


# Right Join 'Case' with 'Region' on Province and City columns
#and return particular columns
cases = cases.join(regions,((cases.province == regions.province) & (cases.city == regions.city)),how='right').select(regions.province,regions.city,regions.code,cases.infection_case)
#return columns from regions filtere by province
#please notice in infection cases column we get NULL what means no corresponding row in cases
cases.filter(regions.province == 'Seoul').show(10)

spark.stop()
#article
#https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20Dataframe%20Complete%20Guide%20(with%20COVID-19%20Dataset).ipynb