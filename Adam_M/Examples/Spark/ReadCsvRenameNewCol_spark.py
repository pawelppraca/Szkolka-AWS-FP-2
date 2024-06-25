import findspark
#findspark.init('C:/Users/bokhy/spark/spark-2.4.6-bin-hadoop2.7')
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext

import pyspark.sql.functions as F 
from pyspark.sql.types import * 

#script to retunr data from csv 
#initiate spark session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

#casefile
caseFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/Case.csv'

cases = spark.read.load(caseFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")

#amend source column names
#cases = cases.withColumnRenamed("infection_case","infection_source")

#amend all columns
cases = cases.toDF(*['my_case_id', 'province_nm', 'city_n', 'group_n', 'infection_case_n', 'confirmed_s',
       'latitude_s', 'longitude_s'])
# First 10 rows in the file
cases.show(10)

#add new columns based on existing value 
#first we create method to check input data and then return value
def checkCity(infection_case):
    if infection_case == "Richway":
        return "Yes"
    else:
        return "No"
#convert to UDF function , pass to function and return data     
casesUDF = F.udf(checkCity, StringType())
casesNewCol = cases.withColumn("IsRichway",casesUDF("infection_case_n"))
# First 10 rows in the file
casesNewCol.show(10)


spark.stop()