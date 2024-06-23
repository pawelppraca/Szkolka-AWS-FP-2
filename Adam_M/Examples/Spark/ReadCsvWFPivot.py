import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as O

#script to return data from csv using spark 
#pivot transforms to province rows to columns for confirmed and released
# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

# TimeProvince file
tpFileName = 'D:/Docs/documents/python/csv/coronavirusdataset/TimeProvince.csv'

tp = spark.read.load(tpFileName,
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")

windowtp = tp.groupBy('date').pivot('province').agg(O.sum('confirmed').alias('Confirmed'), O.sum('released').alias('Released')).filter(tp.date > '2020-03-10').show(10)

spark.stop()
#article
#https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20Dataframe%20Complete%20Guide%20(with%20COVID-19%20Dataset).ipynb

#output
#|      date|Busan_Confirmed|Busan_Released|Chungcheongbuk-do_Confirmed|Chungcheongbuk-do_Released|Chungcheongnam-do_Confirmed|Chungcheongnam-do_Released|Daegu_Confirmed|Daegu_Released|Daejeon_Confirmed|Daejeon_Released|Gangwon-do_Confirmed|Gangwon-do_Released|Gwangju_Confirmed|Gwangju_Released|Gyeonggi-do_Confirmed|Gyeonggi-do_Released|Gyeongsangbuk-do_Confirmed|Gyeongsangbuk-do_Released|Gyeongsangnam-do_Confirmed|Gyeongsangnam-do_Released|Incheon_Confirmed|Incheon_Released|Jeju-do_Confirmed|Jeju-do_Released|Jeollabuk-do_Confirmed|Jeollabuk-do_Released|Jeollanam-do_Confirmed|Jeollanam-do_Released|Sejong_Confirmed|Sejong_Released|Seoul_Confirmed|Seoul_Released|Ulsan_Confirmed|Ulsan_Released|
#+----------+---------------+--------------+---------------------------+--------------------------+---------------------------+--------------------------+---------------+--------------+-----------------+----------------+--------------------+-------------------+-----------------+----------------+---------------------+--------------------+--------------------------+-------------------------+--------------------------+-------------------------+-----------------+----------------+-----------------+----------------+----------------------+---------------------+----------------------+---------------------+----------------+---------------+---------------+--------------+---------------+--------------+
#|2020-04-30|            137|           116|                         45|                        41|                        143|                       127|           6852|          6144|               40|              35|                  53|                 40|               30|              27|                  676|                 486|                      1365|                     1147|                       117|                       97|               93|              68|               13|               8|                    18|                   11|                    15|                   11|              46|             38|            633|           453|             43|            37|