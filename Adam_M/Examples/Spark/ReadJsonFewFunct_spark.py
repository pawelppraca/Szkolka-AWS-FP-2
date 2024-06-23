import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext


#script to return data from csv using spark 
#pivot transforms to province rows to columns for confirmed and released
# Initiate the Spark Session
sc = SparkSession.builder.appName('covid-example').getOrCreate()


#JSON
dataframe = sc.read.json('D:/Docs/documents/python/json/nyt2/nyt2.json')



# Returns dataframe column names and data types
print("Dtypes:------")
dataframe.dtypes

# Displays the content of dataframe
print("/r Content of frame--------")
dataframe.show(5)

# Return first n rows
print("/r first n rows-------------")
dataframe.head()

# Returns first row
print("/r first row-------------")
dataframe.first()

# Return first n rows
print("/r first n rows-------------")
dataframe.take(5)

# Computes summary statistics
print("/r summary stats-------------")
dataframe.describe().show()

# Returns columns of dataframe
print("/r columns-------------")
dataframe.columns

# Counts the number of rows in dataframe
print("/r count rows-------------")
dataframe.count()

# Counts the number of distinct rows in dataframe
print("/r count dist-------------")
dataframe.distinct().count()

# Prints plans including physical and logical
print("/r plans-------------")
dataframe.explain(4)

sc.stop()