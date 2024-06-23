import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as O

#script to return data from json using spark 
#return particular column names
# Initiate the Spark Session
sc = SparkSession.builder.appName('covid-example').getOrCreate()


#JSON
df = sc.read.json('D:/Docs/documents/python/json/nyt2/nyt2.json')

#select only John's books
df.select("author","title","published_date").sort(O.desc("title")).filter(df.author == 'John Grisham').show(10)

#select John and Deans
df [df.author.isin("Dean R Koontz","John Grisham")].select("author","title","published_date").show(5)

#select string  Stephan with like 
df [df.author.like("%Stephan%")].select("author","title","published_date").show(5)

#selects with startswith and if exists return true
#StartsWith scans from the beginning of word/content with specified criteria in the brackets. 
#In parallel, EndsWith processes the word/content starting from the end. Both of the functions are case sensitive.
df [df.title.startswith("ODD")].select("author","title","published_date").show(5)
df [df.title.endswith("BOOK")].select("author","title","published_date").show(5)

#select particular string with substring, take 4 characters
df.select("author","title","published_date",df.author.substr(1,4).alias("cut_title")).show(5)

#select with new column and new value
df = df.select("author","title","published_date").withColumn('NewTitle', O.lit('new column')).show(5)


sc.stop()