from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

#script load csv and group by city and sum population column

# Create a Spark session
spark = SparkSession.builder.appName("SparkExample").getOrCreate()
# Read CSV file from S3 into a DataFrame
S3Bucket = 'adamm-bucket'
S3File = 'uscities_noquote_small.csv'#'uscities_test.csv'
#s3 source file
#s3_input_path = "s3://"+S3Bucket+"/"+S3File
#df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

#local file
local_path = "D:/Docs/documents/python/csv/simplemaps_uscities_basicv1.79/"+S3File
df = spark.read.csv(local_path, header=True, inferSchema=True)
# Perform aggregation: sum purchase amount by state
agg_df = df.groupBy("city").agg(sum("population").alias("PopulationPerCity"))
# Show the aggregated DataFrame
agg_df.show()

# Write the result back to S3 in Parquet format
#s3_output_path = "s3://your-bucket/your-output-path/output_data.parquet"
#agg_df.write.mode("overwrite").parquet(s3_output_path)

# Stop the Spark session
spark.stop()

#article
#https://medium.com/@siladityaghosh/unlocking-big-data-insights-a-guide-to-apache-spark-on-aws-emr-with-sample-code-b89c4167e5d9#id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6IjNkNTgwZjBhZjdhY2U2OThhMGNlZTdmMjMwYmNhNTk0ZGM2ZGJiNTUiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTczMzU4NjA4OTkxNjEzOTU0MzUiLCJlbWFpbCI6ImFkYW1nb2VzY2xvdWRAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5iZiI6MTcxODcxMDI5OSwibmFtZSI6IkFkYW0iLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EvQUNnOG9jTFk1bE5hRDRWMHJYTzdyZ0VKX25HR21ncWJVVElQcWF1dlo5UjlkWm9rcmx6N1VRPXM5Ni1jIiwiZ2l2ZW5fbmFtZSI6IkFkYW0iLCJpYXQiOjE3MTg3MTA1OTksImV4cCI6MTcxODcxNDE5OSwianRpIjoiMDVmNzg3MGI5NWRmMTFjMGZkYTExM2EyMDRhNjA5ZGQxYWI0N2I0NSJ9.bQzxZDtekG6NAtO9kAm5mCT3Kica69FbLy8K_SLxSFF-i2r8KsvyfDoXeJ_zRn-_PBI2_ososnaGfKU7H9NCGFcYZQY1zIQug_E2ZEgFSz63ctkiw-kobYDNc0oc-VcjMJtbCRF216x0s37ttuwYdIKKjMAadDEQ_Dgu0vIQnWC0zGAORGpIRhyoCqo7_i8mjE6zocnSnr4HpT9EG7cPEru0RwUMgMJXsdoQYr_jx530UiCHXEfJvhAtpru0FUvQL7_NeSfNZ2YFjO-kEnYsphhIymKQDHliJsNo_FJfixVUDuYlwoTg4vWSwzevmsH2k21idwQPiNgBrrc5BcLWkw