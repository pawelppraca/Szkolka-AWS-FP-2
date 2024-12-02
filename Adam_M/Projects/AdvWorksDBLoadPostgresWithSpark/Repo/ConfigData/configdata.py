#configuration files

FullFolderPath = "D:/Docs/documents/python/projects/AWDBLoadPostgresWithSpark"

FolderExtractSQLToCSV = "Files/Source/0.Extract"
FolderLoadCSV = "Files/Source/1.Load"

FileExtractSQLToCSVList = "extract_table_list.csv"
FileLoadCSVToSQLList = "load_table_list.csv"
FileListOfColumnsToBeDropped = "extract_columns_dropList.json"

#DB SQL connection
SQL_source_database = "AdventureWorks2022"
SQL_source_user = "pythonsql"
SQL_source_password  = "<>"
SQL_source_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

PGSQL_source_database = "dbt_AW"
PGSQL_source_user = "postgres"
PGSQL_source_password  = "<>"
PGSQL_source_driver = "org.postgresql.Driver"
PGSQL_source_schema = "stg"

#transactions
Trans_FolderExtractDictTransToCSV = "Files/TransactionData"
Trans_FileExtractDictToCSVList = 'transactionDict_list.csv'