from ConfigData.configdata import * #require to import config file
from ConfigData.transaction_functions import * #require to import sql functions file

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext

import logging
import string
import random

from datetime import datetime, timedelta 
import datetime


#method to generate random values
def trans_GenRandomData(VAR_trans_GenRandomData):
    VAR_trans_GenRandomData_result = random.choices(VAR_trans_GenRandomData, k=1)
    VAR_trans_GenRandomData_result_str = (', '.join(VAR_trans_GenRandomData_result[0]))    #convert list element to string
    return VAR_trans_GenRandomData_result_str

#----------------------------------------------SET date and number of transactions
trans_date = datetime.datetime(2020, 5, 17)
trans_amount = 100000

#------------main part

if __name__=='__main__':

    #set format logging
    MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    DATETIME_FORMAT = '%H:%M:%S'
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)
    trans_logger = logging.getLogger()

    
    #prepare date
    getdatenow = datetime.datetime.now()                            # current date and time
    getdatenow_final = trans_date.strftime("%Y-%m-%d 00:00:00")     #set date column format
    getdatenow2 = datetime.datetime.now() + timedelta(days=12)                
    getdatenow2_final = trans_date.strftime("%Y-%m-%d 00:00:00")   #set date column format + add days
    getdatenow3 = datetime.datetime.now() + timedelta(days=7)                
    getdatenow3_final = trans_date.strftime("%Y-%m-%d 00:00:00")   #set date column format + add days
    getModifiedDate = getdatenow.strftime("%Y-%m-%d %H:%M:%S")     #set date column format
    trans_logger.info("Processing start time: "+getModifiedDate)

    #create spark context
    conf = SparkConf().setMaster("local[*]").set("spark.sql.debug.maxToStringFields", 1000) \
        .set("spark.executor.heartbeatInterval", 200000) \
        .set("spark.network.timeout", 300000) \
        .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .set("spark.driver.extraClassPath", "Repo/Jars/mssql-jdbc-12.6.3.jre8.jar") \
        .set("spark.ui.port", 4040) \
        .set("spark.driver.cores", "2") \
        .set("spark.executor.cores", "2") \
        .set("spark.driver.memory", "1G")  \
        .set("spark.executor.memory", "1G")  \
        .set("spark.executor.instances", "2") \
        .setAppName("Extract Dict from SQL Server")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")


    #get a list of table names from config files that we will load from SQL --------------------------------
    transaction_GetDictFiles = FullFolderPath+"/Repo/ConfigData/"+Trans_FileExtractDictToCSVList
    trans_get_list_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(transaction_GetDictFiles)

    #filter and return only tables that are marked as enabled
    trans_get_list_df_ready = trans_get_list_df.select("table","column").filter(trans_get_list_df.isenabled == '1').sort(desc("order"))


    for trans_row_getDict in trans_get_list_df_ready.collect(): 
        trans_FileName_final_pre = trans_GetDictFiles(spark,trans_row_getDict["table"],trans_logger)
        trans_Dictlist_final = trans_FileName_final_pre[0]
        print(trans_Dictlist_final)
        if trans_row_getDict["table"] == "Sales.Customer":
            trans_LoadDict_Customer = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Person.Address":
            trans_LoadDict_Address = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Purchasing.ShipMethod":
            trans_LoadDict_ShipMethod = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Sales.CreditCard":
            trans_LoadDict_CreditCard = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Sales.SalesPerson":
            trans_LoadDict_SalesPerson = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Sales.SalesTerritory":
            trans_LoadDict_SalesTerritory = trans_LoadCSVToDict(trans_Dictlist_final,"1")
        if trans_row_getDict["table"] == "Production.Product":
            trans_LoadDict_Product = trans_LoadCSVToDict(trans_Dictlist_final,"0")
            #trans_LoadDict_Product_tmp = trans_LoadCSVToDict(trans_Dictlist_final,"1")
    

    #set seed based on date that we set earlier
    trans_date_get = trans_date.strftime("%Y%m%d0000001")     #set string format
    trans_date_int = int(trans_date_get) 
    #generate random string to CarrierTrackNumber
    letters = string.ascii_letters
    one_letter = random.choice(letters)

    #generate header 
    Range = range(trans_date_int, trans_date_int+trans_amount)
    rdd = sc.parallelize(Range). \
         map(lambda x: (x, trans_GenRandomData(trans_LoadDict_Customer), \
                         trans_GenRandomData(trans_LoadDict_ShipMethod), \
                         trans_GenRandomData(trans_LoadDict_CreditCard), \
                         trans_GenRandomData(trans_LoadDict_SalesPerson), \
                         trans_GenRandomData(trans_LoadDict_SalesTerritory), \
                         trans_GenRandomData(trans_LoadDict_Address), \
                         random.randint(1,3),  \
                        ''.join(random.choices(letters, k=4))+'-'+''.join(random.choices(letters, k=4)), \
                         int(random.randint(1,3)),  \
                         int(random.randint(1,500))
                        ))
    #                     trans_GenRandomData(trans_LoadDict_Product), \
    

    #rename column names
    df_header = rdd.toDF(). \
         withColumnRenamed("_1","HeaderID"). \
         withColumnRenamed("_2", "CustomerID"). \
         withColumnRenamed("_3", "ShipMethodID"). \
         withColumnRenamed("_4", "CreditCardID"). \
         withColumnRenamed("_5", "SalesPersonID"). \
         withColumnRenamed("_6", "TerritoryID"). \
         withColumnRenamed("_7", "AddressID"). \
         withColumnRenamed("_8", "NoOfItems"). \
         withColumnRenamed("_9", "CarrierTrackingNumber"). \
         withColumnRenamed("_10", "OrderQty"). \
         withColumnRenamed("_11", "UnitPrice")
    #     withColumnRenamed("_11", "ProductID").\

    df_header.printSchema()
    #convert to pandas, set header and item
    df_header_pd = df_header.toPandas()
    df_item_pd = df_header_pd.copy()
        
    
    #header
    #drop item columns
    df_header_pd.drop(['CarrierTrackingNumber', 'OrderQty','UnitPrice','NoOfItems'], axis=1, inplace=True)
    #add columns into header df
    df_header_pd = df_header_pd.assign(RevisionNumber='8',
                                    OrderDate=getdatenow_final,
                                    DueDate=getdatenow2_final,
                                    ShipDate=getdatenow3_final,
                                    Status='5',
                                    OnlineOrderFlag='0',
                                    SalesOrderNumber='upt',
                                    PurchaseOrderNumber = "PO" + df_header_pd['HeaderID'].astype(str),
                                    AccountNumber='upt',
                                    CreditCardApprovalCode='abcdefghij',
                                    CurrencyRateID='upt',
                                    TaxAmt='0',
                                    Freight='10',
                                    TotalDue='upt',
                                    Comment='upt',
                                    ModifiedDate=getModifiedDate
                                       )
    #                                SubTotal='0',

    #items
    #drop header columns in items
    df_item_pd.drop(['CustomerID', 'ShipMethodID','CreditCardID','SalesPersonID','TerritoryID','AddressID'], axis=1, inplace=True)
    #add columns into item df
    
    df_item_pd = df_item_pd.assign( SpecialOfferID = '0',
                                    UnitPriceDiscount = '0',
                                    LineTotal = df_item_pd['UnitPrice']*df_item_pd['OrderQty'],
                                    ModifiedDate = getModifiedDate,
                                    NoOfItems_split = df_item_pd['NoOfItems'])
    
    """
    df_header = df_header.withColumn("RevisionNumber", lit('8'))\
                .withColumn('OrderDate', lit(getdatenow_final))\
                .withColumn('DueDate', lit(getdatenow2_final))\
                .withColumn('ShipDate', lit(getdatenow3_final))\
                .withColumn('Status', lit('5'))\
                .withColumn('OnlineOrderFlag', lit('0'))\
                .withColumn('SalesOrderNumber', lit('upt'))\
                .withColumn('PurchaseOrderNumber', concat(lit('PO'),df_header.HeaderID))\
                .withColumn('AccountNumber', lit('upt'))\
                .withColumn('CreditCardApprovalCode', lit('abcdefghij'))\
                .withColumn('CurrencyRateID', lit('upt'))\
                .withColumn('SubTotal', lit('upt'))\
                .withColumn('TaxAmt', lit('upt'))\
                .withColumn('Freight', lit('upt'))\
                .withColumn('TotalDue', lit('upt'))\
                .withColumn('Comment', lit('upt'))\
                .withColumn('ModifiedDate', lit(getModifiedDate))
    """            
    
    #split header lines to item lines based on value in number of items col
    decompose = lambda x: [10**i for i,a in enumerate(str(x)[::-1]) for _ in range(int(a))]
    df_item_pd['NoOfItems_split'] = df_item_pd['NoOfItems_split'].apply(decompose)
    df_item_pd_expl = df_item_pd.explode('NoOfItems_split')
    
    #add auto incerment column in item and drop no needed col
    df_item_pd_expl = df_item_pd_expl.assign(ItemID= lambda x: range(trans_date_int, trans_date_int + len(x)))  

    #add ProductID - rand value from Product 
    trans_LoadDict_Product_np = np.array(trans_LoadDict_Product).reshape(-1)    #set to 1 dim array
    df_item_pd_expl["ProductID"] = np.random.choice(trans_LoadDict_Product_np,size=len(df_item_pd_expl))


    print("exploded to df_item_pd_expl")
    print(df_item_pd_expl)


    #convert header and item pandas to spark df 
    df_header_prep=spark.createDataFrame(df_header_pd)
    df_item_prep=spark.createDataFrame(df_item_pd_expl)
    #calculate SubTotal, join and set into Header
    df_item_final_sum = df_item_prep.groupBy("HeaderID").agg(sum('LineTotal').alias("SubTotal"))
    df_header_prep_final = df_header_prep.join(df_item_final_sum, on="HeaderID")
    df_header_final = df_header_prep_final.select(df_header_prep["*"], df_item_final_sum["SubTotal"])
    df_item_final = df_item_prep.select("HeaderID","NoOfItems","CarrierTrackingNumber","OrderQty","ProductID","UnitPrice","SpecialOfferID","UnitPriceDiscount","LineTotal","ModifiedDate","NoOfItems_split","ItemID")


    
    try:
        #delete data in gen tables
        trans_genTables = ['gen.SalesOrderHeader','gen.SalesOrderDetail'
                       ]
        trans_TruncateGenTables(trans_genTables,trans_logger,spark)

        #put generated transaction data into SQL server
        # ------------------------------------------------------------------------
        for row_trans_genTables in trans_genTables:
            if row_trans_genTables == "gen.SalesOrderHeader":
                trans_GenDataToSQLServer(sc,df_header_final,row_trans_genTables)
            if row_trans_genTables == "gen.SalesOrderDetail":
                trans_GenDataToSQLServer(sc,df_item_final,row_trans_genTables)
        trans_logger.info("Transactions have been processed")
    
    except Exception:
        trans_logger.error("Transaction data into SQL server failed!")
        raise 
    
    getdateend = datetime.datetime.now()                            # total time
    getdatetotal = getdateend - getdatenow
    minutes = divmod(getdatetotal.seconds, 60)
    trans_logger.info('Total time: ' +str(minutes[0])+ ' minutes '+str(minutes[1]) +' seconds')

    spark.stop()


