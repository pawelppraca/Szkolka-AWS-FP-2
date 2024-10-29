--install postgresql objects - this is to cover project export from sql to csv and load into postgres of AdventureWorks DB

\pset tuples_only on

-- Support to auto-generate UUIDs (aka GUIDs)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--create schema if not EXISTS
CREATE SCHEMA IF NOT EXISTS stg;

SELECT 'Creating tables...';
----------------------TABLES
--order header
	DROP TABLE IF EXISTS stg.SalesOrderHeader;

--ID,FIRST_NAME,LAST_NAME
	CREATE TABLE stg.SalesOrderHeader(
    SalesOrderHeaderID_PK SERIAL PRIMARY KEY, --identity column 
	SalesOrderID character varying(11) NOT NULL,--INT NOT NULL,
	OrderDate timestamp without time zone NOT NULL DEFAULT now(),
    DueDate timestamp without time zone NOT NULL,
    ShipDate timestamp without time zone,
	Status character varying(6) NOT NULL, --smallint NOT NULL,
	OnlineOrderFlag character varying(6) NOT NULL, --smallint NOT NULL,
	SalesOrderNumber character varying(50) NULL,
	PurchaseOrderNumber character varying(30) NULL,
	AccountNumber character varying(20) NULL,
	CustomerID character varying(11) NOT NULL, --INT NOT NULL,
	SalesPersonID character varying(11) NULL, --INT NULL,
	TerritoryID character varying(11) NULL, --INT NULL,
	BillToAddressID character varying(11) NOT NULL, --INT NOT NULL,
	ShipToAddressID character varying(11) NOT NULL, --INT NOT NULL,
	ShipMethodID character varying(11) NOT NULL, --INT NOT NULL,
	CreditCardID character varying(11) NULL, --INT NULL,
	CreditCardApprovalCode character varying(20) NULL,
	CurrencyRateID character varying(11) NULL, --INT NULL,
	SubTotal money NOT NULL, --character varying(21) NOT NULL, 
	TaxAmt money NOT NULL, --character varying(21) NOT NULL, 
	Freight money NOT NULL, --character varying(21) NOT NULL, 
	TotalDue  money NOT NULL, --character varying(21) NOT NULL, 
	Comment character varying(128) NULL,
	Rowguid uuid NOT NULL,
	ModifiedDate timestamp without time zone,
	LoadFileName character varying(50) NULL 
	-- character varying(50)
	);

	
--ordersdetail
	DROP table IF EXISTS stg.SalesOrderDetail;

--ID,USER_ID,ORDER_DATE,STATUS	
	CREATE table stg.SalesOrderDetail(
	SalesOrderDetailID_PK SERIAL PRIMARY KEY, --identity column 
	SalesOrderID character varying(10) NOT NULL, --INT NOT NULL,
	SalesOrderDetailID character varying(11) NOT NULL, --INT NOT NULL,
	CarrierTrackingNumber character varying(25) NULL,
	OrderQty character varying(6) NOT NULL, --smallint NOT NULL,
	ProductID character varying(11) NOT NULL, --INT NOT NULL,
	SpecialOfferID character varying(11) NOT NULL, --INT NOT NULL,
	UnitPrice money NOT NULL, --character varying(21) NOT NULL, 
	UnitPriceDiscount money NOT NULL, --character varying(21) NOT NULL, 
	LineTotal money NOT NULL, --character varying(21) NOT NULL, 
	Rowguid uuid  NOT NULL,
	ModifiedDate timestamp NOT NULL,	
	LoadFileName character varying(50) NULL 
	);

--rest if the tables
	--DROP table IF EXISTS jaffle.shop_payments;

--	CREATE table jaffle.shop_payments(
--	ID INT NOT NULL, 
--	CREATED DATE NULL
--	);

--system tables
--ordersdetail
	DROP table IF EXISTS stg.LoadFileStatus;

	CREATE table stg.LoadFileStatus(
	LoadFileStatusID SERIAL PRIMARY KEY, --identity column 
	LoadFileName character varying(50) NOT NULL,
	IsLoadedToStg boolean NOT NULL,
	IsLoadedToDB boolean NOT NULL,
	LoadedDate timestamp NOT NULL	
	);

---stored procedures
	create or replace procedure stg.LoadFileMark(
	   LoadFileName_input character varying(50)
	)
	LANGUAGE plpgsql
	AS $$
	begin
		-- insert loaded file details
		MERGE INTO stg.LoadFileStatus AS tgt
			USING
			(
				SELECT
					LoadFileName_input AS LoadFileName,
					TRUE AS IsLoadedToStg,
					FALSE AS IsLoadedToDB,
					LOCALTIMESTAMP AS LoadedDate
			) AS src
			ON ( tgt.LoadFileName = src.LoadFileName )
			WHEN MATCHED THEN
				UPDATE SET IsLoadedToStg= TRUE, LoadedDate= LOCALTIMESTAMP
			WHEN NOT MATCHED THEN
				INSERT (LoadFileName, IsLoadedToStg,IsLoadedToDB,LoadedDate)
				VALUES (LoadFileName_input, TRUE, FALSE, LOCALTIMESTAMP);

		--commit;
	end;$$;


----------------------LOAD data
----------------
--SELECT 'Copying data into customer';
--\copy jaffle.shop_customers FROM 'jaffle_shop_customers.csv' DELIMITER ',' CSV HEADER;


\pset tuples_only off
