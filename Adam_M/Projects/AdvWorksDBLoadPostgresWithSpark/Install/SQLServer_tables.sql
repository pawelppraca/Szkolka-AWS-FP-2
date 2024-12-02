
--table to keep checkpint about load
DROP TABLE IF EXISTS [dbo].[CheckPointSet]
GO
CREATE TABLE [dbo].[CheckPointSet](
	[CheckPointID] [int] IDENTITY(1,1) NOT NULL,
	[LoadedFileName] varchar(50) NOT NULL,
	[MaxValue] datetime,
	[IsCompleted] BIT NOT NULL default(0),
	[DateStart] datetime,
	--[TSQL] [nvarchar](max) NOT NULL,
 CONSTRAINT [PK_CheckPointSetID] PRIMARY KEY NONCLUSTERED 
(
	[CheckpointID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] 
GO


--sp to insert data into checkpint when initiate load  = NOT IN USE

CREATE OR ALTER PROCEDURE [dbo].[SetCheckpoint]
@FileName varchar(50) 
AS
BEGIN
    SET NOCOUNT ON;

	INSERT INTO  [dbo].[Checkpoint] SELECT @FileName AS [LoadedFileName],0 AS [MaxValue], 0 AS [IsCompleted], getdate() AS [DateStart]


	return;
END;

---add header and item identifiers required for linking transaction generator between gen and dbo schema
if OBJECT_ID('Sales.SalesOrderHeader') is not null
	ALTER TABLE Sales.SalesOrderHeader add [HeaderID] BIGINT NULL
if OBJECT_ID('Sales.SalesOrderDetail') is not null
	ALTER TABLE Sales.SalesOrderDetail add [ItemID] BIGINT NULL


-----create schema for auto generated data and table

CREATE SCHEMA [gen]
GO

drop table if exists [gen].[SalesOrderHeader]
GO 
CREATE TABLE [gen].[SalesOrderHeader](
	[HeaderID] [varchar](15) NOT NULL,
	[CustomerID] [varchar](20) NOT NULL,
	[ShipMethodID] [varchar](20) NOT NULL,
	[CreditCardID] [varchar](20) NULL,
	[SalesPersonID] [varchar](20) NULL,
	[TerritoryID] [varchar](20) NULL,
	[AddressID] [varchar](20) NOT NULL,
	[RevisionNumber] [varchar](10) NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [varchar](10) NOT NULL,
	[OnlineOrderFlag] [varchar](2) NOT NULL,
	[SalesOrderNumber]  [nvarchar](25) NULL,
	[PurchaseOrderNumber] [nvarchar](25) NULL,
	[AccountNumber] [nvarchar](25) NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [varchar](5) NULL,
	[SubTotal] [varchar](20)  NULL,
	[TaxAmt] [varchar](20)  NULL,
	[Freight] [varchar](20) NULL,
	[TotalDue]  [varchar](20) NULL,
	[Comment] [nvarchar](128) NULL,
	[ModifiedDate] [datetime] NOT NULL)
GO

drop table if exists [gen].[SalesOrderDetail]
GO

CREATE TABLE [gen].[SalesOrderDetail](
	[HeaderID] [varchar](15) NOT NULL,
	[NoOfItems] [varchar](3) NOT NULL,
	[CarrierTrackingNumber] [varchar](20) NOT NULL,
	[OrderQty] [varchar](3) NULL,
	[ProductID] [varchar](10) NULL,
	[UnitPrice] [varchar](10) NULL,
	[SpecialOfferID] [varchar](1) NOT NULL,
	[UnitPriceDiscount] [varchar](10) NULL,
	[LineTotal] [varchar](10) NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[NoOfItems_split] [varchar](3) NOT NULL,
	[ItemID] [varchar](15) NOT NULL)
GO

--------#------------merge sprocs to move data from gen schema into dbo dbo.sales tables

CREATE PROCEDURE [gen].[Merge_SalesOrder]
AS
SET NOCOUNT ON;

BEGIN
	BEGIN TRY
	
		BEGIN TRANSACTION transdata

		--insert header
		MERGE INTO Sales.[SalesOrderHeader] AS dest 
		USING gen.[SalesOrderHeader] AS src ON dest.HeaderID=src.HeaderID
		WHEN NOT MATCHED BY TARGET          
			THEN INSERT ([CustomerID],      [ShipMethodID]      ,[CreditCardID]      ,[SalesPersonID]      ,[TerritoryID]      ,[BillToAddressID],	[ShipToAddressID]      ,[RevisionNumber]      ,[OrderDate]      ,[DueDate]
			  ,[ShipDate]      ,[Status]      ,[OnlineOrderFlag]      ,[PurchaseOrderNumber]      ,[AccountNumber]      ,[CreditCardApprovalCode]      ,[CurrencyRateID]
			  ,[SubTotal]      ,[TaxAmt]      ,[Freight]      ,[Comment]      ,[ModifiedDate], rowguid, HeaderID)   
			VALUES ([CustomerID],      [ShipMethodID]      ,[CreditCardID]      ,[SalesPersonID]      ,[TerritoryID]      ,[AddressID],	[AddressID]      ,[RevisionNumber]      ,[OrderDate]      ,[DueDate]
			  ,[ShipDate]      ,[Status]      ,[OnlineOrderFlag]      ,[PurchaseOrderNumber]      ,[AccountNumber]      ,[CreditCardApprovalCode]      ,1 
			  ,cast([SubTotal] as money)      ,cast([TaxAmt] as money) ,[Freight] , NULL     ,[ModifiedDate], NEWID(), CAST(HeaderID as BIGINT))
		WHEN MATCHED 
		THEN 
		UPDATE
		SET
			[CustomerID] = src.[CustomerID],      
			[ShipMethodID] = src.[ShipMethodID],
			[CreditCardID] = src.[CreditCardID],
			[SalesPersonID] = src.[SalesPersonID],
			[TerritoryID] = src.[TerritoryID],
			[BillToAddressID] = src.[AddressID],
			[ShipToAddressID] = src.[AddressID],
			[RevisionNumber] = src.[RevisionNumber],
			[OrderDate] = src.[OrderDate],
			[DueDate] =  src.[DueDate],
			[ShipDate] = src.[ShipDate],
			[Status] = src.[Status],
			[OnlineOrderFlag] = src.[OnlineOrderFlag],
			[PurchaseOrderNumber] = src.[PurchaseOrderNumber],
			[AccountNumber] = src.[AccountNumber],
			[CreditCardApprovalCode] = src.[CreditCardApprovalCode],
			[CurrencyRateID] = src.[CurrencyRateID],
			[SubTotal] = src.[SubTotal],
			[TaxAmt]  = src.[TaxAmt],
			[Freight] = src.[Freight] ,
			[Comment] = src.[Comment],
			[ModifiedDate] = src.[ModifiedDate]
		;
		

	
		----insert details
		MERGE INTO Sales.SalesOrderDetail AS dest 
		USING 
		(
			SELECT
			d.SalesOrderID,
			g.CarrierTrackingNumber,
			g.OrderQty,
			g.ProductID,
			sop.SpecialOfferID,
			CAST(g.UnitPrice as money) AS UnitPrice,
			CAST(g.UnitPriceDiscount as money) AS UnitPriceDiscount,
			newid() as rowguid,
			g.ModifiedDate,
			CAST(g.ItemID as BIGINT) as ItemID
			FROM [gen].[SalesOrderDetail] g
			JOIN [Sales].[SalesOrderHeader] d ON g.HeaderID = d.HeaderID
			JOIN [Sales].[SpecialOfferProduct] sop ON sop.ProductID = g.ProductID
		) src
		ON dest.ItemID = src.ItemID
		WHEN NOT MATCHED BY TARGET          
			THEN INSERT (SalesOrderID,CarrierTrackingNumber,OrderQty,ProductID,SpecialOfferID,UnitPrice,UnitPriceDiscount,rowguid,ModifiedDate, ItemID)   
			VALUES (src.SalesOrderID,src.CarrierTrackingNumber,src.OrderQty,src.ProductID,src.SpecialOfferID,src.UnitPrice ,src.UnitPriceDiscount ,src.rowguid,src.ModifiedDate, src.ItemID)
		WHEN MATCHED 
		THEN 
		UPDATE
		SET
			SalesOrderID = src.SalesOrderID,
			CarrierTrackingNumber = src.CarrierTrackingNumber,
			OrderQty = src.OrderQty,
			ProductID = src.ProductID,
			SpecialOfferID = src.SpecialOfferID,
			UnitPrice = src.UnitPrice,
			UnitPriceDiscount = src.UnitPriceDiscount,
			ModifiedDate = src.ModifiedDate
		;

		--INSERT INTO [Sales].[SalesOrderDetail] 
		--(SalesOrderID,CarrierTrackingNumber,OrderQty,ProductID,SpecialOfferID,UnitPrice,UnitPriceDiscount,rowguid,ModifiedDate)
		--SELECT 
		--	d.SalesOrderID,g.CarrierTrackingNumber,g.OrderQty,g.ProductID,sop.SpecialOfferID,CAST(g.UnitPrice as money),CAST(g.UnitPriceDiscount as money),newid(),g.ModifiedDate
		--FROM [gen].[SalesOrderDetail] g
		--JOIN [Sales].[SalesOrderHeader] d ON g.HeaderID = d.HeaderID
		--JOIN [Sales].[SpecialOfferProduct] sop ON sop.ProductID = g.ProductID

		COMMIT TRANSACTION transdata
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION transdata;
		END;
		DECLARE @errorMessage NVARCHAR(4000) = NULL;
		SET @errorMessage = ERROR_MESSAGE();
		THROW 50000, @errorMessage, 1;
	END CATCH
END