
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

