-- In user DB

https://docs.microsoft.com/en-us/sql/relational-databases/polybase/polybase-guide?view=sql-server-ver15
https://docs.microsoft.com/en-us/azure/azure-sql/managed-instance/data-virtualization-overview?view=azuresql

exec sp_configure 'polybase_enabled', 1;
reconfigure;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''


CREATE DATABASE SCOPED CREDENTIAL [zzDemoCredential01_MI]  WITH IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE zzsqladlstest0001_V01
WITH (
	LOCATION = 'abs://tpch-sf1000-consolidated@tpchdata02.blob.core.windows.net/',
    CREDENTIAL = [zzDemoCredential01_MI] 
)
SELECT TOP 10 * FROM OPENROWSET(BULK 'NATION/*', DATA_SOURCE = 'zzsqladlstest0001_V01', FORMAT = 'parquet') AS filerows




CREATE DATABASE SCOPED CREDENTIAL [zzDemoCredential01]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2020-08-04&ss';


DROP EXTERNAL DATA SOURCE zzsqladlstest0001_V00

CREATE EXTERNAL DATA SOURCE zzsqladlstest0001_V00
WITH (
	LOCATION = 'abs://cont01@zzsqladlstest0001.blob.core.windows.net/',
    CREDENTIAL = [zzDemoCredential01] 
)

SELECT TOP 10 * FROM OPENROWSET( 
 BULK 'bing_covid-19_data.parquet',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows



SELECT TOP 10 * FROM OPENROWSET(
 BULK 'sampleparquet/2022/Mar/',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows

SELECT TOP 10 
filerows.filename() as filename,
filerows.filepath() as filepath,
filerows.filepath(1) as folderlevel1,
filerows.filepath(2) as folderlevel2,*
FROM OPENROWSET(
 BULK 'sampleparquet/*/*/',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows



EXEC sp_describe_first_result_set N'SELECT TOP 10 * FROM OPENROWSET( 
 BULK ''bing_covid-19_data.parquet'',
 DATA_SOURCE = ''zzsqladlstest0001_V00'',
 FORMAT = ''parquet''
) AS filerows'


SELECT 
    r.filename() as FileName,
    r.filepath() as FilePath,
    FullLine,
    JSON_VALUE(FullLine ,'$.info.match_type_number')   as MatchId,
    JSON_VALUE(FullLine ,'$.info.venue')               as info_venue ,
	JSON_VALUE(FullLine ,'$.info.competition')         as info_competition,
	JSON_VALUE(FullLine ,'$.info.dates[0]')            as info_dates,
	JSON_VALUE(FullLine ,'$.info.gender')              as info_gender,
    
   	JSON_VALUE(FullLine ,'$.info.outcome.winner')      as info_outcome_winner,
	JSON_VALUE(FullLine ,'$.info.outcome.by.runs')     as info_outcome_by_runs,
	JSON_VALUE(FullLine ,'$.info.outcome.by.wickets')  as info_outcome_by_wickets
FROM 
OPENROWSET
(
    BULK '/samplejson/*',
    FORMAT='CSV',
	DATA_SOURCE = 'zzsqladlstest0001_V00',
    FIELDTERMINATOR ='0x0b', 
    FIELDQUOTE = '0x0b', 
    ROWTERMINATOR = '0x0b'
) 
WITH 
(
    FullLine VARCHAR(MAX)
) r



SELECT 
        s.filename() as FileName,
        JSON_VALUE(FullLine ,'$.info.match_type_number')   as MatchId,
	c2.[key]					as matchinnings,
	c3.[key]					as ballindex,
	c4.[key]					as ballnum	,

	JSON_VALUE(c2.value,'$.team')			as team,
	JSON_VALUE(c4.value,'$.batsman')		as batsman,
	JSON_VALUE(c4.value,'$.non_striker')		as non_striker,
	JSON_VALUE(c4.value,'$.bowler')			as bowler,
	JSON_VALUE(c4.value,'$.runs.total')		as runs_total,
	JSON_VALUE(c4.value,'$.runs.extras')		as runs_extras,
	JSON_VALUE(c4.value,'$.runs.batsman')		as runs_batsman,
	JSON_VALUE(c4.value,'$.extras.byes')		as extras_byes,
	JSON_VALUE(c4.value,'$.extras.wides')		as extras_wides,
	JSON_VALUE(c4.value,'$.extras.noballs')		as extras_noballs,
	JSON_VALUE(c4.value,'$.extras.legbyes')		as extras_legbyes,
	JSON_VALUE(c4.value,'$.wicket.player_out')	as wicket_player_out,
	JSON_VALUE(c4.value,'$.wicket.kind')		as wicket_kind,
	JSON_VALUE(c4.value,'$.wicket.fielders[0]') 	as wicket_fielders_01,
	JSON_VALUE(c4.value,'$.wicket.fielders[1]') 	as wicket_fielders_02
FROM 
OPENROWSET
(
    BULK '/samplejson/*',
    FORMAT='CSV',
	DATA_SOURCE = 'zzsqladlstest0001_V00',
	FIELDTERMINATOR ='0x0b', 
	FIELDQUOTE = '0x0b', 
	ROWTERMINATOR = '0x0b'
) 
WITH 
(
	FullLine VARCHAR(MAX)
) s

	CROSS APPLY OPENJSON(s.FullLine,'$.innings') c1
	CROSS APPLY OPENJSON(c1.value,'$') c2
	CROSS APPLY OPENJSON(c2.value,'$.deliveries') c3
	CROSS APPLY OPENJSON(c3.value,'$') c4



SELECT 
    r.filename() as FileName,
    r.filepath() as FilePath,
	[Symbol],	
	[Name]	,	
	[LastSale]  ,
	[MarketCap]	,
	[IPOyear]	,
	[Sector]	,
	[industry]  ,
	[Summary Quote] 
    
FROM 
    OPENROWSET
    (
		BULK '/samplecsv/companylist20201.csv',
		FORMAT='CSV',
		DATA_SOURCE = 'zzsqladlstest0001_V00',
        FIRSTROW=2,
        FIELDTERMINATOR =',', 
        ROWTERMINATOR = '\n', 
        FIELDQUOTE = '"' 
    ) 
    WITH 
    (
	[Symbol]	VARCHAR(100),
	[Name]		VARCHAR(100),
	[LastSale]  VARCHAR(200),
	[MarketCap]	VARCHAR(20),
	[IPOyear]	VARCHAR(20),
	[Sector]	VARCHAR(100),
	[industry]  VARCHAR(100),
	[Summary Quote] VARCHAR(200)
    ) r





	GO

DROP EXTERNAL FILE FORMAT [TextFileFormat_C]

CREATE EXTERNAL FILE FORMAT [TextFileFormat_C] WITH 
(FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS 
(STRING_DELIMITER ='"',FIELD_TERMINATOR = N',', DATE_FORMAT = N'yyyy-MM-dd HH:mm:ss', FIRST_ROW = 2, USE_TYPE_DEFAULT = False)
)

DROP EXTERNAL TABLE [dbo].[external_companylist]
CREATE EXTERNAL TABLE [dbo].[external_companylist]
(
	[Symbol]	VARCHAR(100),
	[Name]		VARCHAR(100),
	[LastSale]  VARCHAR(200),
	[MarketCap]	VARCHAR(20),
	[IPOyear]	VARCHAR(20),
	[Sector]	VARCHAR(100),
	[industry]  VARCHAR(100),
	[Summary Quote] VARCHAR(200)
)
WITH (DATA_SOURCE = zzsqladlstest0001_V00,LOCATION = N'/samplecsv/companylist20201.csv',FILE_FORMAT = [TextFileFormat_C],REJECT_TYPE = VALUE,REJECT_VALUE = 0)



select * from [dbo].[external_companylist]
GO

CREATE EXTERNAL TABLE [dbo].[external_companylistexport] WITH 
(
        LOCATION = '/samplecsvexport/',
        DATA_SOURCE = zzsqladlstest0001_V00,
        FILE_FORMAT = [TextFileFormat_C]
) AS
SELECT 'a' as Col1
    

	SELECT TOP 10 
filerows.filename() as filename,
filerows.filepath() as filepath,
filerows.filepath(1) as folderlevel1,
filerows.filepath(2) as folderlevel2,*
FROM OPENROWSET(
 BULK 'SUPPLIER/*/*',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows

34,795,944
SELECT count(*)
FROM OPENROWSET(
 BULK 'SUPPLIER/*/*',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows



SELECT TOP 100 * ,filerows.filepath(1) as folderlevel1
FROM OPENROWSET(
 BULK 'SUPPLIER/*/*',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows
WHERE filerows.filepath(1) = 'S_NATIONKEY=9' and S_PHONE = '19-622-616-1912'



SELECT filerows.filepath(1) as folderlevel1,* INTO Supplier3
FROM OPENROWSET(
 BULK 'SUPPLIER/*/*',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows
WHERE filerows.filepath(1) = 'S_NATIONKEY=10' and S_PHONE = '19-622-616-1912'


DROP VIEW dbo.vwextSupplier

create view dbo.vwextSupplier AS
SELECT filerows.filepath(1) as folderlevel1,* 
FROM OPENROWSET(
 BULK 'SUPPLIER/*/*',
 DATA_SOURCE = 'zzsqladlstest0001_V00',
 FORMAT = 'parquet'
) AS filerows


SELECT 
	s.S_PHONE,SUM(vws.S_ACCTBAL) as AcctBalance 
FROM dbo.vwextSupplier vws
INNER JOIN [dbo].[Supplier] s ON
	s.S_SUPPKEY = vws.S_SUPPKEY
GROUP BY 
	s.S_PHONE


	--ALTER DATABASE  DB01 COLLATE Latin1_General_BIN2 --Run in Master