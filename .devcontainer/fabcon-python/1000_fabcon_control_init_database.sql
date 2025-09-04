IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]='fabcon_control')
BEGIN
    EXEC('CREATE DATABASE [fabcon_control]')
    PRINT '[fabcon_control] database created'
END
GO
USE [fabcon_control]
GO
SET NOCOUNT ON
GO
IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[schemas] WHERE [name]='control')
    EXEC('CREATE SCHEMA [control] AUTHORIZATION [dbo]')
GO
IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[schemas] WHERE [name]='source')
    EXEC('CREATE SCHEMA [source] AUTHORIZATION [dbo]')
GO


-- ==============================================================================
-- Create CardType table
-- ==============================================================================
GO
DROP TABLE IF EXISTS [control].[queries_control]
GO 
CREATE TABLE [control].[queries_control]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [proyect_name]          VARCHAR(128)    NOT NULL,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [name]                  VARCHAR(128)    NOT NULL,
    [query_new_lsn]         NVARCHAR(MAX)   NOT NULL,
    [query_full]            NVARCHAR(MAX)   NOT NULL,
    [query_incremental]     NVARCHAR(MAX)   NOT NULL,
    [unique_keys]           NVARCHAR(MAX)   NOT NULL,
    [next_file_sequence]    BIGINT              NULL,
    [current_timestamp]     BINARY(10)          NULL,
    [active]                BIT             NOT NULL    DEFAULT 1
)
GO
IF (SELECT COUNT(1) FROM [control].[queries_control]) = 0
INSERT INTO [control].[queries_control] ([proyect_name],        [connection_name],  [name],           [query_new_lsn],                              [query_full],                               [query_incremental],                                                                                                                                [unique_keys]) VALUES
                                        ('AutomaticMirroring',  'basic',            'CardType',       'SELECT [lsn]=[sys].[fn_cdc_get_max_lsn]()',  'SELECT * FROM [dbo].[CardType]',           'SELECT * FROM [cdc].[fn_cdc_get_all_changes_dbo_CardType]([sys].[fn_cdc_get_min_lsn](''dbo_CardType''), [sys].[fn_cdc_get_max_lsn](), ''all'')',   '["CardTypeID"]'),
                                        ('AutomaticMirroring',  'basic',            'Currency',       'SELECT [lsn]=[sys].[fn_cdc_get_max_lsn]()',  'SELECT * FROM [dbo].[Currency]',           'SELECT * FROM [cdc].[fn_cdc_get_all_changes_dbo_Currency]([sys].[fn_cdc_get_min_lsn](''dbo_Currency''), [sys].[fn_cdc_get_max_lsn](), ''all'')',   '["CurrencyID"]')
GO
PRINT '[control].[queries_control] done'
GO


GO
DROP TABLE IF EXISTS [source].[sources]
GO
CREATE TABLE [source].[sources]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [object]                VARCHAR(256)    NOT NULL,
    [cdc_enabled]           BIT             NOT NULL    DEFAULT 0
)
GO
INSERT INTO [source].[sources] ([connection_name], [object], [cdc_enabled]) VALUES
                                        ('basic', '[dbo].[example1]', 0),
                                        ('basic', '[dbo].[example2]', 0)


GO
DROP TABLE IF EXISTS [source].[columns]
GO
CREATE TABLE [source].[columns]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [object]                VARCHAR(128)    NOT NULL,
    [column]                VARCHAR(256)    NOT NULL,
    [data_type]             VARCHAR(256)    NOT NULL,
    [unique_key]            BIT             NOT NULL    DEFAULT 0
)
GO
INSERT INTO [source].[columns]  ([connection_name], [object],           [column],           [data_type],    [unique_key]) VALUES
                                ('basic',           '[dbo].[example1]', 'id',               'INT',          1           ),
                                ('basic',           '[dbo].[example1]', 'description',      'VARCHAR(20)',  0           ),
                                ('basic',           '[dbo].[example1]', 'created_on',       'DATETIME2',    0           ),
                                ('basic',           '[dbo].[example1]', 'modified_on',      'DATETIME2',    0           ),
                                ('basic',           '[dbo].[example1]', 'version',          'ROWVERSION',   0           ),
                                ('basic',           '[dbo].[example2]', 'qw_id',            'INT',          1           ),
                                ('basic',           '[dbo].[example2]', 'qw_bk',            'VARCHAR(10)',  1           ),
                                ('basic',           '[dbo].[example2]', 'qw_description',   'VARCHAR(20)',  0           ),
                                ('basic',           '[dbo].[example2]', 'qw_created_on',    'DATETIME2',    0           ),
                                ('basic',           '[dbo].[example2]', 'qw_modified_on',   'DATETIME2',    0           ),
                                ('basic',           '[dbo].[example2]', 'qw_version',       'ROWVERSION',   0           )
GO


GO

CREATE OR ALTER VIEW [control].[v_queries]
AS
    SELECT
        [id],
        [proyect_name],
        [connection_name],
        [name],
        [query_new_lsn],
        [query_full],
        [query_incremental],
        [queries] = ISNULL('- ' + [query_new_lsn], '') + ISNULL(CHAR(10) + '- ' + [query_full], '') + ISNULL(CHAR(10) + '- ' + [query_incremental], ''),
        [unique_keys],           
        [next_file_sequence],    
        [current_timestamp],     
        [active] 
    FROM [control].[queries_control]
GO


CREATE OR ALTER VIEW [source].[v_sources]
AS
    SELECT
        src.[id],
        src.[connection_name],
        src.[object],
        src.[cdc_enabled],
        col.[unique_keys]
    FROM
        [source].[sources] AS src
        LEFT JOIN (
            SELECT
                [connection_name],
                [object],
                [unique_keys] = '[' + STRING_AGG(CAST(IIF([unique_key] = 1, '"' + [column] + '"', NULL) AS NVARCHAR(max)), ', ') + ']'
            FROM [source].[columns]
            GROUP BY
                [connection_name],
                [object]
        ) AS col
            ON  src.[connection_name] = col.[connection_name]
            AND src.[object] = col.[object]
GO







CREATE OR ALTER PROC [control].[usp_add_source_object] 
    @id INT
AS
BEGIN


    DECLARE @base_name varchar(255) = (SELECT [object] FROM [source].[sources] WHERE [id] = @id)
    SET @base_name = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(@base_name, '[dbo].', ''), '[', ''), ']', ''), '.', ' '))
    DECLARE @alter_name INT = 0
    DECLARE @name varchar(255) = @base_name

    WHILE EXISTS (SELECT TOP 1 1 FROM [control].[queries_control] WHERE [query_name] = @name)
    BEGIN
        SET @alter_name = @alter_name + 1
        SET @name = @base_name + ' ' + FORMAT(@alter_name, '00')
    END

    INSERT INTO [control].[queries_control] (
        [proyect_name],
        [connection_name],
        [query_name],
        [base_query],
        [unique_keys],
        [timestamp_keys],        
        [change_detection_mode]
    )
    SELECT  
        [proyect_name] = 'AutomaticMirroring',
        [connection_name] = src.[connection_name],
        [query_name] = @name,
        [base_query] = 'SELECT * FROM ' + src.[object],
        [unique_keys] = src.[unique_keys],
        [timestamp_keys] = src.[timestamp_keys],
        [change_detection_mode] = 'only_insert'
    FROM [source].[v_sources] AS src
    WHERE src.[id] = @id

END
GO




CREATE OR ALTER PROC [control].[usp_refresh_metadata] 
AS
BEGIN


    MERGE [source].[sources] AS tgt
        USING (
                SELECT 
                    [object] = '[' + s.[name] + '].[' + t.[name] + ']',
                    [cdc_enabled] = t.[is_tracked_by_cdc] 
                FROM 
                    [fabcon_source_cdc].[sys].[schemas] AS s 
                    JOIN [fabcon_source_cdc].[sys].[tables] AS t 
                        ON t.[schema_id] = s.[schema_id] 
                WHERE s.[name] NOT IN ('sys', 'cdc', 'information_schema') AND t.[name] NOT IN ('systranschemas')

            ) AS src
            ON  tgt.[object] = src.[object]
        
        WHEN NOT MATCHED BY TARGET
        THEN INSERT ([connection_name], [object], [cdc_enabled])
             VALUES ('basic',           src.[object], [cdc_enabled])
        
        WHEN MATCHED
        THEN UPDATE SET [cdc_enabled] = src.[cdc_enabled]
        
        WHEN NOT MATCHED BY SOURCE 
        THEN DELETE;   



    MERGE [source].[columns] AS tgt
        USING (
                SELECT  
                    [object] = '[' + c.[TABLE_SCHEMA] + '].[' + c.[TABLE_NAME] + ']',
                    [column] = c.[COLUMN_NAME],
                    [data_type] = c.[DATA_TYPE],
                    [unique_key] = ISNULL(u.[unique_key], CAST(0 AS BIT))
                FROM 
                    [fabcon_source_cdc].[INFORMATION_SCHEMA].[COLUMNS] AS c
                    LEFT JOIN (
                        SELECT 
                            tc.[TABLE_SCHEMA],
                            tc.[TABLE_NAME],
                            kcu.[COLUMN_NAME],
                            [unique_key] = CAST(1 AS BIT)
                        FROM 
                            [fabcon_source_cdc].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc
                            JOIN [fabcon_source_cdc].[INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu
                                ON tc.[CONSTRAINT_NAME] = kcu.[CONSTRAINT_NAME]
                                AND tc.[TABLE_SCHEMA] = kcu.[TABLE_SCHEMA]
                                AND tc.[TABLE_NAME] = kcu.[TABLE_NAME]
                        WHERE tc.[CONSTRAINT_TYPE] = 'PRIMARY KEY'
                    ) AS u
                    ON  u.[TABLE_SCHEMA] = c.[TABLE_SCHEMA]
                    AND u.[TABLE_NAME] = c.[TABLE_NAME]
                    AND u.[COLUMN_NAME] = c.[COLUMN_NAME]
                WHERE 
                    c.[TABLE_SCHEMA] NOT IN ('sys', 'cdc', 'information_schema') 
                    AND c.[TABLE_NAME] NOT IN ('systranschemas')
            ) AS src
                ON  tgt.[object] = src.[object]
                AND tgt.[column] = src.[column]
        
        WHEN NOT MATCHED BY TARGET
        THEN INSERT ([connection_name], [object],       [column],       [data_type],        [unique_key])
             VALUES ('basic',           src.[object],   src.[column],   src.[data_type],    src.[unique_key])
        
        WHEN NOT MATCHED BY SOURCE
        THEN DELETE;

END
GO

