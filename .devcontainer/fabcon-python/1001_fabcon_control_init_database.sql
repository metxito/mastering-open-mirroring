IF EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]='fabcon_control')
BEGIN
    EXEC('DROP DATABASE [fabcon_control]')
END
GO
EXEC('CREATE DATABASE [fabcon_control]')
PRINT '[fabcon_control] database created'
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
    [query_name]            VARCHAR(128)    NOT NULL,
    [base_query]            NVARCHAR(MAX)   NOT NULL,
    [unique_keys]           NVARCHAR(MAX)   NOT NULL,
    [timestamp_keys]        NVARCHAR(MAX)   NOT NULL,
    [change_detection_mode] VARCHAR(62)     NOT NULL, -- condition, comparation, only_insert
    [change_detection_code] NVARCHAR(MAX)       NULL,
    [next_file_sequence]    BIGINT              NULL,
    [current_timestamp]     BINARY(8)           NULL,
    [delete_detection]      BIT             NOT NULL    DEFAULT 0,
    [active]                BIT             NOT NULL    DEFAULT 1
)
GO
IF (SELECT COUNT(1) FROM [control].[queries_control]) = 0
INSERT INTO [control].[queries_control] ([proyect_name],        [connection_name],  [query_name],           [base_query],                               [unique_keys],              [timestamp_keys],   [change_detection_mode], [change_detection_code]) VALUES
                                        ('AutomaticMirroring',  'basic',            'CardType',             'SELECT * FROM [dbo].[CardType]',           '["CardTypeID"]',           '["RowVersion"]',   'condition',             '[ModifiedOn] IS NOT NULL'),
                                        ('AutomaticMirroring',  'basic',            'Currency',             'SELECT * FROM [dbo].[Currency]',           '["CurrencyID"]',           '["RowVersion"]',   'condition',             '[ModifiedOn] IS NOT NULL')
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
    [object]                VARCHAR(256)    NOT NULL
)
GO
INSERT INTO [source].[sources] ([connection_name], [object]) VALUES
                                        ('basic', '[dbo].[example1]'),
                                        ('basic', '[dbo].[example2]')


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
    [unique_key]            BIT             NOT NULL    DEFAULT 0,
    [timestamp_key]         BIT             NOT NULL    DEFAULT 0
)
GO
INSERT INTO [source].[columns]  ([connection_name], [object],           [column],           [data_type],    [unique_key],   [timestamp_key]) VALUES
                                ('basic',           '[dbo].[example1]', 'id',               'INT',          1,              0),
                                ('basic',           '[dbo].[example1]', 'description',      'VARCHAR(20)',  0,              0),
                                ('basic',           '[dbo].[example1]', 'created_on',       'DATETIME2',    0,              0),
                                ('basic',           '[dbo].[example1]', 'modified_on',      'DATETIME2',    0,              0),
                                ('basic',           '[dbo].[example1]', 'version',          'ROWVERSION',   0,              1),
                                ('basic',           '[dbo].[example2]', 'qw_id',            'INT',          1,              0),
                                ('basic',           '[dbo].[example2]', 'qw_bk',            'VARCHAR(10)',  1,              0),
                                ('basic',           '[dbo].[example2]', 'qw_description',   'VARCHAR(20)',  0,              0),
                                ('basic',           '[dbo].[example2]', 'qw_created_on',    'DATETIME2',    0,              0),
                                ('basic',           '[dbo].[example2]', 'qw_modified_on',   'DATETIME2',    0,              0),
                                ('basic',           '[dbo].[example2]', 'qw_version',       'ROWVERSION',   0,              1)
GO


GO
CREATE OR ALTER VIEW [source].[v_sources]
AS
    SELECT
        src.[id],
        src.[connection_name],
        src.[object],
        col.[unique_keys],
        col.[timestamp_keys]
    FROM
        [source].[sources] AS src
        LEFT JOIN (
            SELECT
                [connection_name],
                [object],
                [unique_keys] = '[' + STRING_AGG(CAST(IIF([unique_key] = 1, '"' + [column] + '"', NULL) AS NVARCHAR(max)), ', ') + ']',
                [timestamp_keys] = '[' + STRING_AGG(CAST(IIF([timestamp_key] = 1, '"' + [column] + '"', NULL) AS NVARCHAR(max)), ', ') + ']'
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
                    [object] = '[' + [TABLE_SCHEMA] + '].[' + [TABLE_NAME] + ']'
                FROM [fabcon_source_rowversion].[INFORMATION_SCHEMA].[TABLES]
            ) AS src
            ON  tgt.[object] = src.[object]
        WHEN NOT MATCHED BY TARGET
        THEN INSERT ([connection_name], [object])
             VALUES ('basic',           src.[object])
        WHEN NOT MATCHED BY SOURCE 
        THEN DELETE;   



    MERGE [source].[columns] AS tgt
        USING (
                SELECT 
                    [object] = '[' + [TABLE_SCHEMA] + '].[' + [TABLE_NAME] + ']',
                    [column] = [COLUMN_NAME],
                    [data_type] = [DATA_TYPE],
                    [unique_key] = 0,
                    [timestamp_key] = 0
                FROM [fabcon_source_rowversion].[INFORMATION_SCHEMA].[COLUMNS]
            ) AS src
                ON  tgt.[object] = src.[object]
                AND tgt.[column] = src.[column]
        WHEN NOT MATCHED BY TARGET
            THEN INSERT ([connection_name], [object],       [column],       [data_type],        [unique_key],       [timestamp_key])
                 VALUES ('basic',           src.[object],   src.[column],   src.[data_type],    src.[unique_key],   src.[timestamp_key])
        WHEN NOT MATCHED BY SOURCE
            THEN DELETE;

END
GO

