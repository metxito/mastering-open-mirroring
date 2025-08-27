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
    [current_timestamp]     NVARCHAR(128)       NULL,
    [active]                BIT             NOT NULL    DEFAULT 1,
    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)
GO
IF (SELECT COUNT(1) FROM [control].[queries_control]) = 0
INSERT INTO [control].[queries_control] ([proyect_name],        [connection_name],  [query_name],           [base_query],                                   [unique_keys],              [timestamp_keys],               [change_detection_mode], [change_detection_code]) VALUES
                                        ('AutomaticMirroring',  'basic',            'CardType',             'SELECT * FROM [control].[CardType]',           '["CardTypeID"]',           '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'TransactionStatus',    'SELECT * FROM [control].[TransactionStatus]',  '["TransactionStatusID"]',  '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'TransactionType',      'SELECT * FROM [control].[TransactionType]',    '["TransactionTypeID"]',    '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'Currency',             'SELECT * FROM [control].[Currency]',           '["CurrencyID"]',           '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'MerchantCategory',     'SELECT * FROM [control].[MerchantCategory]',   '["MerchantCategoryID"]',   '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'Merchant',             'SELECT * FROM [control].[Merchant]',           '["MerchantID"]',           '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'Customer',             'SELECT * FROM [control].[Customer]',           '["CustomerID"]',           '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'CardAccount',          'SELECT * FROM [control].[CardAccount]',        '["CardAccountID"]',        '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'Card',                 'SELECT * FROM [control].[Card]',               '["CardID"]',               '["CreatedOn", "ModifiedOn"]',  'condition',             'ISNULL([ModifiedOn], [CreatedOn]) > [CreatedOn]'),
                                        ('AutomaticMirroring',  'basic',            'Transactions',         'SELECT * FROM [control].[Transactions]',       '["TransactionID"]',        '["CreatedOn"]',                'only_insert',           NULL),
                                        ('AutomaticMirroring',  'basic',            'Payment',              'SELECT * FROM [control].[Payment]',            '["PaymentID"]',            '["CreatedOn"]',                'only_insert',           NULL)
GO
PRINT '[control].[queries_control] done'
GO



CREATE TABLE [source].[sources]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [query_name]            VARCHAR(128)    NOT NULL,
    [base_query]            NVARCHAR(MAX)   NOT NULL,
    [unique_keys]           NVARCHAR(MAX)   NOT NULL,
    [timestamp_keys]        NVARCHAR(MAX)   NOT NULL
)

