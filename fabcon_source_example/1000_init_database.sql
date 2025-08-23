IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]='fabcon_source')
EXEC('CREATE DATABASE [fabcon_source]')
GO
USE [fabcon_source]
GO


-- ==============================================================================
-- Create CardType table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='CardType')
EXEC ('CREATE TABLE [dbo].[CardType]
(
    [CardTypeID]        INT                         PRIMARY KEY,
    [TypeName]          NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]       NVARCHAR(255)       NULL,
    [CreatedAt]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[CardType]) = 0
INSERT INTO [dbo].[CardType] ([CardTypeID], [TypeName], [Description])
VALUES
    (1, 'Visa', 'Visa Credit or Debit Card'),
    (2, 'MasterCard', 'MasterCard Credit or Debit Card'),
    (3, 'American Express', 'American Express Credit Card'),
    (4, 'Discover', 'Discover Credit Card'),
    (5, 'Debit', 'Generic Debit Card');
GO




-- ==============================================================================
-- Create TransactionStatus table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='TransactionStatus')
EXEC ('CREATE TABLE [dbo].[TransactionStatus]
(
    [TransactionStatusID]   INT                         PRIMARY KEY,
    [StatusName]            NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[TransactionStatus]) = 0
INSERT INTO [dbo].[TransactionStatus] ([TransactionStatusID], [StatusName], [Description])
VALUES
    (1, 'Pending',  'Transaction is initiated but not yet approved'),
    (2, 'Approved', 'Transaction was approved successfully'),
    (3, 'Declined', 'Transaction was declined by issuer or processor'),
    (4, 'Settled',  'Transaction has been cleared and funds settled'),
    (5, 'Reversed', 'Transaction was reversed or refunded');
GO





-- ==============================================================================
-- Create TransactionType table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='TransactionType')
EXEC ('CREATE TABLE [dbo].[TransactionType]
(
    [TransactionTypeID]     INT                         PRIMARY KEY,
    [TypeName]              NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[TransactionType]) = 0
INSERT INTO [dbo].[TransactionType] ([TransactionTypeID], [TypeName], [Description])
VALUES
    (1, 'Purchase', 'Standard purchase transaction'),
    (2, 'Refund', 'Refund issued to the cardholder'),
    (3, 'Cash Advance', 'Cash withdrawal from ATM or teller'),
    (4, 'Fee', 'Bank or service fee applied'),
    (5, 'Interest', 'Interest charges on balance');
GO





-- ==============================================================================
-- Create Currency table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Currency')
EXEC ('CREATE TABLE [dbo].[Currency]
(
    [CurrencyID]        INT                         PRIMARY KEY,
    [CurrencyCode]      CHAR(3)         NOT NULL    UNIQUE, -- ISO 4217 code
    [CurrencyName]      NVARCHAR(50)    NOT NULL,
    [Symbol]            NVARCHAR(5)         NULL,
    [CreatedAt]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Currency]) = 0
INSERT INTO [dbo].[Currency] ([CurrencyID], [CurrencyCode], [CurrencyName], [Symbol])
VALUES
    (1, 'EUR', 'Euro', '€'),
    (2, 'USD', 'US Dollar', '$');
GO








-- ==============================================================================
-- Create MerchantCategory table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='MerchantCategory')
EXEC ('CREATE TABLE [dbo].[MerchantCategory]
(
    [MerchantCategoryID]    INT                         PRIMARY KEY,
    [CategoryName]          NVARCHAR(100)   NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[MerchantCategory]) = 0
INSERT INTO [dbo].[MerchantCategory] ([MerchantCategoryID], [CategoryName], [Description])
VALUES
    (1, 'Retail', 'Stores selling consumer goods'),
    (2, 'Travel', 'Airlines, hotels, and travel services'),
    (3, 'Food & Beverage', 'Restaurants, cafes, bars'),
    (4, 'Entertainment', 'Movies, concerts, events'),
    (5, 'Utilities', 'Electricity, water, internet services'),
    (6, 'Health & Wellness', 'Pharmacies, gyms, clinics');
GO








-- ==============================================================================
-- Create Merchant table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Merchant')
EXEC ('CREATE TABLE [dbo].[Merchant]
(
    [MerchantID]            INT                         PRIMARY KEY,
    [MerchantName]          NVARCHAR(150)   NOT NULL,
    [MerchantCategoryID]    INT             NOT NULL,
    [Address]               NVARCHAR(255)       NULL,
    [City]                  NVARCHAR(100)       NULL,
    [Country]               NVARCHAR(100)       NULL,
    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [FK_Merchant_Category] FOREIGN KEY ([MerchantCategoryID]) REFERENCES [dbo].[MerchantCategory]([MerchantCategoryID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Merchant]) = 0
INSERT INTO [dbo].[Merchant] ([MerchantID], [MerchantName], [MerchantCategoryID], [Address], [City], [Country])
VALUES
(1, 'Walmart', 1, '702 SW 8th St', 'Bentonville', 'USA'),
(2, 'Target', 1, '1000 Nicollet Mall', 'Minneapolis', 'USA'),
(3, 'Best Buy', 1, '7601 Penn Ave S', 'Richfield', 'USA'),
(4, 'IKEA', 1, '420 Alan Wood Rd', 'Conshohocken', 'USA'),
(5, 'H&M', 1, 'Västra Hamngatan 3', 'Stockholm', 'Sweden'),
(6, 'Hilton Hotels', 2, '7930 Jones Branch Dr', 'McLean', 'USA'),
(7, 'Marriott', 2, '10400 Fernwood Rd', 'Bethesda', 'USA'),
(8, 'Expedia', 2, '333 108th Ave NE', 'Bellevue', 'USA'),
(9, 'Delta Airlines', 2, '1030 Delta Blvd', 'Atlanta', 'USA'),
(10, 'Airbnb', 2, '888 Brannan St', 'San Francisco', 'USA'),
(11, 'Starbucks', 3, '2401 Utah Ave S', 'Seattle', 'USA'),
(12, 'McDonald''s', 3, '110 N Carpenter St', 'Chicago', 'USA'),
(13, 'Burger King', 3, '5505 Blue Lagoon Dr', 'Miami', 'USA'),
(14, 'Pizza Hut', 3, '7100 Corporate Dr', 'Plano', 'USA'),
(15, 'Subway', 3, '325 Sub Way', 'Milford', 'USA'),
(16, 'AMC Theatres', 4, '11500 NW 105th St', 'Kansas City', 'USA'),
(17, 'Cinemark', 4, '3900 N Stemmons Fwy', 'Dallas', 'USA'),
(18, 'Live Nation', 4, '9348 Civic Center Dr', 'Beverly Hills', 'USA'),
(19, 'Ticketmaster', 4, '800 Connecticut Ave NW', 'Washington', 'USA'),
(20, 'Spotify', 4, '4 World Trade Center', 'New York', 'USA'),
(21, 'Comcast', 5, '1701 JFK Blvd', 'Philadelphia', 'USA'),
(22, 'AT&T', 5, '208 S Akard St', 'Dallas', 'USA'),
(23, 'Verizon', 5, '1095 Avenue of the Americas', 'New York', 'USA'),
(24, 'E.ON', 5, 'Brüsseler Str. 57', 'Essen', 'Germany'),
(25, 'Pfizer', 6, '235 E 42nd St', 'New York', 'USA'),
(26, 'CVS Pharmacy', 6, 'One CVS Drive', 'Woonsocket', 'USA'),
(27, 'Walgreens', 6, '200 Wilmot Rd', 'Deerfield', 'USA'),
(28, 'Planet Fitness', 6, '4 Liberty Ln W', 'Hampton', 'USA'),
(29, 'LA Fitness', 6, '2600 Michelson Dr', 'Irvine', 'USA'),
(30, 'Rite Aid', 6, '30 Hunter Ln', 'Camp Hill', 'USA')
GO






-- ==============================================================================
-- Create Customer table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Customer')
EXEC ('CREATE TABLE [dbo].[Customer]
(
    [CustomerID]    INT                         PRIMARY KEY,
    [FirstName]     NVARCHAR(100)   NOT NULL,
    [LastName]      NVARCHAR(100)   NOT NULL,
    [Email]         NVARCHAR(150)       NULL,
    [PhoneNumber]   NVARCHAR(20)        NULL,
    [DateOfBirth]   DATE                NULL,
    [CreatedAt]     DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Customer]) = 0
INSERT INTO [dbo].[Customer] ([CustomerID], [FirstName], [LastName], [Email], [PhoneNumber], [DateOfBirth])
VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '+1-555-1010', '1985-03-15'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '+1-555-2020', '1990-07-22'),
(3, 'Michael', 'Johnson', 'michael.johnson@example.com', '+1-555-3030', '1978-12-05'),
(4, 'Emily', 'Brown', 'emily.brown@example.com', '+1-555-4040', '1995-09-10'),
(5, 'David', 'Wilson', 'david.wilson@example.com', '+1-555-5050', '1982-05-30')
GO








-- ==============================================================================
-- Create CardAccount table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='CardAccount')
EXEC ('CREATE TABLE [dbo].[CardAccount]
(
    [CardAccountID]     INT                         PRIMARY KEY,
    [CustomerID]        INT             NOT NULL,
    [AccountNumber]     NVARCHAR(20)    NOT NULL    UNIQUE,
    [Balance]           DECIMAL(18,2)   NOT NULL    DEFAULT 0,
    [CreditLimit]       DECIMAL(18,2)       NULL,
    [CurrencyID]        INT             NOT NULL,
    [CreatedAt]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [FK_CardAccount_Customer] FOREIGN KEY ([CustomerID]) REFERENCES [dbo].[Customer]([CustomerID]),
    CONSTRAINT [FK_CardAccount_Currency] FOREIGN KEY ([CurrencyID]) REFERENCES [dbo].[Currency]([CurrencyID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[CardAccount]) = 0
INSERT INTO [dbo].[CardAccount] ([CardAccountID], [CustomerID], [AccountNumber], [Balance], [CreditLimit], [CurrencyID])
VALUES
(110, 1, 'ACE10110', 0.00,  5000.00, 1), -- John Doe, EUR
(120, 2, 'ACU10120', 0.00,  7000.00, 2), -- Jane Smith, USD
(130, 3, 'ACU10130', 0.00, 10000.00, 2), -- Michael Johnson, USD
(140, 4, 'ACE10140', 0.00,  3000.00, 1), -- Emily Brown, EUR
(150, 5, 'ACU10150', 0.00,  6000.00, 2); -- David Wilson, USD
GO









-- ==============================================================================
-- Create Card table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Card')
EXEC ('CREATE TABLE [dbo].[Card]
(
    [CardID]            INT                         PRIMARY KEY,
    [CardAccountID]     INT             NOT NULL,
    [CardTypeID]        INT             NOT NULL,
    [CardNumber]        NVARCHAR(16)    NOT NULL    UNIQUE,
    [ActivationDate]    DATETIME2           NULL,
    [ExpirationDate]    DATE            NOT NULL,
    [CVV]               NVARCHAR(4)     NOT NULL,
    [IsActive]          BIT             NOT NULL    DEFAULT 1,
    [CreatedAt]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),

    CONSTRAINT [FK_Card_CardAccount] FOREIGN KEY ([CardAccountID]) REFERENCES [dbo].[CardAccount]([CardAccountID]),
    CONSTRAINT [FK_Card_CardType]    FOREIGN KEY ([CardTypeID])    REFERENCES [dbo].[CardType]   ([CardTypeID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Card]) = 0
INSERT INTO [dbo].[Card] ([CardID], [CardAccountID], [CardTypeID], [CardNumber], [ActivationDate], [ExpirationDate], [CVV])
VALUES
(1, 110, 1, '4111111111111111', '2024-01-01T09:00:00', '20271231', '123'), -- John Doe, Visa
(2, 120, 2, '5500000000000004', '2024-05-21T10:00:00', '20261130', '456'), -- Jane Smith, MasterCard
(3, 130, 1, '4007000000456027', '2024-08-15T13:00:00', '20280531', '789'),    -- Michael Johnson, Visa
(4, 140, 3, '3782822463100305', '2024-02-03T16:00:00', '20250930', '012'), -- Emily Brown, American Express
(5, 150, 2, '5105105105105100', '2024-12-11T20:00:00', '20270831', '345') -- David Wilson, MasterCard
GO










-- ==============================================================================
-- Create Transaction table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Transaction')
EXEC ('CREATE TABLE [dbo].[Transaction]
(
    [TransactionID]             UNIQUEIDENTIFIER     NOT NULL   PRIMARY KEY DEFAULT NEWID(),
    [CardID]                    INT                  NOT NULL,
    [TransactionTypeID]         INT                  NOT NULL,
    [TransactionStatusID]       INT                  NOT NULL,
    [MerchantID]                INT                  NOT NULL,
    [CurrencyID]                INT                  NOT NULL,
    [Amount]                    DECIMAL(18,2)        NOT NULL,
    [TransactionDate]           DATETIME2            NOT NULL,

    CONSTRAINT [FK_Transaction_Card] FOREIGN KEY ([CardID]) REFERENCES [dbo].[Card]([CardID]),
    CONSTRAINT [FK_Transaction_TransactionType] FOREIGN KEY ([TransactionTypeID]) REFERENCES [dbo].[TransactionType]([TransactionTypeID]),
    CONSTRAINT [FK_Transaction_TransactionStatus] FOREIGN KEY ([TransactionStatusID]) REFERENCES [dbo].[TransactionStatus]([TransactionStatusID]),
    CONSTRAINT [FK_Transaction_Merchant] FOREIGN KEY ([MerchantID]) REFERENCES [dbo].[Merchant]([MerchantID]),
    CONSTRAINT [FK_Transaction_Currency] FOREIGN KEY ([CurrencyID]) REFERENCES [dbo].[Currency]([CurrencyID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Transaction]) = 0
INSERT INTO [dbo].[Transaction] ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate])
VALUES
(1, 1, 2,  1, 1, 120.50, '2025-01-01T08:00:00')
GO




-- ==============================================================================
-- Create Payments table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Payments')
EXEC ('CREATE TABLE [dbo].[Payments]
(
    [PaymentID]         UNIQUEIDENTIFIER                PRIMARY KEY DEFAULT NEWID(),
    [CardAccountID]     INT                 NOT NULL,
    [Amount]            DECIMAL(18,2)       NOT NULL,
    [CurrencyID]        INT                 NOT NULL,
    [PaymentDate]       DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME()

    CONSTRAINT [FK_Payments_CardAccount] FOREIGN KEY ([CardAccountID]) REFERENCES [dbo].[CardAccount]([CardAccountID]),
    CONSTRAINT [FK_Payments_Currency]    FOREIGN KEY ([CurrencyID])    REFERENCES [dbo].[Currency]([CurrencyID])
)')
GO













-- =======================================
-- Create Stored Procedure: Insert New Transaction
-- =======================================
CREATE OR ALTER PROCEDURE [dbo].[usp_insert_random_transaction]
    @days int = 0
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lasttransaction DATETIME2;
    DECLARE @newtransaction DATETIME2;
    DECLARE @minutesToAdd INT;
    DECLARE @hour INT;

    DECLARE @card INT;
    DECLARE @transactionType INT;
    DECLARE @transactionStatus INT;
    DECLARE @merchant INT;
    DECLARE @currency INT;
    DECLARE @amount DECIMAL(18,2);
    DECLARE @initaltime DATETIME2
    DECLARE @continue BIT = 1

    SELECT @initaltime= MAX([TransactionDate]) FROM [dbo].[Transaction]

    WHILE @continue = 1
    BEGIN



        -- 1. Get the maximum [TransactionDate]
        SELECT @lasttransaction = MAX([TransactionDate]) FROM [dbo].[Transaction];


        -- 2. Add random minutes (5–20)
        IF DAY(@lasttransaction) BETWEEN 5 AND 25
            SET @minutesToAdd = 5 + ABS(CHECKSUM(NEWID())) % 16;
        ELSE
            SET @minutesToAdd = 2 + ABS(CHECKSUM(NEWID())) % 9;
        SET @newtransaction = DATEADD(MINUTE, @minutesToAdd, @lasttransaction);
        SET @newtransaction = DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % 50, @newtransaction);

        -- 3. If the hour > 23, roll to next day +9h
        SET @hour = DATEPART(HOUR, @newtransaction);
        IF @hour >= 23
            SET @newtransaction = DATEADD(HOUR, 9, @newtransaction);

        -- 4. Random card between 1 and 5
        SET @card = 1 + ABS(CHECKSUM(NEWID())) % 5;

        -- 5. Random TransactionStatus between 2 and 3
        SET @transactionStatus = IIF (ABS(CHECKSUM(NEWID())) % 100 < 5, 3, 4)

        -- 6. Random Merchant between 1 and 30
        SET @merchant = 1 + ABS(CHECKSUM(NEWID())) % 30;

        -- 7. Random Currency between 1 and 2
        SELECT @currency = ca.[CurrencyID]
        FROM
            [dbo].[Card] AS c
            JOIN [dbo].[CardAccount] AS ca ON c.[CardAccountID] = ca.[CardAccountID]
        WHERE
            c.[CardID] = @card
        IF ABS(CHECKSUM(NEWID())) % 100 < 10
            SET @currency = 1 + ABS(CHECKSUM(NEWID())) % 2;

        -- 8. Random TransactionType (1 = 95%, others share 5%)
        SET @transactionType = 1

        -- 9. Random Amount between 10.00 and 1000.00
        SET @amount = CAST(10 + (ABS(CHECKSUM(NEWID())) % 991) + (ABS(CHECKSUM(NEWID())) % 100) * 0.01 AS DECIMAL(18,2));




        -- 10. Insert into Transaction table
        INSERT INTO [dbo].[Transaction]
            ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate])
        VALUES
            (@card, @transactionType, @transactionStatus, @merchant, @currency, @amount, @newtransaction);


        IF ABS(CHECKSUM(NEWID())) % 100 <= 2 AND @transactionStatus = 4
        BEGIN
            SET @amount = @amount * 0.2
            SET @newtransaction = DATEADD(SECOND, 1, @newtransaction)
            INSERT INTO [dbo].[Transaction]
                ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate])
            VALUES
                (@card, @transactionType, @transactionStatus, @merchant, @currency, @amount, @newtransaction);
        END


        IF MONTH(@lasttransaction) <> MONTH(@newtransaction)
        BEGIN
            INSERT INTO [dbo].[Payments] (
                [CardAccountID],
                [CurrencyID],
                [PaymentDate],
                [Amount]
            )
            SELECT
                ca.[CardAccountID],
                ca.[CurrencyID],
                [PaymentDate] = DATEADD(MONTH, DATEDIFF(MONTH, 0, @newtransaction), 0),
                SUM(t.[Amount]) AS [Amount]
            FROM
                [dbo].[Transaction] AS t
                JOIN [dbo].[Card] AS c ON t.[CardID] = c.[CardID]
                JOIN [dbo].[CardAccount] AS ca ON c.[CardAccountID] = ca.[CardAccountID]
            WHERE
                t.[TransactionStatusID] IN (2,4) -- Approved or Settled
                AND DATEADD(MONTH, DATEDIFF(MONTH, 0, @lasttransaction), 0) <= t.[TransactionDate] -- Start of last month
                AND t.[TransactionDate] < DATEADD(MONTH, DATEDIFF(MONTH, 0, @newtransaction), 0) -- Start of current month
            GROUP BY
                ca.[CardAccountID],
                ca.[CurrencyID]
            HAVING
                SUM(t.[Amount]) > 0
        END

        IF DATEDIFF(HOUR, @initaltime, @newtransaction) >= @days * 24
            SET @continue = 0
    END

END
GO


DECLARE @currenttime DATETIME2, @daysdiff int
SELECT @currenttime= MAX([TransactionDate]) FROM [dbo].[Transaction]
SET @daysdiff = DATEDIFF(DAY, @currenttime, DATEADD(DAY, -5, GETDATE()))

IF @daysdiff > 0
    EXEC [dbo].[usp_insert_random_transaction] @days = @daysdiff
ELSE
    DELETE FROM [dbo].[Transaction] WHERE [TransactionDate] > DATEADD(DAY, -5, GETDATE())

GO
