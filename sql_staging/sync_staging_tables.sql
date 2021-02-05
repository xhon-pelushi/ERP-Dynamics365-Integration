-- Staging Tables for ERP-CRM Synchronization
-- SQL Server database schema for data transformation

USE ERPIntegration;
GO

-- Sync Log Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SyncLog')
BEGIN
    CREATE TABLE [dbo].[SyncLog] (
        [SyncLogID] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [SAPDocEntry] INT NOT NULL,
        [SAPDocNum] INT,
        [DynamicsID] NVARCHAR(100),
        [EntityType] NVARCHAR(50) DEFAULT 'Opportunity',
        [Status] NVARCHAR(20) NOT NULL, -- success, failed, pending
        [ErrorMessage] NVARCHAR(MAX),
        [SyncDate] DATETIME DEFAULT GETDATE(),
        [RetryCount] INT DEFAULT 0,
        CONSTRAINT UK_SyncLog_SAPDocEntry UNIQUE (SAPDocEntry)
    );
    
    CREATE INDEX IX_SyncLog_Status ON SyncLog(Status);
    CREATE INDEX IX_SyncLog_SyncDate ON SyncLog(SyncDate);
END
GO

-- SAP B1 Staging Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Staging_SAPB1_Orders')
BEGIN
    CREATE TABLE [dbo].[Staging_SAPB1_Orders] (
        [StagingID] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [DocEntry] INT NOT NULL,
        [DocNum] INT,
        [CardCode] NVARCHAR(50),
        [CardName] NVARCHAR(200),
        [DocDate] DATETIME,
        [DocDueDate] DATETIME,
        [DocTotal] DECIMAL(19,6),
        [DocStatus] NVARCHAR(1),
        [ExtractedDate] DATETIME DEFAULT GETDATE(),
        [ProcessedDate] DATETIME,
        [ProcessedStatus] NVARCHAR(20), -- pending, processed, error
        CONSTRAINT UK_Staging_SAPB1_Orders_DocEntry UNIQUE (DocEntry)
    );
    
    CREATE INDEX IX_Staging_SAPB1_Orders_Status ON Staging_SAPB1_Orders(ProcessedStatus);
END
GO

-- Dynamics 365 Staging Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Staging_D365_Opportunities')
BEGIN
    CREATE TABLE [dbo].[Staging_D365_Opportunities] (
        [StagingID] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [OpportunityID] NVARCHAR(100),
        [Name] NVARCHAR(200),
        [EstimatedValue] DECIMAL(19,2),
        [EstimatedCloseDate] DATETIME,
        [CustomerID] NVARCHAR(100),
        [Status] NVARCHAR(50),
        [CreatedDate] DATETIME DEFAULT GETDATE(),
        [SAPDocEntry] INT,
        CONSTRAINT FK_Staging_D365_Opportunities_SAP FOREIGN KEY (SAPDocEntry) 
            REFERENCES Staging_SAPB1_Orders(DocEntry)
    );
    
    CREATE INDEX IX_Staging_D365_Opportunities_SAPDocEntry ON Staging_D365_Opportunities(SAPDocEntry);
END
GO

-- Customer Mapping Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CustomerMapping')
BEGIN
    CREATE TABLE [dbo].[CustomerMapping] (
        [MappingID] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [SAPCardCode] NVARCHAR(50) NOT NULL,
        [DynamicsAccountID] NVARCHAR(100) NOT NULL,
        [LastSyncedDate] DATETIME DEFAULT GETDATE(),
        CONSTRAINT UK_CustomerMapping_SAP UNIQUE (SAPCardCode),
        CONSTRAINT UK_CustomerMapping_D365 UNIQUE (DynamicsAccountID)
    );
    
    CREATE INDEX IX_CustomerMapping_SAP ON CustomerMapping(SAPCardCode);
END
GO

-- Sync Statistics View
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_SyncStatistics')
    DROP VIEW vw_SyncStatistics;
GO

CREATE VIEW [dbo].[vw_SyncStatistics]
AS
SELECT 
    CAST(SyncDate AS DATE) AS SyncDate,
    Status,
    COUNT(*) AS RecordCount,
    COUNT(DISTINCT SAPDocEntry) AS UniqueOrders,
    SUM(CASE WHEN Status = 'success' THEN 1 ELSE 0 END) AS SuccessCount,
    SUM(CASE WHEN Status = 'failed' THEN 1 ELSE 0 END) AS FailedCount
FROM SyncLog
GROUP BY CAST(SyncDate AS DATE), Status;
GO

-- Stored Procedure: Get Pending Syncs
CREATE OR ALTER PROCEDURE [dbo].[sp_GetPendingSyncs]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        s.DocEntry,
        s.DocNum,
        s.CardCode,
        s.CardName,
        s.DocDate,
        s.DocTotal,
        s.ExtractedDate,
        DATEDIFF(HOUR, s.ExtractedDate, GETDATE()) AS HoursPending
    FROM Staging_SAPB1_Orders s
    LEFT JOIN SyncLog sl ON s.DocEntry = sl.SAPDocEntry AND sl.Status = 'success'
    WHERE s.ProcessedStatus = 'pending'
        AND sl.SyncLogID IS NULL
    ORDER BY s.ExtractedDate;
END;
GO

-- Stored Procedure: Retry Failed Syncs
CREATE OR ALTER PROCEDURE [dbo].[sp_RetryFailedSyncs]
    @MaxRetries INT = 3
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE SyncLog
    SET Status = 'pending',
        RetryCount = RetryCount + 1
    WHERE Status = 'failed'
        AND RetryCount < @MaxRetries
        AND SyncDate < DATEADD(HOUR, -1, GETDATE()); -- Only retry after 1 hour
    
    SELECT @@ROWCOUNT AS RecordsRetried;
END;
GO










































