-- =============================================
-- PART 1: CREATE SAMPLE DATABASE
-- =============================================
USE master
GO

DROP DATABASE IF EXISTS MigrationSample;
GO

CREATE DATABASE MigrationSample;
GO

USE MigrationSample;
GO

DROP SCHEMA IF EXISTS Sales;
GO
CREATE SCHEMA Sales;
GO

DROP SCHEMA IF EXISTS OldData;
GO
CREATE SCHEMA OldData;
GO

DROP SCHEMA IF EXISTS Logging;
GO
CREATE SCHEMA Logging;
GO
-- =============================================
-- PART 3: CREATE ALL TABLES
-- =============================================


-- =============================================
-- 3.1 Main Tables
-- =============================================
DROP TABLE IF EXISTS Sales.Customer;
CREATE TABLE Sales.Customer
(
	CustomerID INT IDENTITY(1, 1) PRIMARY KEY,
	CustomerName VARCHAR(100) NOT NULL,
	CustomerEmail VARCHAR(100) NOT NULL UNIQUE,
	CustomerType VARCHAR(50),
	Country VARCHAR(50)
);
GO

DROP TABLE IF EXISTS Sales.Product;
CREATE TABLE Sales.Product
(
	ProductID INT IDENTITY(1, 1) PRIMARY KEY,
	ProductName VARCHAR(100) NOT NULL UNIQUE,
	ProductCategory VARCHAR(50),
	UnitCost DECIMAL(18, 2)
);
GO

DROP TABLE IF EXISTS Sales.ShippingAddress;
CREATE TABLE Sales.ShippingAddress
(
	AddressID INT IDENTITY(1, 1) PRIMARY KEY,
	AddressText VARCHAR(500) NOT NULL,
	City VARCHAR(50),
	Country VARCHAR(50)
);
GO

DROP TABLE IF EXISTS Sales.SaleOrder;
CREATE TABLE Sales.SaleOrder
(
	OrderID BIGINT PRIMARY KEY,
    OrderDate DATE NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    OrderStatus VARCHAR(50) NOT NULL,
    ShippingAddressID INT,
    Notes NVARCHAR(MAX),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE(),

    ADD CONSTRAINT FK_SaleOrder_Customer FOREIGN KEY (CustomerID) REFERENCES Sales.Customer(CustomerID),
    ADD CONSTRAINT FK_SaleOrder_Product FOREIGN KEY (ProductID) REFERENCES Sales.Product(ProductID),
    ADD CONSTRAINT FK_SaleOrder_ShippingAddress FOREIGN KEY (ShippingAddressID) REFERENCES Sales.ShippingAddress(AddressID)
);

CREATE INDEX IX_SalesOrder_OrderDate ON Sales.SaleOrder(OrderDate);
CREATE INDEX IX_SalesOrder_Customer ON Sales.SaleOrder(CustomerID);
GO

DROP TABLE IF EXISTS OldData.SalesOrder;
CREATE TABLE OldData.SalesOrder
(
	OrderID BIGINT PRIMARY KEY,
    OrderDate DATETIME NOT NULL,
    CustomerName NVARCHAR(200),
    CustomerEmail NVARCHAR(200),
    ProductName NVARCHAR(200),
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    OrderStatus VARCHAR(50),
    ShippingAddress NVARCHAR(500),
    Notes NVARCHAR(MAX),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);
CREATE INDEX IX_SalesOrderOld_OrderDate ON OldData.SalesOrder(OrderDate);
CREATE INDEX IX_SalesOrderOld_OrderID_Date ON OldData.SalesOrder(OrderID, OrderDate);
GO


-- =============================================
-- 3.2 Migration Tracking table
-- =============================================

DROP TABLE IF EXISTS logging.MigrationRowDetail;
CREATE TABLE logging.MigrationRowDetail
(
	ReferenceID BIGINT NOT NULL,
	SourceTable VARCHAR(100) NOT NULL,
    DestinationTable VARCHAR(100) NOT NULL,
    MigrationBatch VARCHAR(100) NOT NULL,
    MigrationDate DATETIME2 DEFAULT GETDATE(),
    MigrationStatus VARCHAR(20) DEFAULT 'Completed',
    ErrorMessage NVARCHAR(500) NULL,

	CONSTRAINT PK_MigrationDetail PRIMARY KEY (ReferenceID, SourceTable)
);
CREATE INDEX IX_MigrationTracker_Batch ON logging.MigrationRowDetail(MigrationBatch, MigrationStatus);
CREATE INDEX IX_MigrationTracker_Date ON logging.MigrationRowDetail(MigrationDate);
GO

DROP TABLE IF EXISTS logging.MigrationRun;
CREATE TABLE logging.MigrationRun
(
	RunID INT IDENTITY(1, 1) PRIMARY KEY,
	TableName VARCHAR(100),
    MigrationBatch VARCHAR(100),
    ApproachName VARCHAR(100),
    StartDate DATE,
    EndDate DATE,
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status VARCHAR(20),
    RowsExtracted INT DEFAULT 0,
    RowsInserted INT DEFAULT 0,
    RowsFailed INT DEFAULT 0,
    CurrentCheckpoint BIGINT NULL
);
GO

DROP TABLE IF EXISTS Logging.MigrationRunDetail;
CREATE TABLE Logging.MigrationRunDetail
(
	ProgressID INT IDENTITY(1,1) PRIMARY KEY,
    RunID INT,
    BatchNumber INT,
    StartOrderID BIGINT,
    EndOrderID BIGINT,
    RowsProcessed INT,
    RowsSucceeded INT,
    RowsFailed INT,
    StartTime DATETIME2,
    EndTime DATETIME2,

    ADD CONSTRAINT FK_MigrationRunDetail_Run FOREIGN KEY (RunID) REFERENCES Logging.MigrationRun(RunID)
);
GO

DROP TABLE IF EXISTS Logging.MigrationError;
CREATE TABLE Logging.MigrationError
(
	ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    RunID INT,
    SourceOrderID BIGINT,
    ErrorType VARCHAR(50),
    ErrorMessage NVARCHAR(500),
    ErrorTime DATETIME2 DEFAULT GETDATE(),

    ADD CONSTRAINT FK_MigrationError_Run FOREIGN KEY (RunID) REFERENCES Logging.MigrationRun(RunID)
);
GO

-- =============================================
-- PART 4: GENERATE MOCKUP DATA
-- =============================================

-- Customers
PRINT 'Creating 500 customers...';
DECLARE @i INT = 1;
WHILE @i <= 500
BEGIN
    INSERT INTO Sales.Customer (CustomerName, CustomerEmail, CustomerType, Country)
    VALUES (
        'Customer_' + RIGHT('000' + CAST(@i AS VARCHAR), 3),
        'customer' + CAST(@i AS VARCHAR) + '@example.com',
        CHOOSE((@i % 3) + 1, 'Enterprise', 'SMB', 'Individual'),
        CHOOSE((@i % 5) + 1, 'USA', 'UK', 'Germany', 'France', 'Japan')
    );
    SET @i += 1;
END;


-- Products
PRINT 'Creating 200 products...';
SET @i = 1;
WHILE @i <= 200
BEGIN
    INSERT INTO Sales.Product (ProductName, ProductCategory, UnitCost)
    VALUES (
        'Product_' + RIGHT('000' + CAST(@i AS VARCHAR), 3),
        CHOOSE((@i % 5) + 1, 'Electronics', 'Clothing', 'Food', 'Books', 'Toys'),
        ROUND(10 + (@i * 0.5), 2)
    );
    SET @i += 1;
END;

-- Addresses
PRINT 'Creating 100 shipping addresses...';
SET @i = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO Sales.ShippingAddress (AddressText, City, Country)
    VALUES (
        CAST(@i AS VARCHAR) + ' Main Street',
        'City_' + CAST(@i AS VARCHAR),
        CHOOSE((@i % 5) + 1, 'USA', 'UK', 'Germany', 'France', 'Japan')
    );
    SET @i += 1;
END

-- Generate Sales Orders

IF OBJECT_ID('tempdb..#Nums') IS NOT NULL DROP TABLE #Nums;

DROP TABLE IF EXISTS #Nums;
;WITH N AS (
    SELECT TOP (100000)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
SELECT n INTO #Nums FROM N;

CREATE CLUSTERED INDEX IX_Nums ON #Nums(n);
GO

DECLARE @BaseDate DATE = '2023-01-01';
DECLARE @TotalWeeks INT = 104;            -- 2 years
DECLARE @OrdersPerWeek INT = 96000;      -- ~30M total
DECLARE @StartOrderID BIGINT = 1000000;
DECLARE @Week INT = 0;

WHILE @Week < @TotalWeeks
BEGIN
    DECLARE @WeekStart DATE = DATEADD(WEEK, @Week, @BaseDate);

    PRINT CONCAT(
        'Generating week ',
        @Week + 1,
        ' - ',
        CONVERT(VARCHAR, @WeekStart)
    );

    INSERT INTO OldData.SalesOrder (
        OrderID,
        OrderDate,
        CustomerName,
        CustomerEmail,
        ProductName,
        Quantity,
        UnitPrice,
        TotalAmount,
        OrderStatus,
        ShippingAddress,
        Notes
    )
    SELECT
        @StartOrderID + (@Week * @OrdersPerWeek) + n.n AS OrderID,
        DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 7, @WeekStart),

        c.CustomerName,
        c.CustomerEmail,
        p.ProductName,

        q.Qty,
        p.UnitCost * (1 + r.PriceAdj / 100.0),
        q.Qty * p.UnitCost * (1 + r.PriceAdj / 100.0),

        s.Status,
        sa.AddressText,
        CASE WHEN r.Flag = 0 THEN 'Rush delivery' ELSE NULL END
    FROM #Nums n
    JOIN Sales.Customer c
        ON c.CustomerID = 1 + (n.n % 500)
    JOIN Sales.Product p
        ON p.ProductID = 1 + (n.n % 200)
    JOIN Sales.ShippingAddress sa
        ON sa.AddressID = 1 + (n.n % 100)
    CROSS APPLY (
        SELECT 1 + ABS(CHECKSUM(NEWID())) % 10 AS Qty
    ) q
    CROSS APPLY (
        SELECT
            ABS(CHECKSUM(NEWID())) % 50 AS PriceAdj,
            ABS(CHECKSUM(NEWID())) % 10 AS Flag
    ) r
    CROSS APPLY (
        SELECT v.Status
        FROM (VALUES
            (0, 'Pending'),
            (1, 'Processing'),
            (2, 'Shipped'),
            (3, 'Delivered'),
            (4, 'Completed')
        ) v (Idx, Status)
        WHERE v.Idx = ABS(CHECKSUM(NEWID())) % 5
    ) s
    WHERE n.n < @OrdersPerWeek;


    SET @Week += 1;
END
GO
