CREATE OR ALTER PROCEDURE sales.usp_pattern2_SaleOrder_CheckpointResume
(
    @StartDate  DATE,
    @EndDate    DATE,
    @BatchName  VARCHAR(100),
    @BatchSize  INT = 100000
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @RunID INT,
        @ResumeFromID BIGINT = 0,
        @CurrID BIGINT,
        @MaxID BIGINT,
        @Batch INT = 0,
        @BatchRows INT,
        @ValidRows INT,
        @InvalidRows INT,
        @Total INT = 0,
        @Failed INT = 0;

    /* =======================
       INIT / RESUME
       ======================= */
    SELECT TOP 1 
        @RunID = RunID,
        @ResumeFromID = ISNULL(CurrentCheckpoint, 0)
    FROM logging.MigrationRun
    WHERE MigrationBatch = @BatchName
      AND Status IN ('Running','Paused')
    ORDER BY RunID DESC;

    IF @RunID IS NULL
    BEGIN
        INSERT INTO logging.MigrationRun
        (
            TableName, MigrationBatch, ApproachName,
            StartDate, EndDate, StartTime, Status,
            RowsInserted, RowsFailed
        )
        VALUES
        (
            'SalesOrder', @BatchName, 'Pattern2-CheckpointResume',
            @StartDate, @EndDate, GETDATE(), 'Running',
            0, 0
        );

        SET @RunID = SCOPE_IDENTITY();
    END
    ELSE
    BEGIN
        UPDATE logging.MigrationRun
        SET Status = 'Running', StartTime = GETDATE()
        WHERE RunID = @RunID;
    END

    /* =======================
       RANGE
       ======================= */
    SELECT 
        @CurrID = MIN(OrderID),
        @MaxID  = MAX(OrderID)
    FROM OldData.SalesOrder
    WHERE OrderDate >= @StartDate
      AND OrderDate < DATEADD(DAY,1,@EndDate)
      AND OrderID > @ResumeFromID;

    IF @CurrID IS NULL
    BEGIN
        UPDATE logging.MigrationRun
        SET Status = 'Completed', EndTime = GETDATE()
        WHERE RunID = @RunID;
        RETURN;
    END

    /* =======================
       TEMP TABLE (CREATE ONCE)
       ======================= */
    CREATE TABLE #BatchData
    (
        OrderID BIGINT PRIMARY KEY,
        OrderDate DATE,
        CustomerID INT,
        ProductID INT,
        AddressID INT,
        Quantity INT,
        UnitPrice DECIMAL(18,2),
        TotalAmount DECIMAL(18,2),
        OrderStatus VARCHAR(50),
        Notes NVARCHAR(MAX),
        IsValid BIT
    );

    BEGIN TRY
        WHILE @CurrID <= @MaxID
        BEGIN
            SET @Batch += 1;

            DECLARE 
                @EndID BIGINT = @CurrID + @BatchSize - 1,
                @BatchStart DATETIME2 = SYSDATETIME();

            TRUNCATE TABLE #BatchData;

            /* ===== Extract + Transform ===== */
            INSERT INTO #BatchData
            SELECT
                s.OrderID,
                CAST(s.OrderDate AS DATE),
                c.CustomerID,
                p.ProductID,
                sa.AddressID,
                s.Quantity,
                s.UnitPrice,
                s.TotalAmount,
                s.OrderStatus,
                s.Notes,
                CASE 
                    WHEN c.CustomerID IS NULL
                      OR p.ProductID IS NULL
                      OR s.Quantity <= 0
                    THEN 0 ELSE 1
                END
            FROM OldData.SalesOrder s
            LEFT JOIN Sales.Customer c ON s.CustomerEmail = c.CustomerEmail
            LEFT JOIN Sales.Product p ON s.ProductName = p.ProductName
            LEFT JOIN Sales.ShippingAddress sa ON s.ShippingAddress = sa.AddressText
            WHERE s.OrderID BETWEEN @CurrID AND @EndID
              AND s.OrderDate >= @StartDate
              AND s.OrderDate < DATEADD(DAY, 1, @EndDate)
              AND NOT EXISTS (
                    SELECT 1 FROM logging.MigrationRowDetail t 
                    WHERE t.ReferenceID = s.OrderID 
                    AND t.MigrationStatus = 'Completed'
                    AND t.SourceTable = 'SalesOrder'
                  )

            SET @BatchRows = @@ROWCOUNT;

            SELECT @ValidRows = COUNT(*) FROM #BatchData WHERE IsValid = 1;
            SET @InvalidRows = @BatchRows - @ValidRows;

            /* ===== Load ===== */
            IF @ValidRows > 0
            BEGIN
                INSERT INTO Sales.SaleOrder WITH (TABLOCK)
                (
                    OrderID, OrderDate, CustomerID, ProductID,
                    Quantity, UnitPrice, TotalAmount,
                    OrderStatus, ShippingAddressID, Notes
                )
                SELECT
                    OrderID, OrderDate, CustomerID, ProductID,
                    Quantity, UnitPrice, TotalAmount,
                    OrderStatus, AddressID, Notes
                FROM #BatchData
                WHERE IsValid = 1;
            END

            /* ===== Logging ===== */
            INSERT INTO logging.MigrationRunDetail
            (
                RunID, BatchNumber,
                StartOrderID, EndOrderID,
                RowsProcessed, RowsSucceeded, RowsFailed,
                StartTime, EndTime
            )
            VALUES
            (
                @RunID, @Batch,
                @CurrID, @EndID,
                @BatchRows, @ValidRows, @InvalidRows,
                @BatchStart, SYSDATETIME()
            );

            UPDATE logging.MigrationRun
            SET
                CurrentCheckpoint = @EndID
            WHERE RunID = @RunID;

            SET @CurrID = @EndID + 1;

            IF @Batch % 5 = 0
                PRINT CONCAT('Batch ', @Batch, ' | Success: ', @ValidRows, ' | Failed: ', @InvalidRows);

            SET @Total += @ValidRows;
            SET @Failed += @InvalidRows;
        END

    /* =====================================================
       5. COMPLETE RUN
    ===================================================== */
    UPDATE logging.MigrationRun
    SET Status = 'Completed',
        EndTime = SYSDATETIME(),
        RowsExtracted = @Total + @Failed,
        RowsInserted = @Total,
        RowsFailed = @Failed
    WHERE RunID = @RunID;

    PRINT CONCAT(
        'DONE | Success: ', @Total,
        ' | Failed: ', @Failed
    );
    END TRY
    BEGIN CATCH
        UPDATE logging.MigrationRun
        SET Status = 'Paused', EndTime = GETDATE()
        WHERE RunID = @RunID;
        THROW;
    END CATCH
END
GO
