CREATE OR ALTER PROCEDURE sales.usp_pattern1_SaleOrder_SimpleMigration
    @StartDate DATE,
    @EndDate DATE,
    @BatchName VARCHAR(100),
    @BatchSize INT = 20000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @RunID INT,
        @Batch INT = 0,
        @Total INT = 0,
        @Failed INT = 0,
        @MinID BIGINT,
        @MaxID BIGINT,
        @CurrID BIGINT,
        @EndID BIGINT;

    /* =====================================================
       1. INIT RUN
    ===================================================== */
    INSERT INTO logging.MigrationRun
    (
        TableName, MigrationBatch, ApproachName,
        StartDate, EndDate, StartTime, Status
    )
    VALUES
    (
        'SalesOrder',
        @BatchName,
        'Pattern1-OneTime-NoResume',
        @StartDate,
        @EndDate,
        SYSDATETIME(),
        'Running'
    );

    SET @RunID = SCOPE_IDENTITY();

    /* =====================================================
       2. DETERMINE RANGE
    ===================================================== */
    SELECT
        @MinID = MIN(OrderID),
        @MaxID = MAX(OrderID)
    FROM OldData.SalesOrder
    WHERE OrderDate >= @StartDate
      AND OrderDate < DATEADD(DAY, 1, @EndDate);

    IF @MinID IS NULL
    BEGIN
        UPDATE logging.MigrationRun
        SET Status = 'Completed', EndTime = SYSDATETIME()
        WHERE RunID = @RunID;
        RETURN;
    END

    SET @CurrID = @MinID;

    /* =====================================================
       3. PREPARE TEMP TABLE
    ===================================================== */
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

    /* =====================================================
       4. MAIN LOOP
    ===================================================== */
    WHILE @CurrID <= @MaxID
    BEGIN
        SET @Batch += 1;
        SET @EndID = @CurrID + @BatchSize - 1;

        BEGIN TRY
            BEGIN TRANSACTION;

            TRUNCATE TABLE #BatchData;

            /* ---------- Extract + Transform ---------- */
            INSERT INTO #BatchData
            (
                OrderID, OrderDate, CustomerID, ProductID, AddressID,
                Quantity, UnitPrice, TotalAmount,
                OrderStatus, Notes, IsValid
            )
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
            LEFT JOIN Sales.Customer c
                ON s.CustomerEmail = c.CustomerEmail
            LEFT JOIN Sales.Product p
                ON s.ProductName = p.ProductName
            LEFT JOIN Sales.ShippingAddress sa
                ON s.ShippingAddress = sa.AddressText
            WHERE s.OrderID BETWEEN @CurrID AND @EndID;

            DECLARE
                @BatchRows INT = @@ROWCOUNT,
                @ValidRows INT,
                @InvalidRows INT;

            SELECT @ValidRows = COUNT(*) FROM #BatchData WHERE IsValid = 1;
            SET @InvalidRows = @BatchRows - @ValidRows;

            /* ---------- Insert valid ---------- */
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

                INSERT INTO logging.MigrationRowDetail
                (
                    ReferenceID, SourceTable, DestinationTable,
                    MigrationBatch, MigrationStatus
                )
                SELECT
                    OrderID, 'SalesOrderOld', 'SalesOrder',
                    @BatchName, 'Completed'
                FROM #BatchData
                WHERE IsValid = 1;
            END

            /* ---------- Log invalid ---------- */
            IF @InvalidRows > 0
            BEGIN
                INSERT INTO logging.MigrationRowDetail
                (
                    ReferenceID, SourceTable, DestinationTable,
                    MigrationBatch, MigrationStatus, ErrorMessage
                )
                SELECT
                    OrderID, 'SalesOrderOld', 'SalesOrder',
                    @BatchName, 'Failed', 'Validation failed'
                FROM #BatchData
                WHERE IsValid = 0;

                INSERT INTO logging.MigrationError
                (
                    RunID, SourceOrderID, ErrorType, ErrorMessage
                )
                SELECT
                    @RunID, OrderID, 'Validation',
                    'Missing reference data'
                FROM #BatchData
                WHERE IsValid = 0;
            END

            /* ---------- Batch audit ---------- */
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
                SYSDATETIME(), SYSDATETIME()
            );

            SET @Total += @ValidRows;
            SET @Failed += @InvalidRows;

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;

            UPDATE logging.MigrationRun
            SET Status = 'Failed',
                EndTime = SYSDATETIME()
            WHERE RunID = @RunID;

            THROW;
        END CATCH;

        SET @CurrID = @EndID + 1;

        IF @Batch % 5 = 0
            PRINT CONCAT(
                'Batch ', @Batch,
                ' | Success: ', @Total,
                ' | Failed: ', @Failed
            );
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
END
GO
