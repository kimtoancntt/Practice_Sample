CREATE OR ALTER PROCEDURE sales.usp_pattern3_SaleOrder_Parallel
	@StartDate DATE,
    @EndDate DATE,
    @BatchName VARCHAR(100),
    @PartitionNumber INT, -- Which partition to process (1, 2, 3, 4...)
    @TotalPartitions INT = 4
AS
BEGIN
	SET NOCOUNT ON;

    DECLARE @RunID INT, @Total INT = 0, @Failed INT = 0;

	INSERT INTO logging.MigrationRun (TableName, MigrationBatch, ApproachName, StartDate, EndDate, StartTime, Status)
    VALUES ('SalesOrder', @BatchName + '_P' + CAST(@PartitionNumber AS VARCHAR), 
            'Pattern3-Parallel', @StartDate, @EndDate, GETDATE(), 'Running');
    SET @RunID = SCOPE_IDENTITY();

	BEGIN TRY

	    DECLARE @MinID BIGINT, @MaxID BIGINT, @PartitionSize BIGINT;
        
        -- Get full range
        SELECT @MinID = MIN(OrderID), @MaxID = MAX(OrderID)
        FROM OldData.SalesOrder WITH (NOLOCK)
        WHERE OrderDate >= @StartDate AND OrderDate < DATEADD(DAY, 1, @EndDate);

		        -- Calculate this partition's range
        SET @PartitionSize = (@MaxID - @MinID + 1) / @TotalPartitions;
        DECLARE @PartMinID BIGINT = @MinID + (@PartitionNumber - 1) * @PartitionSize;
        DECLARE @PartMaxID BIGINT = CASE 
										WHEN @PartitionNumber = @TotalPartitions THEN @MaxID
									ELSE @MinID + @PartitionNumber * @PartitionSize - 1
									END;
        
		PRINT 'Partition ' + CAST(@PartitionNumber AS VARCHAR) + ' processing OrderID ' + 
              CAST(@PartMinID AS VARCHAR) + ' to ' + CAST(@PartMaxID AS VARCHAR);		

    -- Temp table for this batch
        CREATE TABLE #BatchData (
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
            IsValid BIT DEFAULT 1
        );
        
        -- Extract and transform
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
                WHEN c.CustomerID IS NULL OR p.ProductID IS NULL OR s.Quantity <= 0 
                THEN 0 ELSE 1 
            END
        FROM OldData.SalesOrder s WITH (NOLOCK)
        LEFT JOIN Sales.Customer c ON s.CustomerEmail = c.CustomerEmail
        LEFT JOIN Sales.Product p ON s.ProductName = p.ProductName
        LEFT JOIN Sales.ShippingAddress sa ON s.ShippingAddress = sa.AddressText
        WHERE s.OrderID BETWEEN @PartMinID AND @PartMaxID
        AND s.OrderDate >= @StartDate 
        AND s.OrderDate < DATEADD(DAY, 1, @EndDate)
        AND NOT EXISTS (
            SELECT 1 FROM logging.MigrationRowDetail t 
            WHERE t.ReferenceID = s.OrderID 
            AND t.MigrationStatus = 'Completed'
            AND t.SourceTable = 'SalesOrder'
        );

		DECLARE @BatchRows INT = @@ROWCOUNT;
        DECLARE @ValidRows INT = (SELECT COUNT(*) FROM #BatchData WHERE IsValid = 1);
        DECLARE @InvalidRows INT = @BatchRows - @ValidRows;

		-- Insert valid records
        IF @ValidRows > 0
        BEGIN
            INSERT INTO Sales.SaleOrder (
                OrderID, OrderDate, CustomerID, ProductID, Quantity,
                UnitPrice, TotalAmount, OrderStatus, ShippingAddressID, Notes
            )
            SELECT OrderID, OrderDate, CustomerID, ProductID, Quantity,
                    UnitPrice, TotalAmount, OrderStatus, AddressID, Notes
            FROM #BatchData WHERE IsValid = 1;
            
            -- Track successful migrations
            INSERT INTO logging.MigrationRowDetail (ReferenceID, SourceTable, DestinationTable, MigrationBatch, MigrationStatus)
            SELECT OrderID, 'SalesOrderOld', 'SalesOrder', @BatchName + '_P' + CAST(@PartitionNumber AS VARCHAR), 'Completed'
            FROM #BatchData WHERE IsValid = 1;
        END

        -- Track failed records
        IF @InvalidRows > 0
        BEGIN
            INSERT INTO logging.MigrationRowDetail (ReferenceID, SourceTable, DestinationTable, MigrationBatch, MigrationStatus, ErrorMessage)
            SELECT OrderID, 'SalesOrderOld', 'SalesOrder', @BatchName, 'Failed', 'Validation failed'
            FROM #BatchData WHERE IsValid = 0;
            
            INSERT INTO Logging.MigrationError (RunID, SourceOrderID, ErrorType, ErrorMessage)
            SELECT @RunID, OrderID, 'Validation', 'Missing reference data'
            FROM #BatchData WHERE IsValid = 0;
        END
        
        SET @Total += @ValidRows;
        SET @Failed += @InvalidRows;

		-- Complete
        UPDATE logging.MigrationRun
        SET Status = 'Completed', EndTime = GETDATE(), 
            RowsExtracted = @Total + @Failed, RowsInserted = @Total, RowsFailed = @Failed,
            CurrentCheckpoint = @MaxID
        WHERE RunID = @RunID;
        
        PRINT 'Migration completed: ' + @BatchName + '_P' + CAST(@PartitionNumber AS VARCHAR) + CAST(@Total AS VARCHAR) + ' successful, ' + CAST(@Failed AS VARCHAR) + ' failed';

        DROP TABLE #BatchData;

	END TRY
	BEGIN CATCH

		UPDATE logging.MigrationRun
		SET Status = 'Failed'
			, EndTime = GETDATE()
		WHERE RunID = @RunID;

        THROW;
	END CATCH
END