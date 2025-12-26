EXEC sales.usp_pattern3_SaleOrder_Parallel
    @PartitionNumber = 1,
    @TotalPartitions = 4,
    @StartDate = '2023-01-01',
    @EndDate   = '2023-03-31',
    @BatchName = 'BATCH_2023';

EXEC sales.usp_pattern3_SaleOrder_Parallel
    @PartitionNumber = 2,
    @TotalPartitions = 4,
    @StartDate = '2023-01-01',
    @EndDate   = '2023-03-31',
    @BatchName = 'BATCH_2023';

    EXEC sales.usp_pattern3_SaleOrder_Parallel
    @PartitionNumber = 3,
    @TotalPartitions = 4,
    @StartDate = '2023-01-01',
    @EndDate   = '2023-03-31',
    @BatchName = 'BATCH_2023';

EXEC sales.usp_pattern3_SaleOrder_Parallel
    @PartitionNumber = 4,
    @TotalPartitions = 4,
    @StartDate = '2023-01-01',
    @EndDate   = '2023-03-31',
    @BatchName = 'BATCH_2023';
