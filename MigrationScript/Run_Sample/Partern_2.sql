EXEC sales.usp_pattern2_SaleOrder_CheckpointResume
    @StartDate = '2023-01-01',
    @EndDate   = '2023-01-07',
    @BatchName = 'TEST_RUN_002',
    @BatchSize = 10000;