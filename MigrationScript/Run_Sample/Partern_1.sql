EXEC sales.usp_pattern1_SaleOrder_SimpleMigration
    @StartDate = '2023-01-01',
    @EndDate   = '2023-01-07',
    @BatchName = 'TEST_RUN_001',
    @BatchSize = 10000;