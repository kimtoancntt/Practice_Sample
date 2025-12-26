CREATE OR ALTER PROCEDURE logging.usp_Clean_All_Migration_Data
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        /* =====================
           CLEAN TARGET DATA
        ===================== */
        TRUNCATE TABLE Sales.SaleOrder;

        /* =====================
           CLEAN LOGGING DATA
           (DELETE due to FK)
        ===================== */
        DELETE FROM logging.MigrationError;
        DELETE FROM logging.MigrationRowDetail;
        DELETE FROM logging.MigrationRunDetail;
        DELETE FROM logging.MigrationRun;

        /* =====================
           RESET IDENTITY
        ===================== */

        DBCC CHECKIDENT ('logging.MigrationRun', RESEED, 0);
        DBCC CHECKIDENT ('logging.MigrationRunDetail', RESEED, 0);
        DBCC CHECKIDENT ('logging.MigrationError', RESEED, 0);

        COMMIT TRANSACTION;

        PRINT 'ALL migration data cleaned successfully';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH
END
GO