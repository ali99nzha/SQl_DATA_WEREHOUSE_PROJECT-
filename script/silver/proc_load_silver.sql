CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @startime DATETIME,
        @endtime DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE();

    PRINT '=============================';
    PRINT 'LOADING SILVER LAYER STARTED';
    PRINT '=============================';

    -----------------------------------------------------------
    -- [1] Load CRM Product Data (crm_prd_info)
    --     - Cleans product line values
    --     - Calculates product end date using LEAD()
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> INSERT INTO silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            crt_prd_id,
            cat_id,
            crt_prd_key,
            crd_prd_nm,
            crt_prd_cost,
            crt_ped_line,
            crt_ord_start_dt,
            crt_prdend_dt
        )
        SELECT 
            crt_prd_id,
            REPLACE(SUBSTRING(crt_prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(crt_prd_key, 7, LEN(crt_prd_key)) AS crt_prd_key,
            crd_prd_nm,  
            ISNULL(crt_prd_cost, 0) AS crt_prd_cost,
            CASE UPPER(TRIM(crt_ped_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS crt_ped_line,
            crt_ord_start_dt,
            LEAD(crt_ord_start_dt) OVER (
                PARTITION BY crt_prd_key 
                ORDER BY crt_ord_start_dt
            ) AS crt_prdend_dt
        FROM bronze.crm_prd_info;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (crm_prd_info): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN crm_prd_info: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [2] Load CRM Sales Details (crm_sales_details)
    --     - Validates date formats
    --     - Recalculates sales if inconsistent
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> INSERT INTO silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num, 
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_sales <= 0 
                 THEN sls_sales / NULLIF(sls_price, 0)
                 ELSE sls_price END AS sls_price
        FROM bronze.crm_sales_details;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (crm_sales_details): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN crm_sales_details: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [3] Load CRM Customer Info (crm_cust_info)
    --     - Normalizes gender & marital status
    --     - Keeps latest record per customer
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> INSERT INTO silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) AS t
        WHERE flag_last = 1;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (crm_cust_info): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN crm_cust_info: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [4] Load ERP Customer Data (erp_cust_az12)
    --     - Cleans customer IDs starting with NAS
    --     - Validates birthdate and gender
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> INSERT INTO silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (erp_cust_az12): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN erp_cust_az12: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [5] Load ERP Product Category Data (erp_px_cat_g1v2)
    --     - Copies data as-is from bronze
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> INSERT INTO silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (erp_px_cat_g1v2): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN erp_px_cat_g1v2: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [6] Load ERP Location Data (erp_loc_a101)
    --     - Removes dashes from CID
    --     - Standardizes country names
    -----------------------------------------------------------
    BEGIN TRY
        SET @startime = GETDATE();
        PRINT '>> TRUNCATE TABLE silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> INSERT INTO silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT  
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @endtime = GETDATE();
        PRINT 'Load Duration (erp_loc_a101): ' + CAST(DATEDIFF(second, @startime, @endtime) AS NVARCHAR) + ' sec';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '?? ERROR IN erp_loc_a101: ' + ERROR_MESSAGE();
    END CATCH;


    -----------------------------------------------------------
    -- [FINAL] Batch summary
    -----------------------------------------------------------
    SET @batch_end_time = GETDATE();
    PRINT '=============================';
    PRINT 'SILVER LAYER LOAD COMPLETED';
    PRINT 'Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec';
    PRINT '=============================';
END;
GO
