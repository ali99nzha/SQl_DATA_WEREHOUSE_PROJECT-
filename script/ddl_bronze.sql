-- =====================================================
-- DROP AND RECREATE TABLE: bronze.crm_cust_info
-- =====================================================
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info ( 
    cst_id             INT,             -- Customer ID (numeric)
    cst_key            NVARCHAR(50),    -- Unique customer key
    cst_firstname      NVARCHAR(50),    -- Customer first name
    cst_lastname       NVARCHAR(50),    -- Customer last name
    cst_marital_status NVARCHAR(50),    -- Marital status (Single, Married, etc.)
    cst_gndr           NVARCHAR(50),    -- Gender (Male, Female, etc.)
    cst_create_date    DATE             -- Account creation date
);

-- =====================================================
-- DROP AND RECREATE TABLE: bronze.crm_prd_info
-- =====================================================
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info ( 
    crt_prd_id       INT,             -- Product ID (numeric)
    crt_prd_key      NVARCHAR(50),    -- Unique product key
    crd_prd_nm       NVARCHAR(50),    -- Product name
    crt_prd_cost     INT,             -- Product cost
    crt_ped_line     NVARCHAR(50),    -- Product line or category
    crt_ord_start_dt DATE,            -- Product start date
    crt_prdend_dt    DATE             -- Product end date
); 

-- =====================================================
-- DROP AND RECREATE TABLE: bronze.crm_sales_details
-- =====================================================
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details ( 
    sls_ord_num  NVARCHAR(50),  -- Sales order number
    sls_prd_key  NVARCHAR(50),  -- Product key (foreign key to crm_prd_info)
    sls_cust_id  INT,           -- Customer ID (foreign key to crm_cust_info)
    sls_order_dt INT,           -- Order date (should ideally be DATE type)
    sls_ship_dt  INT,           -- Shipping date
    sls_due_dt   INT,           -- Due date for delivery/payment
    sls_sales    INT,           -- Sales amount
    sls_quantity INT,           -- Quantity sold
    sls_price    INT            -- Unit price
); 

-- =====================================================
-- DROP AND RECREATE TABLE: bronze.erp_loc_a101
-- =====================================================
IF OBJECT_ID('erp_loc_a101') IS NOT NULL 
    DROP TABLE erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),  -- Customer or location ID
    cntry  NVARCHAR(50)   -- Country name or code
);

-- =====================================================
-- DROP AND RECREATE TABLE: bronze.erp_cust_az12
-- =====================================================
IF OBJECT_ID('erp_cust_az12') IS NOT NULL 
    DROP TABLE erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),  -- Customer ID
    bdate  DATE,          -- Birth date
    gen    NVARCHAR(50)   -- Gender
);

-- =====================================================
-- DROP AND RECREATE TABLE: bronze.erp_px_cat_g1v2
-- =====================================================
IF OBJECT_ID('bronze.erp_px_cat_g1v2') IS NOT NULL 
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),  -- Product or category ID
    cat          NVARCHAR(50),  -- Main category
    subcat       NVARCHAR(50),  -- Sub-category
    maintenance  NVARCHAR(50)   -- Maintenance level or notes
);
