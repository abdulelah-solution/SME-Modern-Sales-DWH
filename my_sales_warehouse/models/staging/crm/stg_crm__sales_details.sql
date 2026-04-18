WITH 

source_data AS (

    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        dwh_load_date
    FROM 
        {{ source('crm', 'crm_sales_details') }}

),

sales_details_deduplicated AS (

    SELECT
        ROW_NUMBER() OVER(PARTITION BY sls_ord_num, sls_prd_key ORDER BY dwh_load_date DESC) AS flag,
        *
    FROM
        source_data

),

sales_details_formatted AS (

    SELECT
        sls_ord_num                                                                 AS order_number,
        sls_prd_key                                                                 AS product_number,
        sls_cust_id                                                                 AS customer_number,
        -- Transform and validate Order Date
        CASE
            WHEN LEN(sls_order_dt) != 8 OR sls_order_dt <= 0 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
        END                                                                         AS ordered_date,
        -- Transform and validate Shipping Date
        CASE
            WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt <= 0 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
        END                                                                         AS shipping_date,
        -- Transform and validate Due Date
        CASE
            WHEN LEN(sls_due_dt) != 8 OR sls_due_dt <= 0 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
        END                                                                         AS due_date,
        -- Logical Reconciliation: Re-calculate Sales if invalid or inconsistent
        CASE
            WHEN sls_sales != sls_quantity * sls_price
                OR sls_sales <= 0
                OR sls_sales IS NULL 
                    THEN ABS(sls_quantity * sls_price)
            ELSE ABS(sls_sales)
        END                                                                         AS sales_amount,
        -- Logical Imputation: Derive Quantity if missing/invalid
        CASE
            WHEN sls_quantity <= 0 OR sls_quantity IS NULL
                    THEN CAST(sls_sales / NULLIF(sls_price, 0) AS INT)
            ELSE ABS(sls_quantity)
        END                                                                         AS quantity,
        -- Logical Imputation: Derive Price if missing/invalid
        CASE
            WHEN sls_price <= 0 OR sls_price IS NULL
                    THEN CAST(sls_sales / NULLIF(sls_quantity, 0) AS DECIMAL)
            ELSE ABS(sls_price)
        END                                                                         AS price,
        dwh_load_date
    FROM 
        sales_details_deduplicated
    WHERE
    -- Keep only the latest record per combination of order and product
    flag = 1

),

sales_details_filtered AS (

    SELECT * FROM sales_details_formatted
    WHERE 
        order_number IS NOT NULL
        AND order_number != ''
        AND ordered_date IS NOT NULL
        
)

SELECT * FROM sales_details_filtered