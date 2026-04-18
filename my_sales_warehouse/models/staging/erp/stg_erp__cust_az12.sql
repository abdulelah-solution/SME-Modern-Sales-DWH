WITH 

source_data AS (

    SELECT
        cid,
        bdate,
        gen,
        dwh_load_date
    FROM 
        {{ source('erp', 'erp_cust_az12') }}

),

cust_az12_transformed AS (

    SELECT 
        -- Standardizing Customer ID by removing 'NAS' prefix if present
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE TRIM(cid)
        END                                                                     AS customer_id,
        -- Validating birth dates to prevent future-dated entries
        CASE
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END                                                                     AS birth_date,
        -- Standardizing Gender labels to match the Data Warehouse convention
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
            ELSE 'n/a'
        END                                                                     AS gender,
        dwh_load_date
    FROM 
        source_data

),

cust_az12_filtered AS (

    SELECT * FROM cust_az12_transformed
    WHERE
        customer_id IS NOT NULL
        AND customer_id != ''

)

SELECT * FROM cust_az12_filtered
