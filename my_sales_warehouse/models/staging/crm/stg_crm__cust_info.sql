WITH 

source_data AS (

    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        dwh_load_date
    FROM 
        {{ source('crm', 'crm_cust_info') }}

),

cust_info_deduplicated AS (

    SELECT
        -- Using ROW_NUMBER to handle potential duplicates at the source level
        -- ensuring we only process the latest snapshot of each customer.
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag,
        *
    FROM 
        source_data

),

cust_info_transformed AS (

    SELECT
        cst_id                                                          AS customer_number,
        TRIM(cst_key)                                                   AS customer_id,
        ISNULL(TRIM(cst_firstname), 'n/a')                              AS first_name,
        ISNULL(TRIM(cst_lastname), 'n/a')                               AS last_name,
        -- Standardizing Marital Status: S -> Single, M -> Married
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END                                                             AS marital_status,
        -- Standardizing Gender: F -> Female, M -> Male
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END                                                             AS gender,
        cst_create_date                                                 AS created_date,
        dwh_load_date
    FROM 
        cust_info_deduplicated
    WHERE
        -- Keep only the latest record per customer
        flag = 1

),

cust_info_filtered AS (

    SELECT * FROM cust_info_transformed
    WHERE
        -- Filter out records with missing IDs                  
        customer_number IS NOT NULL
        -- Filter out invalid IDs
        AND customer_number != 0

)

SELECT * FROM cust_info_filtered