WITH 

source_data AS (

    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        dwh_load_date
    FROM 
        {{ source('crm', 'crm_prd_info') }}

),

prd_info_transformed AS (

    SELECT 
        prd_id                                                              AS product_id,
        -- Extracting Category ID (first 5 characters)
        SUBSTRING(prd_key, 1, 5)                                            AS category_id,
        -- Extracting the actual Product Key (starting from character 7)
        SUBSTRING(prd_key, 7, LEN(prd_key))                                 AS product_number, 
        prd_nm                                                              AS product_name,
        -- Replacing NULL costs with zero
        ISNULL(prd_cost, 0)                                                 AS product_cost,
        -- Mapping Product Line codes to readable labels
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END                                                                 AS product_line,
        prd_start_dt                                                        AS started_date,
        dwh_load_date
    FROM
        source_data

),

prd_info_formatted AS (

    SELECT
        *,
        -- Calculating prd_end_dt based on the start date of the next version
        -- Subtracting 1 day to ensure non-overlapping validity periods
        DATEADD(day, -1, LEAD(started_date) OVER(PARTITION BY product_number ORDER BY started_date)) AS ended_date
    FROM 
        prd_info_transformed

),

prd_info_filtered AS (

    SELECT * FROM prd_info_formatted
    WHERE
        started_date IS NOT NULL
        AND started_date <= GETDATE()
        AND ended_date IS NULL
        
)

SELECT * FROM prd_info_filtered