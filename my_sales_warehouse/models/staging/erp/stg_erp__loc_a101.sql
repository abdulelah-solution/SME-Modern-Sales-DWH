WITH 

source_data AS (

    SELECT
        cid,
        cntry,
        dwh_load_date
    FROM
        {{ source('erp', 'erp_loc_a101') }}

),

loc_a101_transformed AS (

    SELECT
        -- Removing formatting characters (hyphens) to match master customer keys
        REPLACE(cid, '-', '')                                                           AS customer_id, 
        -- Mapping standardized country codes to full descriptive names
        CASE 
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END                                                                             AS country,
        dwh_load_date
    FROM
        source_data

),

loc_a101_filtered AS (

    SELECT * FROM loc_a101_transformed
    WHERE
        customer_id IS NOT NULL
        AND customer_id != ''

)

SELECT * FROM loc_a101_filtered
