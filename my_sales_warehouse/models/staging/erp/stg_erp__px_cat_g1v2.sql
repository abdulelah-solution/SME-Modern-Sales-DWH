WITH 

source_data AS (

    SELECT
        id,
        cat,
        subcat,
        maintenance,
        dwh_load_date
    FROM
        {{ source('erp', 'erp_px_cat_g1v2') }}

),

px_cat_g1v2_transformed AS (

    SELECT
        -- Normalizing the ID format by replacing underscores with hyphens
        REPLACE(TRIM(id), '_', '-')                                             AS category_id, 
        cat                                                                     AS category,
        subcat                                                                  AS subcategory,
        maintenance,
        dwh_load_date
    FROM
        source_data

),

px_cat_g1v2_filtered AS (

    SELECT * FROM px_cat_g1v2_transformed
    WHERE
        category_id IS NOT NULL
        AND category_id != ''

)

SELECT * FROM px_cat_g1v2_filtered
