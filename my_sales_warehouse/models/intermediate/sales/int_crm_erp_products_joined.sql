WITH 

products_master_crm AS (

    SELECT
        product_id,
        category_id,
        product_number,
        product_name,
        product_cost,
        product_line,
        started_date,
        ended_date,
        dwh_load_date
    FROM 
        {{ ref('stg_crm__prd_info') }}

),

products_additional_info_erp AS (

    SELECT
        category_id,
        category,
        subcategory,
        maintenance
    FROM 
        {{ ref('stg_erp__px_cat_g1v2') }}

),

joined_products AS (

    SELECT
        -- Surrogate Key for Star Schema modeling
        {{ dbt_utils.generate_surrogate_key(['pm.product_id', "'crm'"]) }} AS product_key,
        pm.product_id,
        pm.category_id,
        pm.product_number,
        pm.product_name,
        pm.product_cost,
        pm.product_line,
        pm.started_date,
        pm.ended_date,
        pm.dwh_load_date,
        pa.category,
        pa.subcategory,
        pa.maintenance
    FROM
        products_master_crm pm
    LEFT JOIN
        products_additional_info_erp pa
    ON  pm.category_id = pa.category_id

)

SELECT * FROM joined_products