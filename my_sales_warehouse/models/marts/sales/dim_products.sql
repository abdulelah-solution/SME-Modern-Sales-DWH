SELECT
    product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    product_cost,
    product_line,
    started_date,
    dwh_load_date
FROM
    {{ ref('int_crm_erp_products_joined') }}