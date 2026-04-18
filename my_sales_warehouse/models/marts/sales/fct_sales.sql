SELECT
    sales_key,
    order_number,
    product_id,
    product_key,
    customer_key,
    ordered_date,
    shipping_date,
    due_date,
    quantity,
    price,
    sales_amount,
    dwh_load_date
FROM
    {{ ref('int_crm_sales_joined') }}