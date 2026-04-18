SELECT
    customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birth_date,
    created_date,
    dwh_load_date
FROM
    {{ ref('int_crm_erp_customers_joined') }}