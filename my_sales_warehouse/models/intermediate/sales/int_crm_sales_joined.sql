WITH 

customers AS (

    SELECT
        customer_key,
        customer_number
    FROM 
        {{ ref('int_crm_erp_customers_joined') }}

),

products AS (

    SELECT
        product_key,
        product_number,
        product_id
    FROM
        {{ ref('int_crm_erp_products_joined') }}
),

sales AS (

    SELECT
        order_number,
        product_number,
        customer_number,
        ordered_date,
        shipping_date,
        due_date,
        sales_amount,
        quantity,
        price,
        dwh_load_date
    FROM
        {{ ref('stg_crm__sales_details') }}

),

final_sales_fact AS (

    SELECT
        -- Surrogate Key for Star Schema modeling
        {{ dbt_utils.generate_surrogate_key(['s.order_number', 'p.product_id', "'crm'"]) }} AS sales_key,
        s.order_number,
        -- Linking to the surrogate key in the Product Dimension
        p.product_key,
        -- Linking to the surrogate key in the Customer Dimension
        p.product_id,
        c.customer_key,
        s.ordered_date,
        s.shipping_date,
        s.due_date,
        s.sales_amount,
        s.quantity,
        s.price,
        s.dwh_load_date
    FROM
        sales s
    LEFT JOIN 
        customers c
    ON  s.customer_number = c.customer_number
    LEFT JOIN 
        products p
    ON  s.product_number = p.product_number

)

SELECT * FROM final_sales_fact