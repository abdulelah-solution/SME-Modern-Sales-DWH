WITH 

sales AS (

    SELECT * FROM {{ ref('fct_sales') }}

),

customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

),

products AS (

    SELECT * FROM {{ ref('dim_products') }}

)

SELECT
    -- Sales Data (Facts)
    s.sales_key,
    s.order_number,
    s.ordered_date,
    s.quantity,
    s.price,
    s.sales_amount,

    -- Customers Data (Customer Context/dimension)
    c.customer_number,
    c.first_name + ' ' + c.last_name AS full_name,
    c.country,
    c.gender,

    -- Products Data (Product Context/dimension)
    p.product_number,
    p.category,
    p.subcategory,
    p.product_name
FROM 
    sales s
LEFT JOIN 
    customers c 
ON  s.customer_key = c.customer_key
LEFT JOIN 
    products p 
ON  s.product_key = p.product_key