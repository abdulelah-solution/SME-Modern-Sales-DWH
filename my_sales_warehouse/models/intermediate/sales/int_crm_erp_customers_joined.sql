WITH 

customers_master_crm AS (

    SELECT
        customer_number,
        customer_id,
        first_name,
        last_name,
        marital_status,
        gender,
        created_date,
        dwh_load_date
    FROM 
        {{ ref('stg_crm__cust_info') }}

),

​customers_demographic_erp AS (

    SELECT
        customer_id,
        birth_date,
        gender
    FROM 
        {{ ref('stg_erp__cust_az12') }}

),

​customers_location_erp AS (

    SELECT
        customer_id,
        country
    FROM
        {{ ref('stg_erp__loc_a101') }}

),

joined_customers AS (

    SELECT
        -- Surrogate Key for Star Schema modeling
        {{ dbt_utils.generate_surrogate_key(['cm.customer_number', "'crm'"]) }}  AS customer_key,
        cm.customer_number,
        cm.customer_id,
        cm.first_name,
        cm.last_name,
        cm.marital_status,
        -- Conflict resolution: Use CRM gender unless it's unknown, then fallback to ERP
        CASE
            WHEN cm.gender != 'n/a' THEN cm.gender
            ELSE COALESCE(cd.gender, 'n/a')
        END                                             AS gender,
        cm.created_date,
        cm.dwh_load_date,
        cd.birth_date,
        cl.country
    FROM
        customers_master_crm cm
    LEFT JOIN
        ​customers_demographic_erp cd
    ON  cm.customer_id = cd.customer_id
    LEFT JOIN
        ​customers_location_erp cl
    ON  cm.customer_id = cl.customer_id

)

SELECT * FROM joined_customers