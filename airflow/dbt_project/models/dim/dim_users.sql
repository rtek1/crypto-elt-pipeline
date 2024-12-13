-- models/dim/dim_users.sql
SELECT
    user_id,
    user_name
FROM
    {{ ref('stg_amazon_sales_data') }}
GROUP BY
    user_id, user_name