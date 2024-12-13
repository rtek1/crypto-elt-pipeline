-- models/mart/user_lifetime_spend.sql
SELECT
    u.user_id,
    u.user_name,
    SUM(f.actual_price) AS total_spend
FROM
    {{ ref('fact_sales') }} AS f
LEFT JOIN
    {{ ref('dim_users') }} AS u
    ON f.user_id = u.user_id
GROUP BY
    u.user_id, u.user_name
ORDER BY
    total_spend DESC
