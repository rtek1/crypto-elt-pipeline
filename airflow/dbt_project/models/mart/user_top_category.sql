-- models/mart/user_top_category.sql
WITH category_revenue_per_user AS (
    SELECT
        f.user_id,
        p.category,
        SUM(f.actual_price) AS total_revenue,
        COUNT(f.product_id) AS order_count
    FROM
        {{ ref('fact_sales') }} AS f
    LEFT JOIN
        {{ ref('dim_products') }} AS p
        ON f.product_id = p.product_id
    GROUP BY
        f.user_id, p.category
),
ranked_categories AS (
    SELECT
        user_id,
        category,
        total_revenue,
        order_count,
        RANK() OVER (PARTITION BY user_id ORDER BY total_revenue DESC) AS category_rank
    FROM
        category_revenue_per_user
)
SELECT
    user_id,
    category AS top_category,
    total_revenue,
    order_count
FROM
    ranked_categories
WHERE
    category_rank = 1
ORDER BY
    total_revenue DESC