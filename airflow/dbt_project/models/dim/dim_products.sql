-- models/dim/dim_products.sql
SELECT
    product_id,
    product_name,
    category,
    about_product,
    img_link,
    product_link
FROM
    {{ ref('stg_amazon_sales_data') }}
GROUP BY
    product_id, product_name, category, about_product, img_link, product_link