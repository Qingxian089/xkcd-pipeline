{{
    config(
        materialized='table',
        tags=['mart', 'fact']
    )
}}

WITH base_metrics AS (
    SELECT
        comic_id,
        title_length * 5 as title_cost_euros,
        FLOOR(RANDOM() * 10000) as estimated_views,
        CAST(1.0 + (RANDOM() * 9.0) AS DECIMAL(3,1)) as customer_rating,
        dbt_updated_at
    FROM {{ ref('dim_comics') }}
)

SELECT
    comic_id,
    title_cost_euros,
    estimated_views,
    customer_rating,
    dbt_updated_at
FROM base_metrics