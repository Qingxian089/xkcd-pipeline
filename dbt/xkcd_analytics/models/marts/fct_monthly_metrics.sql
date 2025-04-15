{{
    config(
        materialized='table',
        tags=['mart', 'fact', 'aggregate']
    )
}}

WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', d.published_date) as month,
        COUNT(DISTINCT d.comic_id) as comics_count,
        SUM(f.title_cost_euros) as total_cost_euros,
        AVG(f.estimated_views) as avg_views,
        AVG(f.customer_rating) as avg_rating
    FROM {{ ref('dim_comics') }} d
    JOIN {{ ref('fct_comic_metrics') }} f ON d.comic_id = f.comic_id
    GROUP BY 1
)

SELECT * FROM monthly_metrics