{{
    config(
        materialized='table',
        tags=['mart', 'dimension']
    )
}}

SELECT
    comic_id,
    title,
    LENGTH(COALESCE(title, '')) as title_length,
    alt_text,
    img_url,
    published_date,
    transcript,
    loaded_at,
    dbt_updated_at
FROM {{ ref('stg_xkcd_comics') }}