{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('xkcd', 'raw_xkcd_comics') }}
)

SELECT
    num as comic_id,
    title,
    alt_text,
    img_url,
    published_date,
    transcript,
    loaded_at,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM source
ORDER BY comic_id