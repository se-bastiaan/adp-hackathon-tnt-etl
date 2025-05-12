WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_FILTER') }}
),

egg_hatch_time AS (
    SELECT
        ID,
        CEIL(EGG_STEPS / 100) AS EGG_HATCH_TIME
    FROM source
)

SELECT * FROM egg_hatch_time
