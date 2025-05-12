WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_FILTER') }}
),

stats AS (
    SELECT
        ID,
        (HP + ATTACK + DEFENSE + SP_ATTACK + SP_DEFENSE + SPEED) AS BASE_STATS
    FROM source
)

SELECT * FROM stats
