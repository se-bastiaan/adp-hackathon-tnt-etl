WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_RENAME') }}
),

generation_1 AS (
    SELECT
        *
    FROM source
    WHERE GEN = 1
)

SELECT * FROM generation_1
