WITH source AS (
    SELECT *
    FROM {{ source('pokemon', 'RAW_POKEMON_BASE') }}
),

pokemon_base AS (
    SELECT
        id,
        name,
        height_inches,
        height_meters,
        weight_pounds,
        weight_kilograms,
        classification_info,
        forms,
        gen,
        is_legendary,
        is_mythical
    FROM source
)

SELECT * FROM pokemon_base
