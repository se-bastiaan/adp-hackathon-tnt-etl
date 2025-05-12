WITH source AS (
    SELECT * FROM {{ source('pokemon', 'RAW_POKEMON_REPRODUCTION') }}
),

pokemon_repro AS (
    SELECT
        id,
        capturing_rate,
        gender_male_ratio,
        egg_steps,
        egg_cycles
    FROM source
)

SELECT * FROM pokemon_repro
