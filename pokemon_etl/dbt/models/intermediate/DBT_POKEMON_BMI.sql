WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_FILTER') }}
),

bmi AS (
    SELECT
        ID,
        (WEIGHT_KILOGRAMS / (HEIGHT_METERS * HEIGHT_METERS)) AS BMI
    FROM source
)

SELECT * FROM bmi
