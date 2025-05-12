{{ descritpion}}

WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_BMI') }}
),

ranked_bmi AS (
    SELECT
        *,
        RANK() OVER (ORDER BY BMI DESC) AS BMI_RANK
    FROM source
),

ordered_bmi AS (
    SELECT
        *
    FROM ranked_bmi
    ORDER BY ID ASC
)

SELECT * FROM ordered_bmi
