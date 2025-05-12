WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_FILTER') }}
),
ranked_bmi AS (
    SELECT * FROM {{ ref('DBT_POKEMON_BMI_RANK') }}
),
egg_hatch_time AS (
    SELECT * FROM {{ ref('DBT_POKEMON_EGG_HATCH_TIME') }}
),
stats AS (
    SELECT * FROM {{ ref('DBT_POKEMON_STATS') }}
),

result AS (
    SELECT
        source.*,
        ranked_bmi.BMI,
        ranked_bmi.BMI_RANK,
        egg_hatch_time.EGG_HATCH_TIME,
        stats.BASE_STATS
    FROM source
    INNER JOIN ranked_bmi USING (ID)
    INNER JOIN egg_hatch_time USING (ID)
    INNER JOIN stats USING (ID)
)

SELECT * FROM result
