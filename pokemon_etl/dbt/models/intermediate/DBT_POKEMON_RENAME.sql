WITH source AS (
    SELECT * FROM {{ ref('DBT_POKEMON_JOIN') }}
),

renamed AS (
    SELECT
        source.id,
        source.name,
        source.height_inches,
        source.height_meters,
        source.weight_pounds,
        source.weight_kilograms,
        source.classification_info,
        source.forms,
        source.gen,
        source.is_legendary,
        source.is_mythical,
        source."Type 1" AS type_1,
        source."Type 2" AS type_2,
        source.abilities,
        source.hp,
        source.attack,
        source.defense,
        source."Sp. Attack" AS sp_attack,
        source."Sp. Defense" AS sp_defense,
        source.speed,
        source.normal_weakness,
        source.fire_weakness,
        source.water_weakness,
        source.electric_weakness,
        source.grass_weakness,
        source.ice_weakness,
        source.fighting_weakness,
        source.poison_weakness,
        source.ground_weakness,
        source.flying_weakness,
        source.psychic_weakness,
        source.bug_weakness,
        source.rock_weakness,
        source.ghost_weakness,
        source.dragon_weakness,
        source.fairy_weakness,
        source.capturing_rate,
        source.gender_male_ratio,
        source.egg_steps,
        source.egg_cycles
    FROM source
)

SELECT * FROM renamed
