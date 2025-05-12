WITH source AS (
    SELECT * FROM {{ source('pokemon', 'RAW_POKEMON_BATTLE') }}
),

pokemon_battle AS (
    SELECT
        source.id,
        source."Type 1",
        source."Type 2",
        source.abilities,
        source.hp,
        source.attack,
        source.defense,
        "Sp. Attack",
        "Sp. Defense",
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
        source.fairy_weakness
    FROM source
)

SELECT * FROM pokemon_battle
