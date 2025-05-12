WITH base AS (
    SELECT * FROM {{ ref('DBT_POKEMON_BASE') }}
),

battle AS (
    SELECT * FROM {{ ref('DBT_POKEMON_BATTLE') }}
),

repro AS (
    SELECT * FROM {{ ref('DBT_POKEMON_REPRO') }}
),

joined AS (
    SELECT
        base.id,
        base.name,
        base.height_inches,
        base.height_meters,
        base.weight_pounds,
        base.weight_kilograms,
        base.classification_info,
        base.forms,
        base.gen,
        base.is_legendary,
        base.is_mythical,
        battle."Type 1",
        battle."Type 2",
        battle.abilities,
        battle.hp,
        battle.attack,
        battle.defense,
        battle."Sp. Attack",
        battle."Sp. Defense",
        battle.speed,
        battle.normal_weakness,
        battle.fire_weakness,
        battle.water_weakness,
        battle.electric_weakness,
        battle.grass_weakness,
        battle.ice_weakness,
        battle.fighting_weakness,
        battle.poison_weakness,
        battle.ground_weakness,
        battle.flying_weakness,
        battle.psychic_weakness,
        battle.bug_weakness,
        battle.rock_weakness,
        battle.ghost_weakness,
        battle.dragon_weakness,
        battle.fairy_weakness,
        repro.capturing_rate,
        repro.gender_male_ratio,
        repro.egg_steps,
        repro.egg_cycles
    FROM base
    INNEr JOIN battle ON base.id = battle.id
    INNEr JOIN repro ON base.id = repro.id
)

SELECT * FROM joined
