import dagster as dg

pokedex_job = dg.define_asset_job(
    name="pokedex_job",
    description="Materialize the processed Pokémon table in Snowflake",
    selection='tag:"domain"="pokedex"',
)
