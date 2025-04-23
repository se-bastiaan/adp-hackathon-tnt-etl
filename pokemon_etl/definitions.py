import dagster as dg
from pathlib import Path

from pokemon_etl import assets
from pokemon_etl.resources.snowflake_resource import snowflake_resource

all_assets = dg.load_assets_from_modules([assets])

pokedex_job = dg.define_asset_job(name="pokedex_job", description="Materialize the processed Pokémon table in Snowflake", selection='tag:"domain"="pokedex"')
pokedex_schedule = dg.ScheduleDefinition(
    name="refresh_pokedex",
    description="Refresh all Pokemon assets every hour",
    job=pokedex_job,
    cron_schedule="0 * * * *",  # start of every hour
    default_status=dg.DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Amsterdam",
)

defs = dg.Definitions(
    assets=dg.link_code_references_to_git(
        assets_defs=all_assets,
        git_url="https://github.com/se-bastiaan/adp-hackathon-tnt-etl",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__),
            file_anchor_path_in_repository="pokemon_etl/definitions.py",
        ),
    ),
    asset_checks=dg.build_column_schema_change_checks(assets=all_assets),
    jobs=[
        pokedex_job
    ],
    schedules=[
        pokedex_schedule
    ],
    resources={
        "snowflake": snowflake_resource,
    },
)
