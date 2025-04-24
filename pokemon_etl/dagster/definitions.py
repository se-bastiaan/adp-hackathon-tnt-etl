import dagster as dg
from pathlib import Path

from pokemon_etl.dagster import assets
from pokemon_etl.dagster.resources.snowflake_resource import snowflake_resource
from pokemon_etl.dagster.jobs import pokedex_job
from pokemon_etl.dagster.schedules import pokedex_schedule
from pokemon_etl.dagster.dbt_project import dbt_resource, dbt_models

code_assets = dg.load_assets_from_modules([assets], group_name="polars")
combined_assets = [*code_assets, dbt_models]

defs = dg.Definitions(
    assets=dg.link_code_references_to_git(
        assets_defs=combined_assets,
        git_url="https://github.com/se-bastiaan/adp-hackathon-tnt-etl",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__),
            file_anchor_path_in_repository="pokemon_etl/dagster/definitions.py",
        ),
    ),
    asset_checks=dg.build_column_schema_change_checks(assets=code_assets),
    jobs=[
        pokedex_job
    ],
    schedules=[
        pokedex_schedule
    ],
    resources={
        "snowflake": snowflake_resource,
        "dbt": dbt_resource
    },
)
