import os
from pathlib import Path
from typing import Any, Mapping
import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator
from dagster_snowflake import SnowflakeResource
import httpx

from pokemon_etl.dagster.resources.snowflake_resource import DynamicSnowflakeResource

dbt_project_directory = Path(__file__).absolute().parent.parent / "dbt"
dbt_project = DbtProject(project_dir=dbt_project_directory)
dbt_resource = DbtCliResource(project_dir=dbt_project)
dbt_project.prepare_if_dev()


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        return super().get_asset_key(dbt_resource_props).with_prefix("dbt")


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource, snowflake: DynamicSnowflakeResource):
    if snowflake.authenticator == "oauth":
        os.environ["SNOWFLAKE_TOKEN"] = snowflake.get_sts_token()
    yield from dbt.cli(["build"], context=context).stream().fetch_column_metadata().fetch_row_counts()
