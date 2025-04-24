import os
from collections.abc import Mapping
from typing import Any

import requests
from dagster import EnvVar
from dagster._utils.cached_method import cached_method
from dagster_snowflake import SnowflakeResource


class DynamicSnowflakeResource(SnowflakeResource):
    @property
    @cached_method
    def _connection_args(self) -> Mapping[str, Any]:
        token = None
        if self.authenticator == "oauth":
            daemon_host = os.getenv("DAEMON_HOST", "localhost")
            response = requests.get(f"http://{daemon_host}:2774/gettoken")
            token = response.json()["Token"]

        return {
            "account": self._resolved_config_dict.get("account"),
            "user": self._resolved_config_dict.get("user") if not token else None,
            "database": self._resolved_config_dict.get("database"),
            "schema": self._resolved_config_dict.get("schema"),
            "warehouse": self._resolved_config_dict.get("warehouse"),
            "role": self._resolved_config_dict.get("role"),
            "authenticator": self._resolved_config_dict.get("authenticator"),
            "token": token,
        }


snowflake_resource = DynamicSnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA", "TNT"),
    authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR", "oauth"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
