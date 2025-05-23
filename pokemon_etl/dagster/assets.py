import os
import polars as pl
import dagster as dg
import tempfile

from dagster_snowflake import SnowflakeResource
from pathlib import Path


def _read_table(snowflake: SnowflakeResource, table_name) -> pl.LazyFrame:
    """
    Read a table from Snowflake into a Polars DataFrame.

    This function uses the SnowflakeResource to establish a connection and execute
    a SQL query to fetch the data. The result is returned as a Polars DataFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
        table_name (str): The name of the table to read.
    Returns:
        pl.DataFrame: The table data as a Polars DataFrame.
    """
    with snowflake.get_connection() as conn:
        query = f"SELECT * FROM {table_name}"
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pl.DataFrame(data, schema=columns).lazy()


@dg.asset(
    key_prefix=["polars"],
    kinds={"snowflake"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def raw_base_table(snowflake: SnowflakeResource) -> pl.LazyFrame:
    """
    Read the Pokémon base table from Snowflake and return it as a Polars DataFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.DataFrame: The Pokémon base table as a Polars DataFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_BASE")


@dg.asset(
    key_prefix=["polars"],
    kinds={"snowflake"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def raw_battle_table(snowflake: SnowflakeResource) -> pl.LazyFrame:
    """
    Read the Pokémon battle table from Snowflake and return it as a Polars LazyFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.LazyFrame: The Pokémon battle table as a Polars LazyFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_BATTLE")


@dg.asset(
    key_prefix=["polars"],
    kinds={"snowflake"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def raw_repro_table(snowflake: SnowflakeResource) -> pl.LazyFrame:
    """
    Read the Pokémon reproduction table from Snowflake and return it as a Polars LazyFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.LazyFrame: The Pokémon reproduction table as a Polars LazyFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_REPRODUCTION")


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def joined_table(
    context: dg.AssetExecutionContext,
    raw_base_table: pl.LazyFrame,
    raw_battle_table: pl.LazyFrame,
    raw_repro_table: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Returns a LazyFrame that joins the Pokémon base, battle, and reproduction tables together on the "ID" column.

    This function returns a single LazyFrame containing all the relevant data.
    The join is performed on the "ID" column, which is common to all three tables.
    The resulting LazyFrame contains all columns from the base, battle, and reproduction tables.

    Args:
        raw_base_table (pl.LazyFrame): The Pokémon base table.
        raw_battle_table (pl.LazyFrame): The Pokémon battle table.
        raw_repro_table (pl.LazyFrame): The Pokémon reproduction table.
    Returns:
        pl.LazyFrame: A LazyFrame that joins the Pokémon base, battle, and reproduction tables together on the `ID` column.
    """
    return raw_base_table.join(raw_battle_table, "ID").join(raw_repro_table, "ID")


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def renamed_table(joined_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Rename specific columns in the joined LazyFrame for clarity.

    This function renames the following columns:
    - "Type 1" to "TYPE_1"
    - "Type 2" to "TYPE_2"
    - "Sp. Attack" to "SP_ATTACK"
    - "Sp. Defense" to "SP_DEFENSE"
    The renaming is done to standardise the column names and make them more descriptive.

    Args:
        joined_table (pl.LazyFrame): The joined LazyFrame containing all relevant data.
    Returns:
        pl.LazyFrame: A LazyFrame with renamed columns for clarity and consistency.
    """
    return joined_table.rename(
        mapping={
            "Type 1": "TYPE_1",
            "Type 2": "TYPE_2",
            "Sp. Attack": "SP_ATTACK",
            "Sp. Defense": "SP_DEFENSE",
        }
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def filtered_table(renamed_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter the LazyFrame to include only Pokémon from Generation 1.
    Our use case is to focus on Generation 1 Pokémon for analysis.
    This function filters the LazyFrame based on the "GEN" column, retaining only rows
    where "GEN" is equal to 1.

    Args:
        renamed_table (pl.LazyFrame): The LazyFrame with renamed columns.
    Returns:
        pl.LazyFrame: A filtered LazyFrame containing only Generation 1 Pokémon.
    """
    return renamed_table.filter(GEN=1)


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def type_count_table(filtered_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Count the number of Pokémon per type in the filtered LazyFrame.
    This function uses the "TYPE_1" column to count the number of Pokémon
    for each type. The result is a LazyFrame with the "ID" and "TYPE_COUNT" columns,
    where "TYPE_COUNT" represents the total count of Pokémon that also have the same type
    as the Pokémon with that ID.

    Args:
        filtered_table (pl.LazyFrame): The filtered LazyFrame containing only Generation 1 Pokémon.
    Returns:
        pl.LazyFrame: A LazyFrame with Pokémon "ID" and "TYPE_COUNT" columns.
    """
    return filtered_table.select(pl.col("ID"), TYPE_COUNT=pl.count().over("TYPE_1"))


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def calculated_base_stats_table(filtered_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate the base stats for each Pokémon in the filtered LazyFrame.
    This function computes the sum of the following columns:
    - "HP"
    - "ATTACK"
    - "DEFENSE"
    - "SP_ATTACK"
    - "SP_DEFENSE"
    - "SPEED"
    The result is a LazyFrame with the "ID" and "BASE_STATS" columns,
    where "BASE_STATS" represents the overall strength for each Pokémon.

    Args:
        filtered_table (pl.LazyFrame): The filtered LazyFrame containing only Generation 1 Pokémon.
    Returns:
        pl.LazyFrame: A LazyFrame with Pokémon "ID" and "BASE_STATS" columns.
    """
    return filtered_table.select(
        pl.col("ID"),
        BASE_STATS=pl.col("HP")
        + pl.col("ATTACK")
        + pl.col("DEFENSE")
        + pl.col("SP_ATTACK")
        + pl.col("SP_DEFENSE")
        + pl.col("SPEED"),
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def calculated_egg_hatch_time_table(filtered_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate the egg hatch time for each Pokémon in the filtered LazyFrame.
    This function computes the hatch time based on the "EGG_STEPS" column.
    The result is a LazyFrame with the "ID" and "EGG_HATCH_TIME" columns,
    where "EGG_HATCH_TIME" is calculated by dividing "EGG_STEPS" by 100,
    rounding up to the nearest integer.

    Args:
        filtered_table (pl.LazyFrame): The filtered LazyFrame containing only Generation 1 Pokémon.
    Returns:
        pl.LazyFrame: A LazyFrame with Pokémon "ID" and "EGG_HATCH_TIME" columns.
    """
    return filtered_table.select(
        pl.col("ID"), EGG_HATCH_TIME=(pl.col("EGG_STEPS") / 100).ceil().cast(pl.Int64)
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "true"},
)
def calculated_bmi_table(filtered_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Rank the Pokémon based on their BMI values.
    This function assigns a rank to each Pokémon based on their BMI,
    with the highest BMI receiving the highest rank.
    The ranking is done using the ordinal method, where ties are given the same rank.
    The result is a DataFrame with the "ID" and "BMI_RANK" columns,
    where "BMI_RANK" represents the rank of each Pokémon based on their BMI.

    Args:
        calculated_bmi_table (pl.DataFrame): The DataFrame with "ID" and "BMI" columns.
    Returns:
        pl.DataFrame: A DataFrame with the Pokémon "ID", "BMI" and "BMI_RANK" columns.
    """
    return filtered_table.with_columns(
        HEIGHT_METERS=pl.col("HEIGHT_METERS").cast(pl.Float32)
    ).select(
        pl.col("ID"), BMI=pl.col("WEIGHT_KILOGRAMS") / (pl.col("HEIGHT_METERS").pow(2))
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "true"},
)
def ranked_bmi_table(calculated_bmi_table: pl.LazyFrame) -> pl.LazyFrame:
    """
    Rank the Pokémon based on their BMI values.
    This function assigns a rank to each Pokémon based on their BMI,
    with the highest BMI receiving the highest rank.
    The ranking is done using the ordinal method, where ties are given the same rank.
    The result is a LazyFrame with the "ID" and "BMI_RANK" columns,
    where "BMI_RANK" represents the rank of each Pokémon based on their BMI.

    Args:
        calculated_bmi_table (pl.LazyFrame): The LazyFrame with "ID" and "BMI" columns.
    Returns:
        pl.LazyFrame: A LazyFrame with the Pokémon "ID", "BMI" and "BMI_RANK" columns.
    """
    return calculated_bmi_table.with_columns(
        BMI_RANK=pl.col("BMI").rank(method="ordinal", descending=True)
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"polars"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "true"},
)
def aggregated_table(
    context: dg.AssetExecutionContext,
    filtered_table: pl.LazyFrame,
    type_count_table: pl.LazyFrame,
    calculated_base_stats_table: pl.LazyFrame,
    calculated_egg_hatch_time_table: pl.LazyFrame,
    ranked_bmi_table: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Aggregate the filtered LazyFrame with additional calculated columns.
    This function joins the filtered LazyFrame with the type count, base stats,
    egg hatch time, and ranked BMI LazyFrames on the "ID" column.
    The result is a LazyFrame containing all relevant columns for each Pokémon.
    The aggregation is done to provide a comprehensive view of each Pokémon's attributes.

    Args:
        filtered_table (pl.LazyFrame): The filtered LazyFrame containing only Generation 1 Pokémon.
        type_count_table (pl.LazyFrame): The LazyFrame with "ID" and "TYPE_COUNT" columns.
        calculated_base_stats_table (pl.LazyFrame): The LazyFrame with "ID" and "BASE_STATS" columns.
        calculated_egg_hatch_time_table (pl.LazyFrame): The LazyFrame with "ID" and "EGG_HATCH_TIME" columns.
        ranked_bmi_table (pl.LazyFrame): The LazyFrame with "ID" and "BMI_RANK" columns.
    Returns:
        pl.LazyFrame: The aggregated LazyFrame containing all relevant columns for each Pokémon.
    """
    return (
        filtered_table.join(type_count_table, "ID")
        .join(calculated_base_stats_table, "ID")
        .join(calculated_egg_hatch_time_table, "ID")
        .join(ranked_bmi_table, "ID")
    )


@dg.asset(
    key_prefix=["polars"],
    kinds={"snowflake"},
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "false"},
)
def pokemon_table(snowflake: SnowflakeResource, aggregated_table: pl.LazyFrame) -> None:
    """
    Create a Snowflake table and grant permissions to the specified role.
    This function generates a CREATE TABLE statement based on the schema of the
    aggregated LazyFrame. The table is created in Snowflake, and the specified role
    is granted all permissions on the table.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
        aggregated_table (pl.LazyFrame): The aggregated LazyFrame containing Pokémon data.
    Returns:
        Creates a Snowflake table and grants permissions to the specified role.
    """
    dtype_map = {
        pl.Int64: "INTEGER",
        pl.Int32: "INTEGER",
        pl.UInt32: "INTEGER",
        pl.UInt64: "INTEGER",
        pl.Float64: "FLOAT",
        pl.Float32: "FLOAT",
        pl.Boolean: "BOOLEAN",
        pl.Utf8: "VARCHAR",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP",
        pl.Time: "TIME",
        pl.Duration: "INTERVAL",
        pl.Object: "VARIANT",
        pl.List: "ARRAY",
        pl.Decimal: "DECIMAL",
        pl.Decimal(None, 1): "DECIMAL",
        pl.Decimal(None, 2): "DECIMAL",
    }

    columns_sql = [
        f'"{name}" {dtype_map.get(dtype, "VARIANT")}'
        for name, dtype in aggregated_table.schema.items()
    ]

    table_name = "PY_POKEMON_PROCESSED"
    create_stmt = f"CREATE OR REPLACE TABLE {table_name} ({','.join(columns_sql)});"

    with snowflake.get_connection() as conn:
        conn.cursor().execute(create_stmt)


@dg.asset(
    key_prefix=["polars"],
    kinds={"snowflake", "polars"},
    deps=[pokemon_table],
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "true"},
)
def filled_pokemon_table(
    snowflake: SnowflakeResource, aggregated_table: pl.LazyFrame
) -> None:
    """
    Fill the Snowflake table with data from the aggregated LazyFrame.
    This function writes the aggregated LazyFrame to a CSV file, uploads it to Snowflake,
    and copies the data into the Snowflake table. The file is removed after the operation.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
        aggregated_table (pl.LazyFrame): The aggregated LazyFrame containing Pokémon data.
    Returns:
        Loads the aggregated LazyFrame into the Snowflake table.
    """
    temp_dir = Path(tempfile.gettempdir())
    file_path = temp_dir / "pokemon.csv"
    aggregated_table.collect().write_csv(file_path)

    with snowflake.get_connection() as conn:
        conn.cursor().execute(f"PUT file://{file_path} @%PY_POKEMON_PROCESSED;")
        conn.cursor().execute(
            "COPY INTO PY_POKEMON_PROCESSED FROM @%PY_POKEMON_PROCESSED FILE_FORMAT = (type = 'CSV' SKIP_HEADER = 1);"
        )

    os.remove(file_path)
