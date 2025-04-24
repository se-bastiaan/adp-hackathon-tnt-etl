import os
import polars as pl
import dagster as dg
import tempfile

from dagster_snowflake import SnowflakeResource
from pathlib import Path


def _read_table(snowflake: SnowflakeResource, table_name) -> pl.DataFrame:
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
        return pl.DataFrame(data, schema=columns)


@dg.asset(
    kinds={"snowflake"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def raw_base_table(snowflake: SnowflakeResource) -> pl.DataFrame:
    """
    Read the Pokémon base table from Snowflake and return it as a Polars DataFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.DataFrame: The Pokémon base table as a Polars DataFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_BASE")


@dg.asset(
    kinds={"snowflake"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def raw_battle_table(snowflake: SnowflakeResource) -> pl.DataFrame:
    """
    Read the Pokémon battle table from Snowflake and return it as a Polars DataFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.DataFrame: The Pokémon battle table as a Polars DataFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_BATTLE")


@dg.asset(
    kinds={"snowflake"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def raw_repro_table(snowflake: SnowflakeResource) -> pl.DataFrame:
    """
    Read the Pokémon reproduction table from Snowflake and return it as a Polars DataFrame.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
    Returns:
        pl.DataFrame: The Pokémon reproduction table as a Polars DataFrame.
    """
    return _read_table(snowflake, "RAW_POKEMON_REPRODUCTION")


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def joined_table(
    raw_base_table: pl.DataFrame,
    raw_battle_table: pl.DataFrame,
    raw_repro_table: pl.DataFrame,
) -> pl.DataFrame:
    return raw_base_table.join(raw_battle_table, "ID").join(raw_repro_table, "ID")


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def renamed_table(joined_table: pl.DataFrame) -> pl.DataFrame:
    """
    Rename specific columns in the joined DataFrame for clarity.

    This function renames the following columns:
    - "Type 1" to "TYPE_1"
    - "Type 2" to "TYPE_2"
    - "Sp. Attack" to "SP_ATTACK"
    - "Sp. Defense" to "SP_DEFENSE"
    The renaming is done to standardise the column names and make them more descriptive.

    Args:
        joined_table (pl.DataFrame): The joined DataFrame containing all relevant data.
    Returns:
        pl.DataFrame: A DataFrame with renamed columns for clarity and consistency.
    """
    df = joined_table.rename(
        mapping={
            "Type 1": "TYPE_1",
            "Type 2": "TYPE_2",
            "Sp. Attack": "SP_ATTACK",
            "Sp. Defense": "SP_DEFENSE",
        }
    )
    return df


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def filtered_table(renamed_table: pl.DataFrame) -> pl.DataFrame:
    """
    Filter the DataFrame to include only Pokémon from Generation 1.
    Our use case is to focus on Generation 1 Pokémon for analysis.
    This function filters the DataFrame based on the "GEN" column, retaining only rows
    where "GEN" is equal to 1.

    Args:
        renamed_table (pl.DataFrame): The DataFrame with renamed columns.
    Returns:
        pl.DataFrame: A filtered DataFrame containing only Generation 1 Pokémon.
    """
    df = renamed_table.filter(GEN=1)
    return df


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def type_count_table(filtered_table: pl.DataFrame) -> pl.DataFrame:
    """
    Count the number of Pokémon per type in the filtered DataFrame.
    This function uses the "TYPE_1" column to count the number of Pokémon
    for each type. The result is a DataFrame with the "ID" and "TYPE_COUNT" columns,
    where "TYPE_COUNT" represents the total count of Pokémon that also have the same type
    as the Pokémon with that ID.

    Args:
        filtered_table (pl.DataFrame): The filtered DataFrame containing only Generation 1 Pokémon.
    Returns:
        pl.DataFrame: A DataFrame with Pokémon "ID" and "TYPE_COUNT" columns.
    """
    return filtered_table.select(pl.col("ID"), TYPE_COUNT=pl.count().over("TYPE_1"))


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def calculated_base_stats_table(filtered_table: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the base stats for each Pokémon in the filtered DataFrame.
    This function computes the sum of the following columns:
    - "HP"
    - "ATTACK"
    - "DEFENSE"
    - "SP_ATTACK"
    - "SP_DEFENSE"
    - "SPEED"
    The result is a DataFrame with the "ID" and "BASE_STATS" columns,
    where "BASE_STATS" represents the overall strength for each Pokémon.

    Args:
        filtered_table (pl.DataFrame): The filtered DataFrame containing only Generation 1 Pokémon.
    Returns:
        pl.DataFrame: A DataFrame with Pokémon "ID" and "BASE_STATS" columns.
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
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def calculated_egg_hatch_time_table(filtered_table: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the egg hatch time for each Pokémon in the filtered DataFrame.
    This function computes the hatch time based on the "EGG_STEPS" column.
    The result is a DataFrame with the "ID" and "EGG_HATCH_TIME" columns,
    where "EGG_HATCH_TIME" is calculated by dividing "EGG_STEPS" by 100,
    rounding up to the nearest integer.

    Args:
        filtered_table (pl.DataFrame): The filtered DataFrame containing only Generation 1 Pokémon.
    Returns:
        pl.DataFrame: A DataFrame with Pokémon "ID" and "EGG_HATCH_TIME" columns.
    """
    return filtered_table.select(
        pl.col("ID"), EGG_HATCH_TIME=(pl.col("EGG_STEPS") / 100).ceil().cast(pl.Int64)
    )


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "true"}
)
def calculated_bmi_table(filtered_table: pl.DataFrame) -> pl.DataFrame:
    return filtered_table.with_columns(
        HEIGHT_METERS=pl.col("HEIGHT_METERS").cast(pl.Float32)
    ).select(
        pl.col("ID"), BMI=pl.col("WEIGHT_KILOGRAMS") / (pl.col("HEIGHT_METERS").pow(2))
    )


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "true"}
)
def ranked_bmi_table(calculated_bmi_table: pl.DataFrame) -> pl.DataFrame:
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
    return calculated_bmi_table.with_columns(
        BMI_RANK=pl.col("BMI").rank(method="ordinal", descending=True)
    )


@dg.asset(
    kinds={"polars"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "true"}
)
def aggregated_table(
    filtered_table: pl.DataFrame,
    type_count_table: pl.DataFrame,
    calculated_base_stats_table: pl.DataFrame,
    calculated_egg_hatch_time_table: pl.DataFrame,
    ranked_bmi_table: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate the filtered DataFrame with additional calculated columns.
    This function joins the filtered DataFrame with the type count, base stats,
    egg hatch time, and ranked BMI DataFrames on the "ID" column.
    The result is a DataFrame containing all relevant columns for each Pokémon.
    The aggregation is done to provide a comprehensive view of each Pokémon's attributes.

    Args:
        filtered_table (pl.DataFrame): The filtered DataFrame containing only Generation 1 Pokémon.
        type_count_table (pl.DataFrame): The DataFrame with "ID" and "TYPE_COUNT" columns.
        calculated_base_stats_table (pl.DataFrame): The DataFrame with "ID" and "BASE_STATS" columns.
        calculated_egg_hatch_time_table (pl.DataFrame): The DataFrame with "ID" and "EGG_HATCH_TIME" columns.
        ranked_bmi_table (pl.DataFrame): The DataFrame with "ID" and "BMI_RANK" columns.
    Returns:
        pl.DataFrame: The aggregated DataFrame containing all relevant columns for each Pokémon.
    """
    return (
        filtered_table.join(type_count_table, "ID")
        .join(calculated_base_stats_table, "ID")
        .join(calculated_egg_hatch_time_table, "ID")
        .join(ranked_bmi_table, "ID")
    )


@dg.asset(
    kinds={"snowflake"}, owners=["team:tnt"], tags={"domain": "pokedex", "pii": "false"}
)
def pokemon_table(snowflake: SnowflakeResource, aggregated_table: pl.DataFrame) -> None:
    """
    Create a Snowflake table and grant permissions to the specified role.
    This function generates a CREATE TABLE statement based on the schema of the
    aggregated DataFrame. The table is created in Snowflake, and the specified role
    is granted all permissions on the table.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
        aggregated_table (pl.DataFrame): The aggregated DataFrame containing Pokémon data.
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
    kinds={"snowflake", "polars"},
    deps=[pokemon_table],
    owners=["team:tnt"],
    tags={"domain": "pokedex", "pii": "true"},
)
def filled_pokemon_table(
    snowflake: SnowflakeResource, aggregated_table: pl.DataFrame
) -> None:
    """
    Fill the Snowflake table with data from the aggregated DataFrame.
    This function writes the aggregated DataFrame to a CSV file, uploads it to Snowflake,
    and copies the data into the Snowflake table. The file is removed after the operation.

    Args:
        snowflake (SnowflakeResource): The Snowflake resource for database connection.
        aggregated_table (pl.DataFrame): The aggregated DataFrame containing Pokémon data.
    Returns:
        Loads the aggregated DataFrame into the Snowflake table.
    """
    temp_dir = Path(tempfile.gettempdir())
    file_path = temp_dir / "pokemon.csv"
    aggregated_table.write_csv(file_path)

    with snowflake.get_connection() as conn:
        conn.cursor().execute(f"PUT file://{file_path} @%PY_POKEMON_PROCESSED;")
        conn.cursor().execute(
            "COPY INTO PY_POKEMON_PROCESSED FROM @%PY_POKEMON_PROCESSED FILE_FORMAT = (type = 'CSV' SKIP_HEADER = 1);"
        )

    os.remove(file_path)
