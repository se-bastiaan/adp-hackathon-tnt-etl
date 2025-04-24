import dagster as dg
from pokemon_etl.dagster.jobs import pokedex_job

pokedex_schedule = dg.ScheduleDefinition(
    name="refresh_pokedex",
    description="Refresh all Pokemon assets every hour",
    job=pokedex_job,
    cron_schedule="0 * * * *",  # start of every hour
    default_status=dg.DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Amsterdam",
)
