from dagster import repository

from real_estate.jobs.say_hello import say_hello_job
from real_estate.schedules.my_hourly_schedule import my_hourly_schedule
from real_estate.sensors.my_sensor import my_sensor


@repository
def real_estate():
    """
    The repository definition for this real_estate Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
