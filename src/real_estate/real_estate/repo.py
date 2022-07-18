from dagster import repository
#from real_estate.jobs.say_hello import say_hello_job
#from real_estate.jobs.perform_scrapping import perform_scrape
from real_estate.jobs.py_spark_scrape import create_df
@repository
def real_estate_web_scrape():
    jobs = [create_df]
    schedules = []
    sensors = []
    return jobs + schedules + sensors