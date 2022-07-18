from dagster import job

from real_estate.ops.try import hello_cereal

@job
def get_cereal_job():
    """
    This is the first dagster job i created
    """
    hello_cereal()
