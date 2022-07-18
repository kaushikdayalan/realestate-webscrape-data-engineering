from dagster import job
from real_estate.ops.spark_delta import scrape_convert
from real_estate.resource import my_pyspark_resource
from real_estate.ops.scrapping import scrap_wg

@job(resource_defs = {"pyspark": my_pyspark_resource})
def create_df():
    scrape_convert()