from dagster import job
from real_estate.ops.scrapping import scrap_wg

@job(
    description = '''Job creation to perform the scraping'''
)
def perform_scrape():
    scrap_wg()