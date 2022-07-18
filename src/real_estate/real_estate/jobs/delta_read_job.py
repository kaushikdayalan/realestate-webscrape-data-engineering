from dagster import job
from real_estate.ops.spark_delta import read_delta_table
from real_estate.resource import my_pyspark_resource

delta_path_config = {'ops':{'read_delta_table':{'config':{'delta_table_path':'s3a://realestatedb/delta_table'}}}}

@job(resource_defs = {"pyspark": my_pyspark_resource}, 
config=delta_path_config)
def read_delta_job():
    read_delta_table()