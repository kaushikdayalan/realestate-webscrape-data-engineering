from socketserver import DatagramRequestHandler
import dagster
from dagster import op, graph, Field, String, AssetMaterialization
from typing import List
import pandas as pd
from pyspark.sql import Row
from real_estate.ops.scrapping import scrap_wg
from delta.tables import *



@op(required_resource_keys={'pyspark'},
    description="This is the OP to create the delta table"
)
def create_delta_table(context, json_data: List):
    spark_session = context.resources.pyspark.spark_session
    columns = ['property_id', 'name', 'price', 'size', 'extra_information']
    RowData = map(lambda x: Row(**x), json_data)
    wg_df = spark_session.createDataFrame(RowData, columns)
    delta_path = "s3a://realestatedb/delta_table"
    wg_df.write.format('delta').mode("overwrite").save(delta_path)


@op(required_resource_keys={'pyspark'},
description="This OP adds the JSON data fetched to s3 bucket")
def write_json_to_s3(context, json_data: List):
    spark_session = context.resources.pyspark.spark_session
    columns = ['property_id', 'name', 'price', 'size', 'extra_information']
    RowData = map(lambda x: Row(**x), json_data)
    df = spark_session.createDataFrame(RowData, columns)
    print(df.show(n=5))
    #df.write.format('csv').overwrite("s3a://realestatedb/json_data/")

@op(required_resource_keys = {'pyspark'}, 
config_schema={
    'delta_table_path':Field(
        String,
    )
},
description="This OP reads the delta table from s3")
def read_delta_table(context):
    df = context.resources.pyspark.spark_session.read.format('delta').load(context.op_config['delta_table_path'])
    print(df.show(n=5))
    context.log_event(
        AssetMaterialization(
            asset_key="raw_dataset", description="Result from reading delta table"
        )
    )
    return df.toPandas()

@graph(
    description="This graph contains scrapping wg and putting it to JSON s3 and creating a delta table"
)
def scrape_convert():
    json_data = scrap_wg()
    df = write_json_to_s3(json_data)
    create_delta_table(json_data)