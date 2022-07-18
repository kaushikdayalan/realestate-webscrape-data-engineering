import json
from dagster import DagsterType

def is_json(_, value):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False

real_estate_dataframe = DagsterType(
    name = "RealEstateDataFrame",
    type_check_fn = lambda _, value: isinstance(value, list),
    description = "This is the list of data scrapped from the WG website"
)

JsonType = DagsterType(
    name = "JsonType",
    description = "a valid json representation",
    type_check_fn = is_json,
)