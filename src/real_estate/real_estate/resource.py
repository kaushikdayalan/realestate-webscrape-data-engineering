from dagster_spark.configs_spark import spark_config
from dagster_spark.utils import flatten_dict
from pyspark.sql import SparkSession
import dagster._check as check
from dagster import resource
from delta import *

access_key = "AKIA44FNDXAER3ZGVO4U"
secret_key = "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"
hadoop_aws = "/home/azureuser/pyspark_env/src/real_estate/real_estate/notebooks/jars/hadoop-aws-3.2.3.jar"
hadoop_jdk = "/home/azureuser/pyspark_env/src/real_estate/real_estate/notebooks/jars/aws-java-sdk-bundle-1.11.901.jar"
jety3 = "/home/azureuser/pyspark_env/src/real_estate/real_estate/notebooks/jars/jets3t-0.9.4.jar"
delta_core = "/home/azureuser/pyspark_env/src/real_estate/real_estate/notebooks/jars/delta-core_2.13-1.2.1.jar"

def spark_session_from_config(spark_conf=None):
    spark_conf = check.opt_dict_param(spark_conf, "spark_conf")
    builder = SparkSession.builder
    flat = flatten_dict(spark_conf)
    for key, value in flat:
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()

class PySparkResource:
    def __init__(self, spark_conf):
        self._spark_session = spark_session_from_config(spark_conf)

    @property
    def spark_session(self):
        return self._spark_session

    @property
    def spark_context(self):
        return self.spark_session.sparkContext

@resource({"spark_conf": spark_config()})
def pyspark_resource(init_context):
    """This resource provides access to a PySpark SparkSession for executing PySpark code within Dagster.

    Example:

    .. code-block:: python

        @op(required_resource_keys={"pyspark"})
        def my_op(context):
            spark_session = context.resources.pyspark.spark_session
            dataframe = spark_session.read.json("examples/src/main/resources/people.json")

        my_pyspark_resource = pyspark_resource.configured(
            {"spark_conf": {"spark.executor.memory": "2g"}}
        )

        @job(resource_defs={"pyspark": my_pyspark_resource})
        def my_spark_job():
            my_op()

    """
    return PySparkResource(init_context.resource_config["spark_conf"])


my_pyspark_resource = pyspark_resource.configured(
    {
    "spark_conf": {  
    "spark":{
        "app":{
            'name':'test'
        }
    },
    "spark.executor.memory": "4g",
    "spark.hadoop.fs.s3a.access.key":access_key,
    "spark.hadoop.fs.s3a.secret.key":secret_key,
    "spark.hadoop.fs.s3a.path.style.access":"true",
    'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.jars":f"{hadoop_aws},{hadoop_jdk},{jety3}",
    "spark.hadoop.fs.s3a.connection.ssl.enabled":"false",
    "spark.delta.logStore.class":"org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.executor.cores":"2"
     }}
)