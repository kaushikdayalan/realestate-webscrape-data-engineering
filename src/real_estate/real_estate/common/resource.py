import boto3
from dagster import Field, resource

class boto3_connecter(object):
    def __init__(self, aws_access_key_id, aws_secret_key_id, endpoint):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_key_id = aws_secret_key_id
        self.endpoint = endpoint

    def get_client(self):
        session = boto3.session.Session()
        s3_clinet = session.client(
            service_name = 's3',
            aws_access_key_id = self.aws_access_key_id,
            aws_secret_key_id = self.aws_secret_key_id,
            endpoint_url = self.endpoint
        )
        return s3_clinet

@resource(
    config_schema = {
            'aws_access_key_id': Field(str),
            'aws_secret_key_id': Field(str),
            'endpoint_url': Field(str)
    }
)
def boto3connection(context):
    return boto3_connecter(context.resource_config['aws_access_key_id'], 
    context.resource_config['aws_secret_key_id'], context.resource_config['endpoint_url'])