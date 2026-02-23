from dagster import resource
from pyiceberg.catalog import load_catalog
import boto3


@resource
def iceberg_catalog():

    return load_catalog(
        "dev",
        uri="http://rest:8181",
        **{
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        }
    )


@resource
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

