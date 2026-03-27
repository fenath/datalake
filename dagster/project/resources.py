from contextlib import contextmanager
from dagster import resource
from dagster_duckdb import DuckDBResource
from pyiceberg.catalog import load_catalog
import boto3
import logging


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

class IcebergDuckDBResource(DuckDBResource):
    def post_connect(self, conn):
        pass

    def _post_connect(self, conn):
        logger = logging.getLogger("duck")
        logger.info('post-connect ativado')
        # Este código roda sempre que um asset pedir a conexão
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        
        # Configuração do Secret MinIO
        conn.execute("""
            CREATE OR REPLACE SECRET minio (
                TYPE S3,
                KEY_ID 'minioadmin',
                SECRET 'minioadmin',
                ENDPOINT 'minio:9000',
                REGION 'us-east-1',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """)
        
        # Attach do catálogo Iceberg
        conn.execute("""
            ATTACH 'dev' (
                TYPE iceberg,
                AUTHORIZATION_TYPE none,
                ENDPOINT 'http://rest:8181'
            );
        """)

    @contextmanager
    def get_connection(self):
        with super().get_connection() as con:
            self._post_connect(con)
            yield con

duckdb = IcebergDuckDBResource(database="/tmp/db.duckdb")


