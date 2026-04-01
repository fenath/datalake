from functools import wraps
import duckdb
import inspect
import logging
from contextlib import contextmanager
import os

from pathlib import Path

DBPATH = Path(__file__).parent / 'duck.db'

MINIO_URL = os.getenv('MINIO__URL','localhost:9000')
MINIO_USERNAME = os.getenv('MINIO__USERNAME', 'minioadmin')
MINIO_PASSOWRD = os.getenv('MINIO__PASSOWRD', 'minioadmin')
REST_URL = os.getenv('REST__URL','http://localhost:8181')

@contextmanager
def get_connection(minio=MINIO_URL, 
                   rest=REST_URL):
    con = duckdb.connect(":memory:")
    logger = logging.getLogger("duck")
    logger.debug('duckdb conectado')

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    logger.debug('instalou httpfs e iceberg')

    # Configuração do Secret MinIO
    con.execute(f"""
                CREATE OR REPLACE SECRET minio (
                    TYPE S3,
                    KEY_ID '{MINIO_USERNAME}',
                    SECRET '{MINIO_PASSOWRD}',
                    ENDPOINT '{minio}',
                    REGION 'us-east-1',
                    USE_SSL false,
                    URL_STYLE 'path'
                    );
                """)

    # Attach do catálogo Iceberg
    con.execute(f"""
                 ATTACH IF NOT EXISTS'dev' (
                     TYPE iceberg,
                     AUTHORIZATION_TYPE none,
                     --ENDPOINT 'http://rest:8181'
                     ENDPOINT '{rest}'
                     );
                 """)
    yield con
    con.close()

def duck_conn(func=None, *, rest_url: str = REST_URL, minio_url: str = MINIO_URL):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if 'con' in kwargs:
                # Conexão já fornecida externamente — respeita ela
                return f(*args, **kwargs)
            with get_connection(rest=rest_url, minio=minio_url) as con:
                return f(*args, con=con, **kwargs)

        
        # Remove 'con' da assinatura visível para o Airflow
        sig = inspect.signature(f)
        new_params = [p for name, p in sig.parameters.items() if name != 'con']
        wrapper.__signature__ = sig.replace(parameters=new_params)
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator

