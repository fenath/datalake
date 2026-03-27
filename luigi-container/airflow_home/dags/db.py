import duckdb
import logging
from contextlib import contextmanager

from pathlib import Path

DBPATH = Path(__file__).parent / 'duck.db'

MINIO_URL = 'localhost:9000'
REST_URL = 'http://localhost:8181'

@contextmanager
def get_connection(minio=MINIO_URL, 
                   rest=REST_URL):
    con = duckdb.connect(DBPATH.absolute())
    logger = logging.getLogger("duck")
    logger.debug('duckdb conectado')

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    logger.debug('instalou httpfs e iceberg')

    # Configuração do Secret MinIO
    con.execute(f"""
                CREATE OR REPLACE SECRET minio (
                    TYPE S3,
                    KEY_ID 'minioadmin',
                    SECRET 'minioadmin',
                    --ENDPOINT 'minio:9000',
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

def duck_conn(func, 
              rest_url: str = REST_URL,
              minio_url: str = MINIO_URL
              ):
    def wrapper(*args, **kwargs):
        with get_connection() as con:
            if 'con' in kwargs:
                con = kwargs.pop('con')
            func(*args, con=con, **kwargs)
    return wrapper

