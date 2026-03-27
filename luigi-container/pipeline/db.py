import duckdb
import logging

from pathlib import Path

DBPATH = Path(__file__).parent / 'duck.db'

def get_connection():
    con = duckdb.connect(":memory:")
    logger = logging.getLogger("duck")
    logger.debug('duckdb conectado')

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    logger.debug('instalou httpfs e iceberg')

    # Configuração do Secret MinIO
    con.execute("""
                CREATE OR REPLACE SECRET minio (
                    TYPE S3,
                    KEY_ID 'minioadmin',
                    SECRET 'minioadmin',
                    --ENDPOINT 'minio:9000',
                    ENDPOINT 'localhost:9000',
                    REGION 'us-east-1',
                    USE_SSL false,
                    URL_STYLE 'path'
                    );
                """)

    # Attach do catálogo Iceberg
    con.execute("""
                 ATTACH IF NOT EXISTS'dev' (
                     TYPE iceberg,
                     AUTHORIZATION_TYPE none,
                     --ENDPOINT 'http://rest:8181'
                     ENDPOINT 'http://localhost:8181'
                     );
                 """)
    yield con
    con.close()
