import duckdb
import polars as pl
from IPython.core.magic import register_cell_magic

con = duckdb.connect("duck.db")
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_region='us-east-1';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_url_compatibility_mode=true;
""")

con.sql("""
    INSTALL httpfs;
    LOAD httpfs;
    CREATE OR REPLACE SECRET minio (
        TYPE S3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'minio:9000', -- Remove http:// prefix here
        REGION 'us-east-1',
        USE_SSL false,
        URL_STYLE 'path'
    );
""")
    
con.sql("""
    INSTALL iceberg;
    LOAD iceberg;

    
    ATTACH 'dev' (
        TYPE iceberg
        , AUTHORIZATION_TYPE none
        , ENDPOINT 'http://rest:8181'
    );

    --show schemas 
""")

@register_cell_magic
def duck(line, cell):
    rel = con.sql(cell)
    return rel.pl()
