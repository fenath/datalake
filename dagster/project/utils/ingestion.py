import uuid
import polars as pl
from datetime import datetime, timezone

def add_ingestion_metadata(df, file): 
    return df.with_columns(
        pl.lit(file).alias("_source_file"),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingestion_time"), 
    )

def add_request_to_queue(endpoint, cat):
    endpoints = [endpoint] if isinstance(endpoint, str) else endpoint
    df = pl.DataFrame([{
        'request_id': uuid.uuid4().bytes,
        'endpoint': endpoint,
        'status': 'pending',
        'created_at': datetime.now()
    } for endpoint in endpoints])
    tbl = cat.load_table('bronze.request_queue')
    schema = tbl.schema().as_arrow()
    arrow_df = df.to_arrow().cast(schema)
    tbl.append(arrow_df)

