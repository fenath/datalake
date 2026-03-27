from typing import Any
from project.resources import IcebergDuckDBResource
from datetime import datetime
import dagster as dg
import polars as pl
import requests
import uuid

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

