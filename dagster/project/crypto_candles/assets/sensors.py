import re
from dagster import sensor, RunRequest, SensorEvaluationContext
from .ops import crypto_ingestion_job

@sensor(job=crypto_ingestion_job, 
        required_resource_keys={"s3_client"},
        minimum_interval_seconds=30)
def minio_csv_sensor(context: SensorEvaluationContext):
    # Acessamos o resource s3_client (que está configurado para o MinIO)
    s3 = context.resources.s3_client
    bucket = "raw"
    prefix = "cryptocurrency/"
    
    # Listamos os arquivos do bucket
    # Nota: Usamos o cursor do sensor para não processar o mesmo arquivo duas vezes
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if "Contents" not in response:
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        
        # 1. Filtra apenas CSVs que seguem seu padrão
        # Ex: BTC-1h-2024-01.csv
        match = re.search(r"([^/]+)-([^-]+)-(\d{4})-(\d{2})\.csv$", key)
        
        if match:
            symbol, timeframe, year, month = match.groups()
            
            # 2. O run_key garante IDEMPOTÊNCIA (não processa o mesmo arquivo 2x)
            yield RunRequest(
                run_key=key,
                run_config={
                    "ops": {
                        "ingest_crypto_file_op": {
                            "config": {
                                "file_path": key,
                                "symbol": symbol,
                                "time_frame": timeframe
                            }
                        }
                    }
                }
            )
