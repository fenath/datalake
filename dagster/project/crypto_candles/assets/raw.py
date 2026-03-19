from dagster import asset

@asset
def raw_crypto_files():
    return [
        "cryptocurrency/BTCUSDT-5m-2026-01.csv",
        "cryptocurrency/BTCUSDT-5m-2025-01.csv",
        "cryptocurrency/BTCUSDT-5m-2025-02.csv",
        "cryptocurrency/BTCUSDT-5m-2025-03.csv",
        "cryptocurrency/BTCUSDT-5m-2025-04.csv",
        "cryptocurrency/BTCUSDT-5m-2025-05.csv",
        "cryptocurrency/BTCUSDT-5m-2025-06.csv",
        "cryptocurrency/BTCUSDT-5m-2025-07.csv",
        "cryptocurrency/BTCUSDT-5m-2025-08.csv",
        "cryptocurrency/BTCUSDT-5m-2025-09.csv",
        "cryptocurrency/BTCUSDT-5m-2025-10.csv",
        "cryptocurrency/BTCUSDT-5m-2025-11.csv",
        "cryptocurrency/BTCUSDT-5m-2025-12.csv",
    ]
