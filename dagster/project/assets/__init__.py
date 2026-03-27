import dagster as dg
from . import bronze, silver, gold


bronze_assets = dg.load_assets_from_package_module(bronze, group_name="BRONZE")
silver_assets = dg.load_assets_from_package_module(silver, group_name="SILVER")
gold_assets = dg.load_assets_from_package_module(gold, group_name="GOLD")

all_assets = [
        *bronze_assets,
        *silver_assets,
        *gold_assets,
        ]
