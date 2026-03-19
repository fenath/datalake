from dagster import Definitions, AutomationPolicy, AssetExecutionContext
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets, DagsterDbtTranslator

from .bronze import raw_excel
from .silver import carros_astro
from .ingestion_table import ensure_ingestion_table
from .resources import iceberg_catalog, s3_client, duckdb

from .crypto_candles.assets import crypto_assets
from .crypto_candles.assets.sensors import minio_csv_sensor
from .crypto_candles.assets.ops import crypto_ingestion_job, log_job

from .nhl.assets import nhl_assets

from pathlib import Path

auto_materialize_policy = AutomationPolicy.eager()
class MyCustomTranslator(DagsterDbtTranslator):
    def get_automation_policy(self, dbt_resource_props):
        return auto_materialize_policy
my_translator = MyCustomTranslator()

dbt_project = DbtProject(
        project_dir= Path(__file__).parent / "nhl/dbt_nhl"
        )


@dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=my_translator
        )
def nhl_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


defs = Definitions(
    assets=[
        raw_excel, 
        carros_astro,
        ensure_ingestion_table,
        *crypto_assets,
        nhl_dbt_assets,
        *nhl_assets
        ],
    resources={
        "iceberg_catalog": iceberg_catalog,
        "s3_client": s3_client,
        "duckdb": duckdb,
        'dbt': DbtCliResource(project_dir=dbt_project,
                              profiles_dir=Path("/opt/dagster/app/dbt")
                              )
    },
    sensors=[
        minio_csv_sensor
        ],
    jobs=[
        crypto_ingestion_job,
        log_job
        ]
)

