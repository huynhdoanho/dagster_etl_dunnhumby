from dagster import Definitions
from etl_pipeline.assets.bronze import make_assets
from etl_pipeline.assets.silver import (
    sales_data,
    campaign_per_household,
    redemption_per_household,
    sensitivity,
    sales_per_hh_demographic
)
from etl_pipeline.assets.gold import gold_dataset, dataset
from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager


ls_tables = [
    "campaign_table",
    "campaign_desc",
    "coupon_redempt",
    "hh_demographic",
    "transaction_data"
]

factory_assets = [make_assets(key) for key in ls_tables]


MYSQL_CONFIG = {
        "user": "admin",
        "password": "admin123",
        "host": "de_mysql",
        "port": 3306,
        "database": "dunnhumby",
        }


MINIO_CONFIG = {
        "endpoint_url": "minio:9000",
        "bucket": "warehouse",
        "aws_access_key_id": "minio",
        "aws_secret_access_key": "minio123",
        }


PSQL_CONFIG = {
        "host": "de_psql",
        "port": 5432,
        "database": "postgres",
        "user": "admin",
        "password": "admin123",
        "table_name": "dataset"
        }


defs = Definitions(
    assets=[
        *factory_assets,
        campaign_per_household,
        redemption_per_household,
        sales_data,
        sensitivity,
        sales_per_hh_demographic,
        gold_dataset,
        dataset
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)