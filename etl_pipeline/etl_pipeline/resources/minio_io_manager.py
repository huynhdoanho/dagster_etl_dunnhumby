import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio


class MinIOIOManager(IOManager):

    def __init__(self, config):
        self._config = config
        self.client = Minio(
            config["endpoint_url"],
            access_key=config["aws_access_key_id"],
            secret_key=config["aws_secret_access_key"],
            secure=False
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # upload to MinIO <bucket_name>/<key_prefix>/<your_file_name>.csv

        client = self.client

        file_name = f"{context.asset_key.path[-1]}.csv"
        obj = obj.to_csv(file_name, index=False)

        key_prefix = "/".join(context.asset_key.path)

        bucket = self._config["bucket"]

        # if client.bucket_exists(bucket):
        #     context.log.info(f"{bucket} exists")
        # else:
        #     context.log.info(f"{bucket} does not exist")
        #     context.log.info(f"Create bucket {bucket}")
        #     client.make_bucket(bucket)

        client.fput_object(
            bucket,  # bucket
            f"{key_prefix}.csv",  # object
            file_name,  # filename
        )

        context.log.info(f"Upload {key_prefix}.csv successfully!!!!!")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # download data <bucket_name>/<key_prefix>/<your_file_name>.csv from MinIO
        # read and return as Pandas Dataframe

        client = self.client

        file_name = f"{context.asset_key.path[-1]}.csv"

        key_prefix = "/".join(context.asset_key.path)

        bucket = self._config["bucket"]

        client.fget_object(
            bucket,
            f"{key_prefix}.csv",
            file_name
        )

        df = pd.read_csv(file_name)
        df = pd.DataFrame(df)

        context.log.info(f"{key_prefix}.csv is downloaded!!!")

        return df
