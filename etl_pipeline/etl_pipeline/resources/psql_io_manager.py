import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine


class PostgreSQLIOManager(IOManager):

    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # insert new data from Pandas Dataframe to PostgreSQL table

        # Create a connection string
        conn_str = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**self._config)

        # Create a SQLAlchemy engine
        engine = create_engine(conn_str)

        table_name = self._config["table_name"]

        # Insert the DataFrame into the table
        obj.to_sql(table_name, schema='warehouse', con=engine, if_exists='replace', index=False)

        context.log.info(f'DataFrame inserted into PostgreSQL table: {table_name}, schema: warehouse')

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass