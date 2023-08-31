import pandas as pd
from sqlalchemy import create_engine, text


class MySQLIOManager():

    def __init__(self, config):
        # config for connecting to MySQL database
        self._config = config

    def extract_data(self, sql: str) -> pd.DataFrame:
        conn_str = "mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}".format(**self._config)

        engine = create_engine(conn_str)

        with engine.connect() as con:
            df = pd.DataFrame(con.execute(text(sql)))

        return df
