from sqlalchemy import create_engine, text


PSQL_CONFIG = {
        "host": "localhost",
        "port": 5434,
        "database": "postgres",
        "user": "admin",
        "password": "admin123",
        "table_name": "dataset"
        }


# Create a connection string
conn_str = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**PSQL_CONFIG)

# Create a SQLAlchemy engine
engine = create_engine(conn_str)

engine.connect().execute(text("DROP SCHEMA IF EXISTS warehouse;"))

# Create the schema
#engine.connect().execute(text("CREATE SCHEMA warehouse"))
