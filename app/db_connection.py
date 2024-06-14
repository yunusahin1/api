import pandas as pd
from sqlalchemy import create_engine
import logging

class DBConnection:
    """Handles database connections and queries."""

    def __init__(self, user: str, password: str, host: str, port: str, dbname: str):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.engine = self.create_engine()

    def create_engine(self):
        """Creates a SQLAlchemy engine."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        try:
            engine = create_engine(connection_string)
            logging.info("Database engine created successfully.")
            return engine
        except Exception as e:
            logging.error(f"Error creating database engine: {e}")
            raise

    def fetch_data(self, query: str) -> pd.DataFrame:
        """Fetches data from the database using a SQL query."""
        try:
            df = pd.read_sql(query, self.engine)
            logging.info(f"Data fetched successfully!")
            return df
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise
