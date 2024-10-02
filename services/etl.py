import polars as pl
import duckdb
import logging


class Etl:

    def __init__(self, cofig_file):
        """
        I think they used config files, because then that would be the easiest to
        swap out and decoupled from the ETL logic?

        To do this, there probably needs to be a method to connect to databases.
        I think this is why they were verbose in the the config definitions.
        """
        pass

    def extract(self):
        """
        This should pull data out and store to an attribute?
        """
        pass

    def transform(self):
        """
        This should do any transformations (i.e. add timestamp)
        """
        pass

    def load(self):
        """
        This should load the transformed (or untransformed) dataset
        """
        pass


import logging
import sqlite3
import duckdb
import configparser
import polars as pl
from datetime import datetime

CONFIG = {
    "DATABASE": {
        'SQLite' : 'data/base.base.sqlite',
        'DuckDB' : 'data/luna.luna.duckdb'
    }
}

class ETL:
    def __init__(self, config_file):
        self.config = config_file #self.load_config(config_file)
        self.raw_data = None
        self.transformed_data = None
        self.setup_logging()

    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def extract(self):
        """Extract data from SQLite database."""
        try:
            sqlite_connection_string = "sqlite://" + self.config['DATABASE']['SQLite']
            self.raw_data = pl.read_database_uri(f"SELECT * FROM {self.config['TABLE']}", sqlite_connection_string)
            logging.info("Data extraction complete.")
        except Exception as e:
            logging.error(f"Error during extraction: {e}")

    def transform(self):
        """Transform the raw data."""
        try:
            # Example transformation: Add a timestamp column
            #self.raw_data['RUN_AT'] = pl.lit(datetime.now())
            self.transformed_data = self.raw_data.with_columns(
                RUN_AT = datetime.now()
            )  # Placeholder for more complex transformations
            logging.info("Data transformation complete.")
            print('transform')
        except Exception as e:
            logging.error(f"Error during transformation: {e}")
            print("error")

    def load(self):
        """Load transformed data into DuckDB."""
        try:
            duckdb_conn = duckdb.connect(self.config['DATABASE']['DuckDB'])
            duckdb_conn.execute(f"DROP TABLE IF EXISTS {self.config['SCHEMA']}.{self.config['TABLE']}")
            tmp = self.transformed_data
            duckdb_conn.sql(
                f"""CREATE or REPLACE TABLE {self.config['SCHEMA']}.{self.config['TABLE']} AS
                SELECT * FROM tmp"""
            )
            logging.info("Data loading complete.")
            print("okay")
        except Exception as e:
            logging.error(f"Error during loading: {e}")
            print(e)
        finally:
            duckdb_conn.close()

    def run(self):
        """Run the ETL process."""
        self.extract()
        self.transform()
        self.load()

def sqlite_to_duckb(db_path, table_name, duckdb_path = 'data/luna.duckdb', schema='BASE'):

    sqlite_connection_string = "sqlite://" + db_path
    query = f"SELECT * FROM {table_name.lower()}"
    try:
        df = pl.read_database_uri(query, sqlite_connection_string)
    except Exception as e:
        logging.error(f"SQLite query failed")
        raise

    try:
        con = duckdb.connect(duckdb_path)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {schema}.{table_name} AS
            SELECT * FROM df
        """
        )
        # TODO: add some kind of check for amount of rows
        logging.info('NFLFASTR_PBP created in duckdb')

        con.close()
    except Exception as e:
        logging.error(f"Failed to write duckdb")
        raise


if __name__ == '__main__':
    print('ok')
    sqlite_to_duckb('data/pbp_db.sqlite', 'NFLFASTR_PBP', 'data/luna.duckdb')
    #sqlite_to_duckb('data/cfb_pbp_db.sqlite', 'CFBFASTR_PBP','data/luna.duckdb', schema='CFB')
    # table = 'SNAP_COUNTS'
    # config_file = {
    #     "DATABASE": {
    #         "SQLite": "data/base.sqlite",
    #         "DuckDB": "data/luna.duckdb",
    #     },
    #     "SCHEMA": "BASE",
    #     "TABLE": 'SNAP_COUNTS'
    # }
    # nflfastrETL = ETL(config_file=config_file)
    # nflfastrETL.run()
