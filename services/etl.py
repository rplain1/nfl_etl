import polars as pl
import duckdb
import logging


def sqlite_to_duckb(db_path, table_name, duckdb_path = 'data/luna.duckdb'):

    sqlite_connection_string = "sqlite://" + db_path
    query = f"SELECT * FROM {table_name.lower()}"
    try:
        df = pl.read_database_uri(query, sqlite_connection_string)
    except Exception as e:
        logging.error(f"SQLite query failed")
        raise

    try:
        con = duckdb.connect(duckdb_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS BASE")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE BASE.{table_name} AS
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
    sqlite_to_duckb('data/pbp_db.sqlite', 'NFLFASTR_PBP', 'data/luna.duckdb')
