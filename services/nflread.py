import duckdb

duck_conn = duckdb.connect('data/luna.duckdb')
duck_conn.sql(
    """SELECT count(*) AS total_size
FROM read_parquet(
  list_transform(
    generate_series(2012, 2023),
    n -> 'https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_' || format('{:04d}', n) || '.parquet'
  )
); """
)


duck_conn.sql(
    """SELECT  * from
  list_transform(
    generate_series(2012, 2023),
    n -> 'https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_' || format('{:04d}', n) || '.parquet'
  ); """
)


import nfl_data_py as nfl
from ibis import _

import sqlite3
import ibis

con = ibis.duckdb.connect('data/luna.duckdb')
con.list_tables()
t = con.sql("select * from base.snap_counts")
t1 = t.group_by(t.season).aggregate(games = t.game_id.nunique())

t1.mutate(ibis.case().when(t.season == 2023, 1))

df_snaps = nfl.import_snap_counts(range(2012, 2024))
df_snaps_memtable = ibis.memtable(df_snaps)
t2 = df_snaps_memtable.group_by(df_snaps_memtable.season).aggregate(
    games=df_snaps_memtable.game_id.nunique()
)

t1.join(t2,  'season').filter(_.games != _.games_right).to_polars().shape[0]
t2


def update_duckdb(con, source_df, table_name, schema='BASE', db = 'data/duckdb'):

    con.con.register("temp_source", source_df)

    sql_command = f"""
    CREATE OR REPLACE TABLE {schema}.{table_name} AS
    SELECT *, get_current_timestamp() AS updated_at
    FROM source_df
    """
    con.con.execute(sql_command)
    print("Table updated successfully")

update_duckdb(con, df_snaps, "SNAP_COUNTS")
update_duckdb(con, nfl.import_ftn_data(range(2022, 2024), downcast=False), "FTN_DATA")
update_duckdb(con, nfl.import_ids(), "IDS")
update_duckdb(con, nfl.import_draft_picks(), "DRAFT_PICKS")
update_duckdb(con, nfl.import_draft_values(), "DRAFT_VALUES")
update_duckdb(con, nfl.import_ngs_data('passing'), "NGS_PASSING")
update_duckdb(con, nfl.import_ngs_data("receiving"), "NGS_REC")
update_duckdb(con, nfl.import_ngs_data("rushing"), "NGS_RUSH")
update_duckdb(con, nfl.import_qbr(level = 'nfl', frequency='weekly'), "QBR_WEEKLY")
update_duckdb(con, nfl.import_qbr(level="nfl", frequency="season"), "QBR_SEASON")
update_duckdb(con, nfl.import_weekly_pfr('pass'), "PFR_WK_PASS")
update_duckdb(con, nfl.import_weekly_pfr("rush"), "PFR_WK_RUSH")
update_duckdb(con, nfl.import_weekly_pfr("rec"), "PFR_WK_REC")
con.disconnect()
