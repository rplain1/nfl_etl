import nfl_data_py as nfl
import ibis
from ibis import _


con = ibis.duckdb.connect('data/luna.duckdb')

def update_duckdb(con, source_df, table_name, schema='BASE', db = 'data/duckdb'):

    con.con.register("temp_source", source_df)

    sql_command = f"""
    CREATE OR REPLACE TABLE {schema}.{table_name} AS
    SELECT *, get_current_timestamp() AS updated_at
    FROM source_df
    """
    con.con.execute(sql_command)
    print("Table updated successfully")

update_duckdb(con, nfl.import_snap_counts(range(2012, 2024)), "SNAP_COUNTS")
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
update_duckdb(con, nfl.import_seasonal_pfr("pass"), "PFR_SEAS_PASS")
update_duckdb(con, nfl.import_seasonal_pfr("rush"), "PFR_SEAS_RUSH")
update_duckdb(con, nfl.import_seasonal_pfr("rec"), "PFR_SEAS_REC")
update_duckdb(con, nfl.import_players(), "PLAYERS")
update_duckdb(con, nfl.import_team_desc(), "TEAM_DESC")
update_duckdb(con, nfl.clean_nfl_data(nfl.import_weekly_data(range(1999, 2025), downcast=False)), "PLAYER_STATS_WK")
con.disconnect()
