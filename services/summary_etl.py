import ibis
from ibis import _

def execute_team_game_epa(con, source_database="BASE", target_database="SUMMARY"):

    t = con.table("NFLFASTR_PBP", database=source_database)
    t2 = (
        t.filter(t.play == 1, t.special == 0).group_by(
            t.posteam,
            t.season,
            t.game_date.to_date("%Y-%m-%d").name("game_date"),
            t.play_type
        )
        .aggregate(
            total_plays = _.count(),
            total_yards = _.yards_gained.sum(),
            total_epa = _.epa.sum(),
            mean_epa = _.epa.mean()
        )
    )

    try:
        con.create_view("TEAM_GAME_EPA", t2, database=target_database, overwrite=True)
        print("View Created")
    except Exception as e:
        print(f"Failed to write duckdb")
        raise


def execute_qb_stats_season(con, source_database="BASE", target_database="SUMMARY"):

    t = con.table("PLAYER_STATS_WK", database=source_database)
    t2 = (
        t.filter(t.attempts > 0)
        .group_by(t.player_id, t.player_name, t.position_group, t.season, t.season_type)
        .aggregate(
            games=_.count(),
            att_total=_.attempts.sum(),
            att_mean=_.attempts.mean(),
            comp_total=_.completions.sum(),
            comp_mean=_.completions.mean(),
            comp_perc=_.completions.sum() / _.attempts.sum(),
            yds_total=_.passing_yards.sum(),
            yds_mean=_.passing_yards.mean(),
            pass_tds_total=_.passing_tds.sum(),
            pass_tds_mean=_.passing_tds.mean(),
            int_total=_.interceptions.sum(),
            int_mean=_.interceptions.mean(),
            sacks_total=_.sacks.sum(),
            sacks_mean=_.sacks.mean(),
            sack_yards=_.sack_yards.sum(),
            sack_fumbles=_.sack_fumbles.sum(),
            sack_fumbles_lost=_.sack_fumbles_lost.sum(),
            air_yds_total=_.passing_air_yards.sum(),
            air_yds_mean=_.passing_air_yards.mean(),
            yac_total=_.passing_yards_after_catch.sum(),
            yac_mean=_.passing_yards_after_catch.mean(),
            passing_first_downs=_.passing_first_downs.sum(),
            first_downs_mean=_.passing_first_downs.mean(),
        )
    )

    try:
        con.create_view("QB_STATS_SEASON", t2, database=target_database, overwrite=True)
        print("View Created")
    except Exception as e:
        print(f"Failed to write duckdb")
        raise


def execute_ffc_adp(con, source_database="BASE", target_database="SUMMARY"):

    # Load the BASE.IDS and BASE.FFC_ADP_PPR tables
    ids_table = con.table("IDS", database=source_database)
    adp_table = con.table("FFC_ADP_PPR", database=source_database)

    ids = ids_table.mutate(
        filter_var=(
            ibis.case()
            .when(
                (ids_table["merge_name"] == "mike williams")
                & (ids_table["gsis_id"] != "00-0033536"),
                False,
            )
            .when(
                (ids_table["merge_name"] == "steve smith")
                & (ids_table["gsis_id"] != "00-0020337"),
                False,
            )
            .when(
                (ids_table["merge_name"] == "zach miller")
                & (ids_table["gsis_id"] != "00-0027125"),
                False,
            )
            .else_(True)
            .end()
        )
    )

    adp = adp_table.mutate(
        join_name=(
            ibis.case()
            .when(adp_table["name"] == "Mike Vick", "michael vick")
            .when(
                adp_table["name"].like("% Jr"),
                adp_table["name"].lower().re_replace(" Jr", ""),
            )
            .else_(adp_table["name"].lower())
            .end()
        )
    )

    final_query = (
        adp.left_join(
            ids.filter(ids["filter_var"] == True),
            predicates=[
                ids["merge_name"] == adp["join_name"],
                ids["position"] == adp["position"],
            ],
        )
        .mutate(
            pos_rank=adp["adp"]
            .rank()
            .over(
                ibis.window(group_by=[adp["season"], adp["position"]], order_by=adp["adp"])
            )
        )
    )

    try:
        con.create_view("FFC_ADP_IDS", final_query, database=target_database, overwrite=True)
        print("View Created")
    except Exception as e:
        print(f"Failed to write duckdb")
        raise


if __name__ == '__main__':
    import os
    con = ibis.duckdb.connect("data/luna.duckdb")
    execute_team_game_epa(con)
    execute_qb_stats_season(con)
    execute_ffc_adp(con)
    con.disconnect()

    # con = ibis.duckdb.connect(f"md:belle?motherduck_token={os.getenv('motherduck_token')}")
    # execute_team_game_epa(con)
    # execute_qb_stats_season(con)
    # execute_ffc_adp(con)
    # con.disconnect()
