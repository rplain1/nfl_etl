SELECT adp.name
  , adp.position
  , adp.adp
  , adp.season
  , adp.pos_rank
  , stats.week
  , stats.fantasy_points_ppr
FROM summary.FFC_ADP_IDS adp
JOIN base.PLAYER_STATS_WK stats ON adp.gsis_id = stats.player_id
