SELECT
 game_id
, posteam
, season
, cast(game_date as date) as game_date
, play_type
, count(*) AS total_plays
, sum(yards_gained) total_yards
, sum(epa) total_epa
, avg(epa) mean_epa
FROM  BASE.nflfastr_pbp
WHERE  play = 1
AND special = 0
GROUP BY all
