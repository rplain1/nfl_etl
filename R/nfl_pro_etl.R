
## must acquire authorization token by logging into NFL Pro:
### 1) log into NFL pro and open any page where a table is populating
### 2) open up "Developer Tools" (F12 or Ctrl + Shift + I)
### 3) navigate to "Network" ribbon
### 4) find an XHR request type on the list of requests and click on it
### 5) scroll down to the "Request Headers" section
### 6) find the string to the right of "Authorization"
### 7) copy that string and paste as your token as a character string below
### the token is good for 60 minutes after being generated

#this token is now invalid
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjbGllbnRJZCI6ImU1MzVjN2MwLTgxN2YtNDc3Ni04OTkwLTU2NTU2ZjhiMTkyOCIsImNsaWVudEtleSI6IjRjRlVXNkRtd0pwelQ5TDdMckczcVJBY0FCRzVzMDRnIiwiaXNzIjoiTkZMIiwiZGV2aWNlSWQiOiJmN2EwNmUwZC0yNTE5LTQ2MjYtODdlYy0xYjQyMmE4MTU0ZTIiLCJwbGFucyI6W3sicGxhbiI6ImZyZWUiLCJleHBpcmF0aW9uRGF0ZSI6IjIwMjUtMTAtMDIiLCJzb3VyY2UiOiJORkwiLCJzdGFydERhdGUiOiIyMDI0LTEwLTAxIiwic3RhdHVzIjoiQUNUSVZFIiwidHJpYWwiOmZhbHNlfSx7InBsYW4iOiJORkxfUExVU19QUkVNSVVNIiwicHVyY2hhc2VDaGFubmVsIjoiIiwiYmlsbGluZ1R5cGUiOiJtb250aGx5IiwiZXhwaXJhdGlvbkRhdGUiOiIyMDI0LTEwLTA1IiwiZXh0ZXJuYWxTdWJzY3JpcHRpb25JZCI6IjU3MDAwMTAxMDE5MzUzOSIsInNvdXJjZSI6IkFQUExFIiwic3RhcnREYXRlIjoiMjAyMi0wOS0xMSIsInN0YXR1cyI6IkFDVElWRSIsInRyaWFsIjp0cnVlfSx7InBsYW4iOiJORkxfUExVU19QUkVNSVVNIiwicHVyY2hhc2VDaGFubmVsIjoiIiwiYmlsbGluZ1R5cGUiOiJtb250aGx5IiwiZXhwaXJhdGlvbkRhdGUiOiIyMDI0LTEwLTA1IiwiZXh0ZXJuYWxTdWJzY3JpcHRpb25JZCI6IjU3MDAwMTAxMDE5MzUzOSIsInNvdXJjZSI6IkFQUExFIiwic3RhcnREYXRlIjoiMjAyMi0wOS0xMSIsInN0YXR1cyI6IkFDVElWRSIsInRyaWFsIjp0cnVlfV0sIkRpc3BsYXlOYW1lIjoiV0VCX0RFU0tUT1BfREVTS1RPUCIsIk5vdGVzIjoiIiwiZm9ybUZhY3RvciI6IkRFU0tUT1AiLCJsdXJhQXBwS2V5IjoiU1pzNTdkQkdSeGJMNzI4bFZwN0RZUSIsInBsYXRmb3JtIjoiREVTS1RPUCIsInByb2R1Y3ROYW1lIjoiV0VCIiwicm9sZXMiOlsiY29udGVudCIsImV4cGVyaWVuY2UiLCJmb290YmFsbCIsInV0aWxpdGllcyIsInRlYW1zIiwicGxheSIsImxpdmUiLCJpZGVudGl0eSIsIm5nc19zdGF0cyIsInBheW1lbnRzX2FwaSIsIm5nc190cmFja2luZyIsIm5nc19wbGF0Zm9ybSIsIm5nc19jb250ZW50IiwibmdzX2NvbWJpbmUiLCJuZ3NfYWR2YW5jZWRfc3RhdHMiLCJuZmxfcHJvIiwiZWNvbW0iLCJuZmxfaWRfYXBpIiwiZnJlZSIsIk5GTF9QTFVTX1BSRU1JVU0iLCJORkxfUExVU19QUkVNSVVNIl0sImNpdHkiOiJjb2xvcmFkbyBzcHJpbmdzIiwiY291bnRyeUNvZGUiOiJVUyIsImRtYUNvZGUiOiI3NTIiLCJobWFUZWFtcyI6WyIxMDQwMTQwMC1iODliLTk2ZTUtNTVkMS1jYWE3ZTE4ZGUzZDgiXSwicmVnaW9uIjoiQ08iLCJ6aXBDb2RlIjoiODA5MDQiLCJicm93c2VyIjoiQ2hyb21lIiwiY2VsbHVsYXIiOmZhbHNlLCJlbnZpcm9ubWVudCI6InByb2R1Y3Rpb24iLCJ1aWQiOiI5YmI1ZTI0YzQ2OTE0MDA4YmYzNDRiNWJmYzA2ZDZhYiIsImV4cCI6MTcyNzgyOTY4OH0.j96a3Ygh81meT7gVJY5AY_Y_qKHFxuKsCEAe7MRmiZo'

NFLPRO_single_game_table <- function(token, table_type = 'passing', week = 1, season = 2024) {
  message(glue::glue("Pulling data from: SEASON {season} WEEK {week}"))
  ### take a two second break in between calls
  ### only need this step if you're running this function repeatedly
  Sys.sleep(2)

  ### vector to map week to appropriate text
  season_length = ifelse(season < 2021, 17, 18)
  week_slug_vec = c(paste0('WEEK_', 1:season_length), 'WC', 'DIV', 'CONF', 'SB')

  ### proper table url
  url_modifier = ifelse(table_type == 'defending', 'defense/overview', paste0('players-offense/', table_type))

  httr::GET(
    url = paste0('https://pro.nfl.com/api/stats/', url_modifier, '/week?season=', season, '&week=', week_slug_vec[week],'&limit=3997'),
    httr::add_headers(Authorization = token)
  ) |>
    httr::content(as = 'parsed') |>
    (function(i) i[[gsub('ing', 'ers', table_type)]])() |>
    data.table::rbindlist(b, fill = TRUE) |>
    tibble::as_tibble() |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      season = season,
      table_type = table_type,
      week = week
    )

}

### equipped to handle passing, rushing, receiving, and defending table_type
# single weeks for 2024
rush_df <- NFLPRO_single_game_table(
  token = token,
  table_type = 'rushing',
  week = 1,
  season = 2024
)

passing_df <- map(.x = 1:4, ~NFLPRO_single_game_table(
  token = token,
  table_type = 'passing',
  week = .x,
  season = 2024
)) |>
  dplyr::bind_rows()

receving_df <- map(.x = 1:4, ~NFLPRO_single_game_table(
  token = token,
  table_type = 'receiving',
  week = .x,
  season = 2024
)) |>
  dplyr::bind_rows()

defense_df <- map(.x = 1:4, ~NFLPRO_single_game_table(
  token = token,
  table_type = 'defending',
  week = .x,
  season = 2024
)) |>
  dplyr::bind_rows()

### some of the fields provided by NFL Pro can be joined to other public data
### here is how to join to the proper team, player, and game


seasons <- 2018:2022
pass_data <- map(seasons, function(szn) {
  season_length <- ifelse(szn < 2021, 17, 18)
  purrr::map(1:season_length, ~ NFLPRO_single_game_table(token, table_type = 'passing', week = .x, season = szn))
}) |>
  dplyr::bind_rows()

rush_data <- map(seasons, function(szn) {
  season_length <- ifelse(szn < 2021, 17, 18)
  purrr::map(1:season_length, ~ NFLPRO_single_game_table(token, table_type = 'rushing', week = .x, season = szn))
}) |>
  dplyr::bind_rows()

rec_data <- map(seasons, function(szn) {
  season_length <- ifelse(szn < 2021, 17, 18)
  purrr::map(1:season_length, ~ NFLPRO_single_game_table(token, table_type = 'receiving', week = .x, season = szn))
}) |>
  dplyr::bind_rows()

def_data <- map(seasons, function(szn) {
  season_length <- ifelse(szn < 2021, 17, 18)
  purrr::map(1:season_length, ~ NFLPRO_single_game_table(token, table_type = 'defense', week = .x, season = szn))
}) |>
  dplyr::bind_rows()


teams_df <- nflreadr::load_teams()
roster_df <- nflreadr::load_rosters(2024)
sched_df <- nflreadr::load_schedules(2024) |>
  dplyr::mutate(old_game_id = as.numeric(old_game_id))

receving_df |>
  dplyr::left_join(teams_df, by = dplyr::join_by(teamId == team_id)) |>
  dplyr::left_join(roster_df, by = dplyr::join_by(nflId == gsis_it_id)) |>
  dplyr::left_join(sched_df, by = dplyr::join_by(gameId == old_game_id))






duckdb::dbWriteTable(con, DBI::Id("BASE", "NGS_PASSING_PREV_YEARS"), pass_data)
duckdb::dbWriteTable(con, DBI::Id("BASE", "NGS_RECEIVING_PREV_YEARS"), rec_data)
duckdb::dbWriteTable(con, DBI::Id("BASE", "NGS_RUSHING_PREV_YEARS"), rush_data)
duckdb::dbWriteTable(con, DBI::Id("BASE", "NGS_RECEIVING_CURRENT_YEAR"), receving_df)
duckdb::dbWriteTable(con, DBI::Id("BASE", "NGS_RUSHING_CURRENT_YEAR"), receving_df)
