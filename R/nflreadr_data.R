library(tidyverse)
library(duckdb)

df_snaps <- nflreadr::load_snap_counts(2012:2023) |>
  dplyr::mutate(
    extracted_time = Sys.time()
  )


con <- dbConnect(RSQLite::SQLite(), 'data/base.sqlite')
#dbExecute(con, "CREATE SCHEMA IF NOT EXISTS NFLREADR;")
#dbWriteTable(con, Id(schema = "BASE", table = "TEST_3"), df_snaps)
dbWriteTable(con, "SNAP_COUNTS", df_snaps)
dbDisconnect(con)
