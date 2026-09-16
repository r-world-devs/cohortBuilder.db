librarian <- cohortBuilder::librarian
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
# Create schema
tmp <- tempfile()
DBI::dbExecute(conn, paste0("ATTACH '", tmp, "' AS cb"))

dplyr::copy_to(conn, librarian$books, dbplyr::in_schema("cb", "books"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$borrowers, dbplyr::in_schema("cb", "borrowers"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$issues, dbplyr::in_schema("cb", "issues"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$returns, dbplyr::in_schema("cb", "returns"), temporary = FALSE, overwrite = TRUE)

# `librarian` has no datetime column, so a derived table is used to exercise the
# `datetime_range` filter. `genre` gets a missing value to cover `keep_na`.
events <- librarian$books
events$genre[1] <- NA
events$stamp <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC") +
  seq_len(nrow(events)) * 3600
events$stamp[2] <- NA
dplyr::copy_to(conn, events, dbplyr::in_schema("cb", "events"), temporary = FALSE, overwrite = TRUE)

withr::defer(DBI::dbDisconnect(conn), testthat::teardown_env())
