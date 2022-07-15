librarian <- cohortBuilder::librarian
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
# Create schema
tmp <- tempfile()
DBI::dbExecute(conn, paste0("ATTACH '", tmp, "' AS cb"))
dplyr::copy_to(conn, librarian$books, dbplyr::in_schema("cb", "books"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$borrowers, dbplyr::in_schema("cb", "borrowers"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$issues, dbplyr::in_schema("cb", "issues"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, librarian$returns, dbplyr::in_schema("cb", "returns"), temporary = FALSE, overwrite = TRUE)
