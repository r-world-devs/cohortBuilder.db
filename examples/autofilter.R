library(magrittr)
library(RPostgres)
library(cohortBuilder)
library(shinyCohortBuilder)
library(cohortBuilder.db)
options("cb_active_filter" = FALSE)

datasets <- list(
  patients = data.frame(
    id = 1:10,
    group = c("A", "B", "C", NA, "B", "C", "A", "B", "C", "B"),
    gender = c("F", "M", "F", "F", "F", "M", "M", "F", "F", "M"),
    age = sample(30:50, 10),
    visit = sample(seq.Date(as.Date("1989-01-01"), as.Date("1991-01-01"), by = "month"), 10),
    biom1 = c("A", "B", "A", "A", "B", "B", "B", "A", "A", "A"),
    biom2 = c("C", "D", "C", "D", "E", "E", "C", "C", "E", "C")
  ),
  therapy = data.frame(
    id = 1:10,
    treatment = c("Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Atezo", "Atezo")
  )
)

conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")

# Create schema
# DBI::dbSendQuery(conn, "CREATE SCHEMA IF NOT EXISTS cb;") # RPostgres case
tmp <- tempfile()
DBI::dbExecute(conn, paste0("ATTACH '", tmp, "' AS cb"))

dplyr::copy_to(conn, datasets$patients, dbplyr::in_schema("cb", "patients"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, datasets$therapy, dbplyr::in_schema("cb", "therapy"), temporary = FALSE, overwrite = TRUE)

data_source <- cohortBuilder::set_source(
  dbtables(
    tables = c("patients", "therapy"),
    schema = "cb",
    connection = conn
  )
) %>% autofilter()

coh <- cohort(data_source)

gui(coh)


code(coh)
