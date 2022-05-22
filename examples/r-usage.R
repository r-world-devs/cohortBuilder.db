library(RSQLite)
library(cohortBuilder)
library(cohortBuilder.db)

patients = data.frame(
  id = 1:10,
  group = c("A", "B", "C", NA, "B", "C", "A", "B", "C", "B"),
  gender = c("F", "M", "F", "F", "F", "M", "M", "F", "F", "M"),
  age = sample(30:50, 10),
  visit = sample(seq.Date(as.Date("1989-01-01"), as.Date("1991-01-01"), by = "month"), 10),
  biom1 = c("A", "B", "A", "A", "B", "B", "B", "A", "A", "A"),
  biom2 = c("C", "D", "C", "D", "E", "E", "C", "C", "E", "C")
)
therapy = data.frame(
  id = 1:10,
  treatment = c("Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Atezo", "Atezo")
)

# create valid connection
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")

# Create schema
# DBI::dbSendQuery(conn, "CREATE SCHEMA IF NOT EXISTS cb;") # RPostgres case
tmp <- tempfile()
DBI::dbExecute(conn, paste0("ATTACH '", tmp, "' AS cb"))
dplyr::copy_to(conn, patients, dbplyr::in_schema("cb", "patients"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, therapy, dbplyr::in_schema("cb", "therapy"), temporary = FALSE, overwrite = TRUE)

db_source <- set_source(
  dbtables(tables = c("patients", "therapy"), schema = "cb", connection = conn)
)
group_filter <- filter(
  type = "discrete", id = "group", name = "Group", variable = "group", dataset = "patients"
)
gender_filter <- filter(
  type = "discrete", id = "gender", name = "Gender", variable = "gender", dataset = "patients", value = "M"
)
age_filter <- cohortBuilder::filter(
  type = "range", id = "age", name = "Age", variable = "age", dataset = "patients", range = NA
)
treatment_filter <- cohortBuilder::filter(
  type = "discrete", id = "treatment", name = "Treatment", variable = "treatment", dataset = "therapy", value = "Atezo"
)

coh <- cohortBuilder::cohort(
  db_source,
  cohortBuilder::step(
    group_filter,
    gender_filter,
    age_filter,
    treatment_filter
  )
)

coh$get_data(1, state = "pre")
coh$run_flow()
coh$get_data(1, state = "post")

