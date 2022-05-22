if (Sys.getenv("LOCAL", "FALSE") == "TRUE") {
  library(RPostgres)

  set.seed(123)
  datasets <- list(
    "01" = list(
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
    ),
    "02" = list(
      patients = data.frame(
        id = 1:15,
        group = c("A", "B", "C", "A", "B", "C", "A", "B", "C", "B", "D", "D", "D", "D", "D"),
        gender = c("F", "M", "F", "F", "F", "M", "M", "F", "F", "M", "F", "M", "F", "M", "F"),
        age = sample(30:50, 15),
        visit = sample(seq.Date(as.Date("1989-01-01"), as.Date("1991-01-01"), by = "month"), 15),
        biom1 = c("A", "B", "A", "A", "B", "B", "B", "A", "A", "A", "B", "A", "A", "B", "B"),
        biom2 = c("C", "D", "C", "D", "E", "E", "C", "C", "E", "C", "C", "D", "C", "D", "E"),
        biom3 = c("A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "B")
      ),
      therapy = data.frame(
        id = 1:15,
        treatment = c("Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Nebul", "Atezo", "Chemo", "Atezo", "Atezo", "Nebul", "Atezo", "Chemo", "Atezo", "Atezo")
      )
    )
  )

  # conn <- dbConnect(
  #   Postgres(),
  #   host = "<host>",
  #   port = 5432,
  #   dbname = "<dbname>",
  #   user = "<username>",
  #   password = "<password>"
  # )
  # DBI::dbSendQuery(conn, "CREATE SCHEMA IF NOT EXISTS cb;")
  # dplyr::copy_to(conn, datasets$`01`$patients, dbplyr::in_schema("cb", "patients"), temporary = FALSE, overwrite = TRUE)
  # dplyr::copy_to(conn, datasets$`01`$therapy, dbplyr::in_schema("cb", "therapy"), temporary = FALSE, overwrite = TRUE)
  #
  # db_source <- set_source(
  #   type = "db", tables = c("patients", "therapy"), schema = "cb", connection = conn
  # )
  #
  # test_that("Calling db type source returns valid structure list", {
  #   expect_equal(class(db_source), c("db", "cb_source"))
  #   expect_equal(names(db_source), c("connection", "tables", "schema", "binding_keys"))
  # })
  #
  test_that("Filtering for db works", {
  #   group_filter <- filter(
  #     type = "discrete", id = "group", name = "Group", variable = "group", dataset = "patients"
  #   )
  #   gender_filter <- filter(
  #     type = "discrete", id = "gender", name = "Gender", variable = "gender", dataset = "patients", value = "M"
  #   )
  #   age_filter <- filter(
  #     type = "range", id = "age", name = "Age", variable = "age", dataset = "patients", range = c(35, 45)
  #   )
  #   visit_filter <- filter(
  #     type = "date_range", id = "visit", name = "Visit", variable = "visit", dataset = "patients",
  #     range =  as.Date(c("1989-07-01", "1991-02-01"))
  #   )
  #   coh <- Cohort$new(
  #     db_source,
  #     step(group_filter, gender_filter, age_filter, visit_filter)
  #   )
  #   expect_setequal(unique(coh$get_data(1, state = "pre")$patients$gender), c("F", "M"))
  #   coh$run_flow()
  #   expect_setequal(unique(coh$get_data(1, state = "post")$patients$gender), c("M"))
  })
}
