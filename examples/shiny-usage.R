library(RPostgres)
library(cohortBuilder)
library(shinyCohortBuilder)
library(cohortBuilder.db)
options("cb_active_filter" = FALSE)

bootstrap <- 3
steps <- TRUE
run_button <- TRUE
stats <- c("pre", "post")

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

# create valid connection
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")

# Create schema
# DBI::dbSendQuery(conn, "CREATE SCHEMA IF NOT EXISTS cb;") # RPostgres case
tmp <- tempfile()
DBI::dbExecute(conn, paste0("ATTACH '", tmp, "' AS cb"))

dplyr::copy_to(conn, datasets$`01`$patients, dbplyr::in_schema("cb", "patients_01"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, datasets$`01`$therapy, dbplyr::in_schema("cb", "therapy_01"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, datasets$`02`$patients, dbplyr::in_schema("cb", "patients_02"), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, datasets$`02`$therapy, dbplyr::in_schema("cb", "therapy_02"), temporary = FALSE, overwrite = TRUE)

db_source <- set_source(
  dbtables(tables = c("patients_01", "therapy_01"), schema = "cb", connection = conn)
)
group_filter <- filter(
  type = "discrete", id = "group", name = "Group", variable = "group", dataset = "patients_01"
)
gender_filter <- filter(
  type = "discrete", id = "gender", name = "Gender", variable = "gender", dataset = "patients_01", value = "M"
)
age_filter <- cohortBuilder::filter(
  type = "range", id = "age", name = "Age", variable = "age", dataset = "patients_01", range = NA
)
# dates are unsupported by sqlite
visit_filter <- filter(
  type = "date_range", id = "visit", name = "Visit", variable = "visit", dataset = "patients_01",
  range =  NA
)
treatment_filter <- cohortBuilder::filter(
  type = "discrete", id = "treatment", name = "Treatment", variable = "treatment", dataset = "therapy_01", value = "Atezo"
)

shiny::runApp(list(
  ui = shiny::fluidPage(
    actionButton("debug", "Debug"),
    shiny::radioButtons("db", "DB", c("01", "02")),
    theme = bslib::bs_theme(version = bootstrap),
    cb_ui(id = "db", style = "width: 300px; float: left;", steps = steps),
    shiny::div(style = "float: right; width: calc(100% - 300px);",
               shiny::verbatimTextOutput("db_obj")
    )
  ),
  server = function(input, output, session) {
    coh <- cohortBuilder::cohort(
      db_source,
      cohortBuilder::step(
        group_filter,
        gender_filter,
        age_filter,
        #visit_filter,
        treatment_filter
      )
    )
    # or below if initialized without default data source
    # coh <- cohortBuilder::cohort()

    cb_server(id = "db", coh, run_button = run_button, stats = stats, feedback = TRUE)

    shiny::observeEvent(input$db, {
      group_filter <- filter(
        type = "discrete", id = "group", name = "Group", variable = "group", dataset = paste0("patients_", input$db)
      )
      gender_filter <- filter(
        type = "discrete", id = "gender", name = "Gender", variable = "gender", dataset = paste0("patients_", input$db), value = "M"
      )
      age_filter <- cohortBuilder::filter(
        type = "range", id = "age", name = "Age", variable = "age", dataset = paste0("patients_", input$db), range = NA
      )
      visit_filter <- filter(
        type = "date_range", id = "visit", name = "Visit", variable = "visit", dataset = "patients_01",
        range =  NA
      )
      treatment_filter <- cohortBuilder::filter(
        type = "discrete", id = "treatment", name = "Treatment", variable = "treatment", dataset = paste0("therapy_", input$db), value = "Atezo"
      )

      data_source <- cohortBuilder::set_source(
        dbtables(
          tables = switch(
            input$db,
            "01" = c("patients_01", "therapy_01"),
            "02" = c("patients_02", "therapy_02")
          ),
          schema = "cb",
          connection = conn
        )
      ) %>%
        cohortBuilder::add_step(
          cohortBuilder::step(
            group_filter,
            gender_filter,
            age_filter,
            visit_filter,
            treatment_filter
          )
        )
      # three options available
      # 1. keep_steps = TRUE preserves all the steps with the selected values.
      # 2. keep_steps = c(1L) or more, the only defined are preserved but also cleared (preferred option when only data changes and we want to keep first step definition)
      # 3. keep_steps = FALSE - the logic assumes the source have steps provided and renders them.
      coh$update_source(data_source, keep_steps = FALSE)
    }, ignoreInit = TRUE)

    # input[["db-cb_data_updated"]] triggers when any data was updated
    # "db-" prefix is the id passed to cb_ui and cb_server (useful when many cohortBuilder objects are created)
    returned_data <- shiny::eventReactive(input[["db-cb_data_updated"]], {
      coh$get_data(step_id = coh$last_step_id(), state = "post")
    }, ignoreInit = FALSE, ignoreNULL = FALSE)

    observeEvent(input$debug, {
      browser()
    }, ignoreInit = TRUE)

    output$db_obj <- shiny::renderPrint({
      print(returned_data())
    })
  }
))
