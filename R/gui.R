get_filter_dataset_db <- function(filter) {
  filter$get_params("dataset")
}

group_filters_db <- function(source, filters) {
  tables <- source$dtconn$tables
  data_filters <- purrr::map_chr(filters, get_filter_dataset_db)
  ordered_filters <- list()
  for (table in tables) {
    ordered_filters <- append(
      ordered_filters,
      list(filters[data_filters == table])
    )
  }

  stats::setNames(ordered_filters, tables)
}

dataset_filters_db <- function(filters, dataset_name, step_id, cohort, ns) {
  stats_id <- ns(paste0(step_id, "-stats_", dataset_name))
  tagList(
    shiny::tags$strong(dataset_name),
    shiny::htmlOutput(stats_id, inline = TRUE, style = "float: right;"),
    shiny::tags$hr(style = "margin-top: 0.3rem;"),
    filters %>%
      purrr::map(
        ~ shinyCohortBuilder::.render_filter(.x, step_id, cohort, ns = ns)
      ),
    shiny::div(style = "padding-top: 1rem; padding-bottom: 1rem;")
  )
}

#' Filters rendering method for db source
#'
#' @param source Source object of db type.
#' @param cohort Cohort object.
#' @param step_id Id of the filtering step.
#' @param ns Namespace function.
#'
#' @name rendering-filters
#' @export
.render_filters.db <- function(source, cohort, step_id, ns) {
  step <- cohort$get_step(step_id)

  group_filters_db(cohort$get_source(), step$filters) %>%
    purrr::imap(~ dataset_filters_db(.x, .y, step_id, cohort, ns = ns)) %>%
    div(class = "cb_filters", `data-step_id` = step_id)
}

#' Update data statistics method for db source
#'
#' @param source Source object of db type.
#' @param step_id Id of the filtering step.
#' @param cohort Cohort object.
#' @param session Shiny session object.
#'
#' @name updating-data-statistics
#' @export
.update_data_stats.db <- function(source, step_id, cohort, session) {
  stats <- cohort$dtconn$stats

  dataset_names <- source$dtconn$tables
  dataset_names %>% purrr::walk(
    ~ shinyCohortBuilder::.sendOutput(
      paste0(step_id, "-stats_", .x),
      shiny::renderUI({
        previous <- cohort$get_cache(step_id, state = "pre")[[.x]]$n_rows
        if (!previous > 0) {
          return("No data selected in previous step.")
        }
        current <- cohort$get_cache(step_id, state = "post")[[.x]]$n_rows
        shinyCohortBuilder::.pre_post_stats(current, previous, percent = TRUE, stats = stats)
      })
    )
  )
}
