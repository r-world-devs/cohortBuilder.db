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

#' Generate output of attrition plot
#'
#' @description
#' The method should return list of two object:
#' \itemize{
#'   \item{render}{ Rendering expression of attrition output.}
#'   \item{output}{ Output expression related to rendering (with id equal to `id` parameter).}
#' }
#' For example:
#' \preformatted{
#'   list(
#'     render = shiny::renderPlot({
#'       cohort$show_attrition()
#'     }),
#'     output = shiny::plotOutput(id)
#'   )
#' }
#'
#' @param source Source object.
#' @param id Id of attrition output.
#' @param cohort Cohort object.
#' @param session Shiny session object.
#' @param ... Extra arguments passed to specific method.
#' @return List of two objects: `render` and `output` defining rendering and
#'     output placeholder for step attrition plot feature.
#'
#' @rdname rendering-step-attrition
#' @export
.step_attrition.db <- function(source, id, cohort, session, ...) {
  ns <- session$ns
  choices <- source$dtconn$tables

  list(
    render = shiny::renderPlot({
      cohort$show_attrition(dataset = session$input$attrition_input)
    }),
    output = shiny::tagList(
      shiny::selectInput(ns("attrition_input"), "Choose dataset", choices),
      shiny::plotOutput(id)
    )
  )
}

drop_nulls <- function(x) {
  purrr::keep(x, ~!is.null(.))
}

rule_character <- function(table_conn, name, dataset_name) {
  stat <- table_conn %>%
    dplyr::select(col = dplyr::sym(!!name)) %>%
    dplyr::summarise(n = n(), unique = dplyr::n_distinct(col)) %>%
    dplyr::collect()

  type <- "discrete"
  gui_input <- NULL
  n_unique <- stat$unique
  if (n_unique == stat$n) {
    type <- "discrete_text"
  } else if (stat$unique > 3) {
    gui_input <- "vs"
  }
  drop_nulls(
    list(
      type = type,
      name = name,
      variable = name,
      dataset = dataset_name,
      value = NA,
      keep_na = TRUE,
      gui_input = gui_input
    )
  )
}

rule_factor <- function(table_conn, name, dataset_name) {
  stat <- table_conn %>%
    dplyr::select(col = dplyr::sym(!!name)) %>%
    dplyr::summarise(n = n(), unique = dplyr::n_distinct(col)) %>%
    dplyr::collect()

  type <- "discrete"
  gui_input <- NULL
  n_levels <- stat$unique
  if (n_levels == stat$n) {
    type <- "discrete_text"
  } else if (stat$unique > 3) {
    gui_input <- "vs"
  }
  drop_nulls(
    list(
      type = type,
      name = name,
      variable = name,
      dataset = dataset_name,
      value = NA,
      keep_na = TRUE,
      gui_input = gui_input
    )
  )
}

rule_numeric <- function(table_conn, name, dataset_name) {
  list(
    type = "range",
    name = name,
    variable = name,
    dataset = dataset_name,
    range = NA,
    keep_na = TRUE
  )
}
rule_integer <- rule_numeric

rule_Date <- function(table_conn, name, dataset_name) {
  list(
    type = "date_range",
    name = name,
    variable = name,
    dataset = dataset_name,
    range = NA,
    keep_na = TRUE
  )
}

filter_rule <- function(name, type, dataset_name, table_conn) {
  rule_method <- paste0("rule_", type)
  do.call(
    rule_method,
    list(
      table_conn = table_conn,
      name = name,
      dataset_name = dataset_name
    )
  )
}

filter_rules <- function(table_conn, dataset_name) {
  tbl_spec <- dplyr::collect(head(table_conn, 0))
  tbl_spec %>%
    purrr::imap(
      ~ filter_rule(.y, class(.y), dataset_name = dataset_name, table_conn = table_conn)
    )
}

#' Generate filters definition based on the Source data
#'
#' The method should analyze source data structure, generate proper filters based on
#' the data (e.g. column types) and attach them to source.
#'
#' @param source Source object.
#' @param attach_as Choose whether the filters should be attached as a new step,
#'    or list of available filters (used in filtering panel when `new_step = "configure"`).
#'    By default in \code{step}.
#' @param ... Extra arguments passed to a specific method.
#' @return Source object having step configuration attached.
#' @rdname autofilter
#' @export
autofilter.db <- function(source, attach_as = c("step", "meta"), ...) {
  attach_as <- rlang::arg_match(attach_as)
  step_rule <- source$dtconn$tables %>%
    purrr::map(
      ~ filter_rules(
          dplyr::tbl(source$dtconn$connection, dbplyr::in_schema(source$dtconn$schema, .x)),
          .x
      )
    ) %>%
    unlist(recursive = FALSE) %>%
    purrr::map(~do.call(cohortBuilder::filter, .)) %>%
    unname()

  if (identical(attach_as, "meta")) {
    source$attributes$available_filters <- step_rule
  } else {
    source %>%
      cohortBuilder::add_step(do.call(cohortBuilder::step, step_rule))
  }

  return(source)
}

#' Generate available filters choices based on the Source data
#'
#' The method should return the available choices for
#' virtualSelect input.
#'
#' @param source Source object.
#' @param cohort cohortBuilder cohort object
#' @param ... Extra arguments passed to a specific method.
#' @return `shinyWidgets::prepare_choices` output value.
#'
#' @rdname available-filters-choices
#' @export
.available_filters_choices.db <- function(source, cohort, ...) {

  available_filters <- cohort$attributes$available_filters

  choices <- purrr::map(available_filters, function(x) {
    tibble::tibble(
      name = as.character(
        shiny::div(
          `data-tooltip-z-index` = 9999,
          `data-tooltip` = x$get_params("description"),
          `data-tooltip-position` = "top right",
          `data-tooltip-allow-html` = "true",
          x$name
        )
      ),
      id = x$id,
      dataset = x$get_params("dataset")
    )
  }) %>% dplyr::bind_rows()

  shinyWidgets::prepare_choices(choices, name, id, dataset)
}
