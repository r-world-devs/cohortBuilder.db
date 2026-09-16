#' @importFrom rlang %||%
NULL

get_filter_dataset_db <- function(filter) {
  filter@dataset
}

# Group filters by their source dataset, preserving `dtconn$tables` ordering.
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
  shiny::div(
    class = c("cb_filters_group", dataset_name),
    shiny::tags$strong(dataset_name),
    shiny::span(id = stats_id, style = "float: right; "),
    shiny::tags$hr(style = "margin-top: 0.3rem;"),
    filters %>%
      purrr::map(
        ~ shinyCohortBuilder::.render_filter(.x, step_id, cohort, ns = ns)
      )
  )
}

#' Filters rendering method for db source
#'
#' @param source Source object of db type.
#' @param cohort Cohort object.
#' @param step_id Id of the filtering step.
#' @param ns Namespace function.
#' @param ... Extra arguments passed to a specific method.
#'
#' @return A `shiny.tag` containing the rendered filters panel.
#' @name rendering-filters
#' @export
.render_filters.db <- function(source, cohort, step_id, ns, ...) {
  step <- cohort$get_step(step_id)

  group_filters_db(cohort$get_source(), step$filters) %>%
    purrr::keep(~ length(.x) > 0) %>%
    purrr::imap(~ dataset_filters_db(.x, .y, step_id, cohort, ns = ns)) %>%
    shiny::div(class = "cb_filters", `data-step_id` = step_id)
}

#' Update data statistics method for db source
#'
#' @param source Source object of db type.
#' @param step_id Id of the filtering step.
#' @param cohort Cohort object.
#' @param session Shiny session object.
#' @param ... Extra arguments passed to a specific method.
#'
#' @return Invisibly `NULL`; called for the side effect of updating the panel.
#' @name updating-data-statistics
#' @export
.update_data_stats.db <- function(source, step_id, cohort, session, ...) {
  stats <- cohort$attributes$stats
  if (is.null(stats)) {
    return(invisible(NULL))
  }
  step <- cohort$get_step(step_id)
  ns <- session$ns

  dataset_names <- source$dtconn$tables
  data_filters <- purrr::map_chr(step$filters, get_filter_dataset_db)
  dataset_names <- intersect(dataset_names, data_filters)

  dataset_names %>% purrr::walk(function(dataset) {
    selector <- paste0("#", ns(paste0(step_id, "-stats_", dataset)))
    pre_stats <- cohort$get_stats(step_id, state = "pre", .recalc_when_missing = TRUE)
    previous <- pre_stats[[dataset]]$n_rows
    if (is.null(previous) || !isTRUE(previous > 0)) {
      ui <- shiny::tags$span("No data selected in previous step.")
    } else {
      current <- cohort$get_stats(step_id, state = "post")[[dataset]]$n_rows
      ui <- shinyCohortBuilder::.pre_post_stats(current, previous, percent = TRUE, stats = stats)
    }
    shiny::removeUI(selector = paste0(selector, " > *"), multiple = TRUE, immediate = TRUE)
    shiny::insertUI(selector = selector, ui = ui, immediate = TRUE)
  })

  invisible(NULL)
}

#' Generate output of attrition plot for db source
#'
#' @param source Source object of db type.
#' @param id Id of attrition output.
#' @param cohort Cohort object.
#' @param session Shiny session object.
#' @param ... Extra arguments passed to specific method.
#' @return List of two objects: `render` and `output` defining rendering and
#'     output placeholder for the step attrition plot feature.
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

#' Create target filter position in UI panel for db source
#'
#' @param source Source object of db type.
#' @param step_id Id of the step that the filter belongs to.
#' @param filter Filter object.
#' @param ns Namespace function.
#' @param ... Extra arguments passed to related method.
#' @return A CSS selector string locating the filter's dataset group.
#' @rdname filter-position
#' @export
.filter_position.db <- function(source, step_id, filter, ns, ...) {
  glue::glue("#{ns(step_id)} .{filter@dataset}")
}

# -- autofilter ---------------------------------------------------------------

drop_nulls <- function(x) {
  purrr::keep(x, ~ !is.null(.))
}

rule_character <- function(table_conn, name, dataset_name) {
  stat <- table_conn %>%
    dplyr::select(col = dplyr::sym(!!name)) %>%
    dplyr::summarise(n = dplyr::n(), unique = dplyr::n_distinct(col)) %>%
    dplyr::collect()

  type <- "discrete"
  gui_input <- NULL
  if (stat$unique == stat$n) {
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

rule_factor <- rule_character

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
  if (!exists(rule_method, mode = "function")) {
    return(NULL)
  }
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
  tbl_spec <- dplyr::collect(utils::head(table_conn, 0))
  tbl_spec %>%
    purrr::imap(
      ~ filter_rule(.y, class(.x)[1], dataset_name = dataset_name, table_conn = table_conn)
    ) %>%
    drop_nulls()
}

#' Generate filters definition based on the db Source data
#'
#' The method analyses the source data structure, generates filters based on the
#' column types and attaches them to the source (as a step or as available
#' filters metadata).
#'
#' @param source Source object of db type.
#' @param attach_as Choose whether the filters should be attached as a new step,
#'    or as a list of available filters (`"meta"`, used in the filtering panel
#'    when `new_step = "configure"`). By default attached as a `step`.
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
    purrr::discard(~ is.null(.x)) %>%
    purrr::map(~ do.call(cohortBuilder::filter, .)) %>%
    unname()

  if (identical(attach_as, "meta")) {
    source$available_filters <- step_rule
  } else {
    source %>%
      cohortBuilder::add_step(do.call(cohortBuilder::step, step_rule))
  }

  source
}

#' Generate available filters choices based on the db Source data
#'
#' The method returns the available choices for the virtualSelect input.
#'
#' @param source Source object of db type.
#' @param cohort cohortBuilder cohort object.
#' @param ... Extra arguments passed to a specific method.
#' @return `shinyWidgets::prepare_choices` output value.
#'
#' @rdname available-filters-choices
#' @export
.available_filters_choices.db <- function(source, cohort, ...) {
  available_filters <- source$available_filters

  choices <- purrr::map(available_filters, function(x) {
    filter_description <- x@description
    if (!is.character(filter_description) || length(filter_description) == 0) {
      filter_description <- NA_character_
    }
    tibble::tibble(
      name = x@name,
      id = x@id,
      dataset = x@dataset,
      description = filter_description
    )
  }) %>% dplyr::bind_rows()

  shinyWidgets::prepare_choices(choices, name, id, dataset, description = description)
}
