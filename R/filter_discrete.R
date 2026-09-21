#' Discrete filter methods for the `db` source
#'
#' S7 methods implementing the `discrete` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterDiscrete, db_class)` and mirror
#' the built-in `tblist` behaviour, but operate lazily on `dbplyr` table
#' connections (statistics are pulled with `dplyr::collect()`).
#'
#' @section Method arguments:
#' - `filter`: A `cohortBuilder` discrete filter (`CbFilterDiscrete`).
#' - `source`: A `db` source object.
#' - `data_object`: Named list of `dbplyr` table connections.
#' - `name`: Optional statistic name(s) to compute.
#' - `cache_object`: Cached statistics used to derive default parameter values.
#' - `...`: Extra arguments.
#'
#' @name db-filter-discrete
#' @return
#' - `cb_filter_data`: the filtered `data_object`.
#' - `cb_get_filter_stats`: a named list (or single value) of statistics.
#' - `cb_get_filter_data`: the filter-related column (lazy).
#' - `cb_get_filter_defaults`: a list of default parameter values.
#' - `cb_filter_to_expr`: an `rlang` expression for reproducible code.
#' - `cb_domain_from_data`: the observed distinct values.
NULL

S7::method(
  cb_filter_data,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  value <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

  if (keep_na && !identical(value, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!sym(variable) %in% !!c(value) | is.na(!!sym(variable)))
  }
  if (!keep_na && identical(value, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!is.na(!!sym(variable)))
  }
  if (!keep_na && !identical(value, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!sym(variable) %in% !!value)
  }
  attr(data_object[[dataset]], "filtered") <- TRUE
  data_object
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, name, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  if (missing(name)) {
    name <- c("n_data", "choices", "n_missing")
  }

  stats <- list(
    choices = if ("choices" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::select(!!sym(variable)) %>%
        dplyr::filter(!is.na(!!sym(variable))) %>%
        dplyr::group_by(!!sym(variable)) %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      stats::setNames(as.integer(res$n), res[[variable]])
    },
    n_data = if ("n_data" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::select(!!sym(variable)) %>%
        dplyr::filter(!is.na(!!sym(variable))) %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    },
    n_missing = if ("n_missing" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::select(!!sym(variable)) %>%
        dplyr::filter(is.na(!!sym(variable))) %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    }
  )
  if (length(name) == 1) stats[[name]] else stats[name]
}

S7::method(
  cb_plot_filter_data,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  column <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::collect() %>%
    dplyr::pull(1)
  if (length(column)) {
    column %>% table() %>% prop.table() %>% graphics::barplot()
  } else {
    graphics::barplot(0, ylim = c(0, 0.1), main = "No data")
  }
}

S7::method(
  cb_get_filter_data,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  list(value = names(cache_object$choices))
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  value <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

  if (keep_na && !identical(value, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!!sym(variable) %in% !!c(value) | is.na(!!sym(variable)))
    })
  } else if (!keep_na && identical(value, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!is.na(!!sym(variable)))
    })
  } else if (!keep_na && !identical(value, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!!sym(variable) %in% !!value)
    })
  } else {
    NULL
  }
}

S7::method(
  cb_domain_from_data,
  list(CbFilterDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  res <- data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable)) %>%
    dplyr::filter(!is.na(!!sym(filter@variable))) %>%
    dplyr::distinct() %>%
    dplyr::collect()
  as.character(res[[filter@variable]])
}
