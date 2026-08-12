#' Discrete text filter methods for the `db` source
#'
#' S7 methods implementing the `discrete_text` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterDiscreteText, db_class)`.
#'
#' @name db-filter-discrete-text
#' @return See [db-filter-discrete].
NULL

S7::method(
  cb_filter_data,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  value <- cohortBuilder::cb_intersect_domain(filter)

  if (!identical(value, NA)) {
    values <- trimws(strsplit(value, split = ",")[[1]])
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!sym(variable) %in% !!values)
  }
  attr(data_object[[dataset]], "filtered") <- TRUE
  data_object
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, name, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  if (missing(name)) {
    name <- c("n_data", "choices", "n_missing")
  }

  distinct_vals <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::distinct() %>%
    dplyr::collect() %>%
    dplyr::pull(1)

  stats <- list(
    choices = if ("choices" %in% name) paste(distinct_vals, collapse = ","),
    n_data = if ("n_data" %in% name) length(distinct_vals),
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
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, ...) {
  invisible(NULL)
}

S7::method(
  cb_get_filter_data,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  list(value = cache_object$choices)
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  value <- cohortBuilder::cb_intersect_domain(filter)

  if (!identical(value, NA)) {
    values <- trimws(strsplit(value, split = ",")[[1]])
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!!sym(variable) %in% !!values)
    })
  } else {
    NULL
  }
}

S7::method(
  cb_domain_from_data,
  list(CbFilterDiscreteText, db_class)
) <- function(filter, source, data_object, ...) {
  res <- data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable)) %>%
    dplyr::filter(!is.na(!!sym(filter@variable))) %>%
    dplyr::distinct() %>%
    dplyr::collect()
  values <- as.character(res[[filter@variable]])
  paste(values, collapse = ",")
}
