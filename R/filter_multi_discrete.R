#' Multi discrete filter methods for the `db` source
#'
#' S7 methods implementing the `multi_discrete` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterMultiDiscrete, db_class)` and mirror the
#' built-in `tblist` behaviour, but operate lazily on `dbplyr` table connections
#' (statistics are pulled with `dplyr::collect()`).
#'
#' @name db-filter-multi-discrete
#' @return See [db-filter-discrete].
NULL

# Per-variable predicate mirroring the `col_in_val()` helper of the tblist
# implementation. Returns NULL when the variable puts no constraint on the data.
db_multi_discrete_var_expr <- function(variable, value, keep_na) {
  if (identical(value, NA)) {
    if (keep_na) {
      return(NULL)
    }
    return(rlang::expr(!is.na(!!sym(variable))))
  }
  if (is.null(value)) {
    # No value selected - the variable excludes every non-missing observation.
    # A literal `FALSE` is used because dbplyr cannot translate `%in%` over an
    # empty vector.
    if (keep_na) {
      return(rlang::expr(is.na(!!sym(variable))))
    }
    return(rlang::expr(FALSE))
  }
  if (keep_na) {
    return(rlang::expr(!!sym(variable) %in% !!c(value) | is.na(!!sym(variable))))
  }
  rlang::expr(!!sym(variable) %in% !!value)
}

# Combine the per-variable predicates into a single `dplyr::filter()` condition.
db_multi_discrete_predicate <- function(values, keep_na) {
  var_exprs <- purrr::compact(
    purrr::imap(values, ~ db_multi_discrete_var_expr(.y, .x, keep_na))
  )
  if (length(var_exprs) == 0) {
    return(NULL)
  }
  Reduce(function(x, y) rlang::expr(!!x & !!y), var_exprs)
}

# -- CbFilterMultiDiscrete x db_class -----------------------------------------

S7::method(
  cb_filter_data,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  dataset <- filter@dataset
  predicate <- db_multi_discrete_predicate(
    cohortBuilder::cb_intersect_domain(filter),
    filter@keep_na
  )

  if (!is.null(predicate)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!predicate)
  }
  attr(data_object[[dataset]], "filtered") <- TRUE
  data_object
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, name, ...) {
  dataset <- filter@dataset
  variables <- unlist(filter@variables)
  if (missing(name)) {
    name <- c("n_data", "choices", "n_missing")
  }

  stats <- list(
    choices = if ("choices" %in% name) {
      purrr::map(rlang::set_names(variables), function(variable) {
        res <- data_object[[dataset]] %>%
          dplyr::select(!!sym(variable)) %>%
          dplyr::filter(!is.na(!!sym(variable))) %>%
          dplyr::group_by(!!sym(variable)) %>%
          dplyr::summarise(n = dplyr::n()) %>%
          dplyr::collect()
        as.list(stats::setNames(as.integer(res$n), as.character(res[[variable]])))
      })
    },
    n_data = if ("n_data" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    },
    n_missing = if ("n_missing" %in% name) {
      purrr::map(rlang::set_names(variables), function(variable) {
        res <- data_object[[dataset]] %>%
          dplyr::select(!!sym(variable)) %>%
          dplyr::filter(is.na(!!sym(variable))) %>%
          dplyr::summarise(n = dplyr::n()) %>%
          dplyr::collect()
        as.integer(res$n)
      })
    }
  )
  if (length(name) == 1) stats[[name]] else stats[name]
}

S7::method(
  cb_plot_filter_data,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  variables <- unlist(filter@variables)
  columns <- data_object[[filter@dataset]] %>%
    dplyr::select(dplyr::all_of(variables)) %>%
    dplyr::collect()
  if (nrow(columns)) {
    counts <- purrr::imap(
      purrr::map(columns, table),
      ~ data.frame(val = as.vector(.x), row.names = names(.x)) %>%
        stats::setNames(.y)
    )
    graphics::barplot(as.matrix(dplyr::bind_cols(counts)), ...)
  } else {
    graphics::barplot(0, ylim = c(0, 0.1), main = "No data")
  }
}

S7::method(
  cb_get_filter_data,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(dplyr::all_of(unlist(filter@variables)))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  list(values = names(cache_object$choices))
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, ...) {
  dataset <- filter@dataset
  predicate <- db_multi_discrete_predicate(
    cohortBuilder::cb_intersect_domain(filter),
    filter@keep_na
  )
  if (is.null(predicate)) {
    return(NULL)
  }
  rlang::expr({
    data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
      dplyr::filter(!!predicate)
  })
}

S7::method(
  cb_domain_from_data,
  list(CbFilterMultiDiscrete, db_class)
) <- function(filter, source, data_object, ...) {
  variables <- names(filter@values)
  purrr::map(rlang::set_names(variables), function(variable) {
    res <- data_object[[filter@dataset]] %>%
      dplyr::select(!!sym(variable)) %>%
      dplyr::filter(!is.na(!!sym(variable))) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    as.character(res[[variable]])
  })
}
