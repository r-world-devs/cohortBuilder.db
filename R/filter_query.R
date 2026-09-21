#' Query filter methods for the `db` source
#'
#' S7 methods implementing the `query` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterQuery, db_class)` and mirror the built-in
#' `tblist` behaviour, but operate lazily on `dbplyr` table connections
#' (statistics are pulled with `dplyr::collect()`).
#'
#' @section Method arguments:
#' - `filter`: A `cohortBuilder` query filter (`CbFilterQuery`).
#' - `source`: A `db` source object.
#' - `data_object`: Named list of `dbplyr` table connections.
#' - `name`: Optional statistic name(s) to compute.
#' - `cache_object`: Cached statistics used to derive default parameter values.
#' - `...`: Extra arguments.
#'
#' @section Limitations:
#' `queryBuilder::queryToExpr()` renders some operators as R functions that have
#' no SQL translation. Those are rewritten to database friendly equivalents,
#' which makes two operator groups behave slightly differently than for
#' `tblist`:
#'
#' - `contains`, `begins_with` and `ends_with` are translated to SQL `LIKE`, so
#'   the provided value is matched as a literal pattern and not as a regular
#'   expression.
#' - `LIKE` case sensitivity is defined by the database backend ('SQLite'
#'   matches ASCII case insensitively, 'PostgreSQL' case sensitively).
#'
#' @name db-filter-query
#' @return
#' - `cb_filter_data`: the filtered `data_object`.
#' - `cb_get_filter_stats`: a named list (or single value) of statistics.
#' - `cb_get_filter_data`: the filter-related columns (lazy).
#' - `cb_get_filter_defaults`: a list of default parameter values.
#' - `cb_filter_to_expr`: an `rlang` expression for reproducible code.
NULL

# Escape the `LIKE` wildcards so that the value is matched literally.
db_query_like_pattern <- function(value, prefix = "", suffix = "") {
  paste0(prefix, gsub("([%_])", "\\\\\\1", as.character(value)), suffix)
}

# `col %LIKE% pattern` call. `%LIKE%` has no R implementation, it exists only as
# a `dbplyr` SQL translation, so the call is built instead of being written down.
db_query_like <- function(column, pattern) {
  rlang::call2("%LIKE%", column, pattern)
}

# Name of the function called by `expr`, ignoring the `pkg::` qualifier.
db_query_call_name <- function(expr) {
  fun <- expr[[1]]
  if (rlang::is_call(fun, "::")) {
    return(as.character(fun[[3]]))
  }
  if (rlang::is_symbol(fun)) {
    return(as.character(fun))
  }
  ""
}

# Replace a single non-translatable `queryBuilder` operator call, or return NULL
# when the call needs no rewriting.
db_query_rewrite_call <- function(expr) {
  operator <- db_query_call_name(expr)
  if (!operator %in% c("in_range", "is_empty", "in_string", "startsWith", "endsWith")) {
    return(NULL)
  }
  args <- as.list(expr)[-1]
  column <- args[[1]]
  # The rewritten operators are always called with a constant value.
  value <- if (length(args) > 1) eval(args[[2]]) else NULL

  switch(operator,
    # `in_range()` is exclusive on both ends.
    in_range = rlang::expr((!!column > !!value[1] & !!column < !!value[2])),
    is_empty = rlang::expr((!!column == "")),
    in_string = db_query_like(column, db_query_like_pattern(value, "%", "%")),
    startsWith = db_query_like(column, db_query_like_pattern(value, suffix = "%")),
    endsWith = db_query_like(column, db_query_like_pattern(value, prefix = "%")),
    NULL
  )
}

# Walk the expression tree returned by `queryToExpr()` and swap the calls that
# `dbplyr` cannot translate. Negated operators (`!in_range(...)`) are handled by
# the recursion into the inner call.
db_query_translate <- function(expr) {
  if (!rlang::is_call(expr)) {
    return(expr)
  }
  rewritten <- db_query_rewrite_call(expr)
  if (!is.null(rewritten)) {
    return(rewritten)
  }
  as.call(lapply(as.list(expr), db_query_translate))
}

# Predicate applied to the data, or NULL when the filter constrains nothing.
db_query_predicate <- function(value, keep_na) {
  if (identical(value, NA)) {
    return(NULL)
  }
  db_query_translate(queryBuilder::queryToExpr(value, keep_na = keep_na))
}

# -- CbFilterQuery x db_class -------------------------------------------------

S7::method(
  cb_filter_data,
  list(CbFilterQuery, db_class)
) <- function(filter, source, data_object, ...) {
  dataset <- filter@dataset
  predicate <- db_query_predicate(filter@value, filter@keep_na)

  if (!is.null(predicate)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!predicate)
  }
  attr(data_object[[dataset]], "filtered") <- TRUE
  data_object
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterQuery, db_class)
) <- function(filter, source, data_object, name, ...) {
  dataset <- filter@dataset
  variables <- unlist(filter@variables)
  if (missing(name)) {
    name <- c("n_data", "specs", "n_missing")
  }
  columns <- data_object[[dataset]] %>%
    dplyr::select(dplyr::all_of(variables))

  stats <- list(
    specs = if ("specs" %in% name) db_query_specs(columns),
    n_data = if ("n_data" %in% name) {
      res <- columns %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    },
    n_missing = if ("n_missing" %in% name) {
      purrr::map(rlang::set_names(variables), function(variable) {
        res <- columns %>%
          dplyr::filter(is.na(!!sym(variable))) %>%
          dplyr::summarise(n = dplyr::n()) %>%
          dplyr::collect()
        as.integer(res$n)
      })
    }
  )
  if (length(name) == 1) stats[[name]] else stats[name]
}

# Distinct column values, limited to `cb_db_query_max_values` entries.
db_query_values <- function(columns, variable) {
  max_values <- getOption("cb_db_query_max_values", 500L)
  res <- columns %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::distinct() %>%
    utils::head(n = max_values + 1) %>%
    dplyr::collect()

  values <- res[[variable]]
  if (length(values) > max_values) {
    rlang::warn(glue::glue(
      "Column {sQuote(variable)} has more than {max_values} distinct values, ",
      "only the first ones are used by the query filter. ",
      "Set the {sQuote('cb_db_query_max_values')} option to change the limit."
    ))
    values <- values[seq_len(max_values)]
  }
  values
}

# `queryBuilder` column specifications, mirroring `queryBuilder:::stat_from_column()`.
db_query_specs <- function(columns) {
  classes <- purrr::map_chr(dplyr::collect(utils::head(columns, 0)), ~ class(.x)[1])

  purrr::imap(classes, function(column_class, variable) {
    if (column_class %in% c("integer", "numeric", "Date", "POSIXct")) {
      bounds <- columns %>%
        dplyr::summarise(
          min = min(!!sym(variable), na.rm = TRUE),
          max = max(!!sym(variable), na.rm = TRUE)
        ) %>%
        dplyr::collect()
      validation <- list(min = bounds$min, max = bounds$max)
      if (identical(column_class, "integer")) {
        validation$min <- as.integer(validation$min)
        validation$max <- as.integer(validation$max)
        validation$step <- 1
      }
      return(list(class = column_class, validation = validation))
    }
    list(class = column_class, values = db_query_values(columns, variable))
  })
}

S7::method(
  cb_plot_filter_data,
  list(CbFilterQuery, db_class)
) <- function(filter, source, data_object, ...) {
  columns <- data_object[[filter@dataset]] %>%
    dplyr::select(dplyr::all_of(unlist(filter@variables))) %>%
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
  list(CbFilterQuery, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(dplyr::all_of(unlist(filter@variables)))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterQuery, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  list(value = names(cache_object$choices))
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterQuery, db_class)
) <- function(filter, source, ...) {
  dataset <- filter@dataset
  predicate <- db_query_predicate(filter@value, filter@keep_na)
  if (is.null(predicate)) {
    return(NULL)
  }
  rlang::expr({
    data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
      dplyr::filter(!!predicate)
  })
}
