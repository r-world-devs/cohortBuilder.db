#' Date range filter methods for the `db` source
#'
#' S7 methods implementing the `date_range` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterDateRange, db_class)`.
#'
#' Dates are frequently stored as numeric values in databases (e.g. SQLite), so
#' collected min/max values are coerced back with [as.Date()].
#'
#' @name db-filter-date-range
#' @return See [db-filter-discrete].
NULL

db_date_range_minmax <- function(data_object, dataset, variable) {
  res <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::summarise(
      min_val = min(!!sym(variable), na.rm = TRUE),
      max_val = max(!!sym(variable), na.rm = TRUE)
    ) %>%
    dplyr::collect()
  if (nrow(res) == 0 || is.na(res$min_val)) {
    return(NULL)
  }
  c(
    as.Date(res$min_val, origin = "1970-01-01"),
    as.Date(res$max_val, origin = "1970-01-01")
  )
}

db_date_range_frequencies <- function(filter, data_object) {
  dataset <- filter@dataset
  variable <- filter@variable
  extra_params <- filter@extra

  minmax <- db_date_range_minmax(data_object, dataset, variable)
  empty <- data.frame(
    level = character(0), count = numeric(0),
    l_bound = numeric(0), u_bound = numeric(0),
    stringsAsFactors = FALSE
  )
  if (is.null(minmax)) {
    return(empty)
  }
  min_val <- minmax[1]
  max_val <- minmax[2]

  n_data <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::collect() %>%
    dplyr::pull(n) %>%
    as.integer()

  if (min_val == max_val) {
    return(
      data.frame(
        level = "1", count = n_data,
        l_bound = min_val, u_bound = max_val,
        stringsAsFactors = FALSE
      )
    )
  }

  step <- "day"
  if (!is.null(extra_params$step)) {
    step <- extra_params$step
  } else {
    max_breaks <- getOption("cb_range_stats_max_breaks", 1000L)
    n_days <- as.numeric(max_val - min_val, units = "days")
    if (n_days > max_breaks) {
      step <- ceiling(n_days / max_breaks)
    }
  }
  breaks <- seq.Date(min_val, max_val, by = step)
  if (rev(breaks)[1] != max_val) {
    breaks[length(breaks) + 1] <- max_val
  }

  per_date_counts <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::group_by(!!sym(variable)) %>%
    dplyr::summarise(count = dplyr::n()) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      count = as.integer(count),
      !!(variable) := as.Date(!!sym(variable), origin = "1970-01-01")
    )

  # Assign each observed date to its break bin and aggregate.
  bin <- findInterval(per_date_counts[[variable]], breaks, rightmost.closed = FALSE)
  bin[bin < 1] <- 1
  bin[bin > length(breaks)] <- length(breaks)
  counts <- tapply(per_date_counts$count, factor(bin, levels = seq_along(breaks)), sum)
  counts[is.na(counts)] <- 0L

  data.frame(
    level = as.character(seq_along(breaks)),
    count = as.integer(counts),
    l_bound = breaks,
    u_bound = c(breaks[-1], breaks[length(breaks)]),
    stringsAsFactors = FALSE
  )
}

# Databases commonly store dates as numbers (e.g. SQLite keeps days since
# 1970-01-01). dbplyr translates a Date bound to a quoted string literal, which
# never matches a numeric column, so date bounds are coerced to numeric before
# building the predicate.
db_date_range_filter_data <- function(filter, data_object) {
  dataset <- filter@dataset
  variable <- filter@variable
  range <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

  if (!identical(range, NA)) {
    range <- as.numeric(as.Date(range, origin = "1970-01-01"))
  }

  if (keep_na && !identical(range, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(
        (!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1]) |
          is.na(!!sym(variable))
      )
  }
  if (!keep_na && identical(range, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!is.na(!!sym(variable)))
  }
  if (!keep_na && !identical(range, NA)) {
    data_object[[dataset]] <- data_object[[dataset]] %>%
      dplyr::filter(!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1])
  }
  attr(data_object[[dataset]], "filtered") <- TRUE
  data_object
}

db_date_range_filter_to_expr <- function(filter) {
  dataset <- filter@dataset
  variable <- filter@variable
  range <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

  if (!identical(range, NA)) {
    range <- as.numeric(as.Date(range, origin = "1970-01-01"))
  }

  if (keep_na && !identical(range, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(
          (!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1]) |
            is.na(!!sym(variable))
        )
    })
  } else if (!keep_na && identical(range, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!is.na(!!sym(variable)))
    })
  } else if (!keep_na && !identical(range, NA)) {
    rlang::expr({
      data_object[[!!dataset]] <- data_object[[!!dataset]] %>%
        dplyr::filter(!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1])
    })
  } else {
    NULL
  }
}

# -- CbFilterDateRange x db_class ---------------------------------------------

S7::method(
  cb_filter_data,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, ...) {
  db_date_range_filter_data(filter, data_object)
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, name, ...) {
  dataset <- filter@dataset
  variable <- filter@variable
  if (missing(name)) {
    name <- c("n_data", "frequencies", "min", "max", "n_missing")
  }

  minmax <- NULL
  if (any(c("min", "max") %in% name)) {
    minmax <- db_date_range_minmax(data_object, dataset, variable)
  }

  stats <- list(
    frequencies = if ("frequencies" %in% name) db_date_range_frequencies(filter, data_object),
    min = if ("min" %in% name) minmax[1],
    max = if ("max" %in% name) minmax[2],
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
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, ...) {
  column <- data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable)) %>%
    dplyr::filter(!is.na(!!sym(filter@variable))) %>%
    dplyr::collect() %>%
    dplyr::pull(1)
  column <- as.Date(column, origin = "1970-01-01")
  if (length(column)) {
    graphics::hist(column, main = NULL, xlab = filter@variable, breaks = "days")
  } else {
    graphics::barplot(0, ylim = c(0, 0.1), main = "No data")
  }
}

S7::method(
  cb_get_filter_data,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  db_range_get_defaults(filter, cache_object)
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, ...) {
  db_date_range_filter_to_expr(filter)
}

S7::method(
  cb_domain_from_data,
  list(CbFilterDateRange, db_class)
) <- function(filter, source, data_object, ...) {
  db_date_range_minmax(data_object, filter@dataset, filter@variable)
}
