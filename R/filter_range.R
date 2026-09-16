#' Range filter methods for the `db` source
#'
#' S7 methods implementing the `range` filter for a `dbtables` source.
#' They dispatch on `list(CbFilterRange, db_class)` and mirror
#' the built-in `tblist` behaviour, but operate lazily on `dbplyr` table
#' connections (min/max and frequency statistics are pulled with
#' `dplyr::collect()`).
#'
#' @name db-filter-range
#' @return See [db-filter-discrete].
NULL

# Shared range filtering used by range / date_range db methods.
db_range_filter_data <- function(filter, data_object) {
  dataset <- filter@dataset
  variable <- filter@variable
  range <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

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

# Shared range to-expr used by range / date_range db methods.
db_range_filter_to_expr <- function(filter) {
  dataset <- filter@dataset
  variable <- filter@variable
  range <- cohortBuilder::cb_intersect_domain(filter)
  keep_na <- filter@keep_na

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

# Min/max range statistic pulled from the database.
db_range_minmax <- function(data_object, dataset, variable) {
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
  c(res$min_val, res$max_val)
}

# Frequency bins computed database-side for the range histogram feedback.
db_range_frequencies <- function(data_object, dataset, variable) {
  minmax <- db_range_minmax(data_object, dataset, variable)
  if (is.null(minmax)) {
    return(
      data.frame(
        level = character(0), count = numeric(0),
        l_bound = numeric(0), u_bound = numeric(0),
        stringsAsFactors = FALSE
      )
    )
  }
  min_val <- minmax[1]
  max_val <- minmax[2]
  if (min_val == max_val) {
    n <- data_object[[dataset]] %>%
      dplyr::select(!!sym(variable)) %>%
      dplyr::filter(!is.na(!!sym(variable))) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::collect() %>%
      dplyr::pull(n)
    return(
      data.frame(
        level = "1", count = as.integer(n),
        l_bound = min_val, u_bound = max_val,
        stringsAsFactors = FALSE
      )
    )
  }

  max_breaks <- getOption("cb_range_stats_max_breaks", 1000L)
  step <- 1L
  if ((max_val - min_val) / step > max_breaks) {
    step <- (max_val - min_val) / max_breaks
  }
  breaks <- seq(min_val, max_val, by = step)
  if (rev(breaks)[1] != max_val) {
    breaks[length(breaks) + 1] <- max_val
  }
  breaks <- round(breaks, 2)
  bounds <- breaks

  # Pull the (already filtered) column and bin in R. The histogram only renders
  # a bounded number of bars, so collecting the single column is acceptable.
  column <- data_object[[dataset]] %>%
    dplyr::select(!!sym(variable)) %>%
    dplyr::filter(!is.na(!!sym(variable))) %>%
    dplyr::collect() %>%
    dplyr::pull(1)

  brk <- breaks
  brk[1] <- brk[1] - 0.01
  brk[length(brk)] <- brk[length(brk)] + 0.01

  level <- factor(
    findInterval(column, brk, rightmost.closed = FALSE),
    levels = seq_along(brk),
    labels = as.character(seq_along(brk))
  )
  counts <- as.integer(table(level))
  data.frame(
    level = as.character(seq_along(brk)),
    count = counts,
    l_bound = bounds,
    u_bound = c(bounds[-1], bounds[length(bounds)]),
    stringsAsFactors = FALSE
  )
}

db_range_get_stats <- function(filter, data_object, name) {
  dataset <- filter@dataset
  variable <- filter@variable
  if (missing(name)) {
    name <- c("range", "frequencies", "n_missing", "n_data")
  }

  stats <- list(
    range = if ("range" %in% name) db_range_minmax(data_object, dataset, variable),
    frequencies = if ("frequencies" %in% name) db_range_frequencies(data_object, dataset, variable),
    n_missing = if ("n_missing" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::select(!!sym(variable)) %>%
        dplyr::filter(is.na(!!sym(variable))) %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    },
    n_data = if ("n_data" %in% name) {
      res <- data_object[[dataset]] %>%
        dplyr::select(!!sym(variable)) %>%
        dplyr::filter(!is.na(!!sym(variable))) %>%
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect()
      as.integer(res$n)
    }
  )
  if (length(name) == 1) stats[[name]] else stats[name]
}

db_range_get_defaults <- function(filter, cache_object) {
  list(
    range = c(
      cache_object$frequencies$l_bound[1],
      rev(cache_object$frequencies$u_bound)[1]
    )
  )
}

db_range_domain_from_data <- function(filter, data_object) {
  db_range_minmax(data_object, filter@dataset, filter@variable)
}

# -- CbFilterRange x db_class -------------------------------------------------

S7::method(
  cb_filter_data,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, ...) {
  db_range_filter_data(filter, data_object)
}

S7::method(
  cb_get_filter_stats,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, name, ...) {
  db_range_get_stats(filter, data_object, name)
}

S7::method(
  cb_plot_filter_data,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, ...) {
  column <- data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable)) %>%
    dplyr::filter(!is.na(!!sym(filter@variable))) %>%
    dplyr::collect() %>%
    dplyr::pull(1)
  if (length(column)) {
    graphics::hist(column, main = NULL, xlab = filter@variable)
  } else {
    graphics::barplot(0, ylim = c(0, 0.1), main = "No data")
  }
}

S7::method(
  cb_get_filter_data,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, ...) {
  data_object[[filter@dataset]] %>%
    dplyr::select(!!sym(filter@variable))
}

S7::method(
  cb_get_filter_defaults,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, cache_object, ...) {
  db_range_get_defaults(filter, cache_object)
}

S7::method(
  cb_filter_to_expr,
  list(CbFilterRange, db_class)
) <- function(filter, source, ...) {
  db_range_filter_to_expr(filter)
}

S7::method(
  cb_domain_from_data,
  list(CbFilterRange, db_class)
) <- function(filter, source, data_object, ...) {
  db_range_domain_from_data(filter, data_object)
}
