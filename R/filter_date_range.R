#' Filters definition
#'
#' Methods used for configuration of specific filter types for `dbtables` source.
#'
#' @inheritParams cohortBuilder::cb_filter.date_range.tblist
#' @rdname filter-source-types
#' @export
cb_filter.date_range.db <- function(
  source, type = "date_range", id = .gen_id(), name = id, variable, range = NA,
  dataset, keep_na = TRUE, ..., description = NULL, active = TRUE) {
  args <- list(...)

  def_filter(
    type = type,
    id = id,
    name = name,
    input_param = "range",
    filter_data = function(data_object) {

      if (keep_na && !identical(range, NA)) {
        # keep_na !value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter((!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1]) | is.na(!!sym(variable)))
        # keep_na !value_na end
      }
      if (!keep_na && identical(range, NA)) {
        # !keep_na value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter(!is.na(!!sym(variable)))
        # !keep_na value_na end
      }
      if (!keep_na && !identical(range, NA)) {
        # !keep_na !value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter(!!sym(variable) <= !!range[2] & !!sym(variable) >= !!range[1])
        # !keep_na !value_na end
      }
      attr(data_object[[dataset]], "filtered") <- TRUE # code include
      return(data_object)
    },
    get_stats = function(data_object, name) {
      if (missing(name)) {
        name <- c("n_data", "frequencies", "n_missing")
      }

      extra_params <- list(...)
      nrows <- data_object[[dataset]] %>% # todo can we compute it once per dataset? Having 3 such filter we'll do it 3 times.
        dplyr::summarise(n = dplyr::n()) %>%
        dplyr::collect() %>%
        dplyr::pull(n) %>%
        as.integer()

      stats <- list(
        frequencies = if ("frequencies" %in% name) {
          step <- "day"
          if (nrows == 0) {
            return(
              data.frame(
                level = character(0),
                count = numeric(0),
                l_bound = numeric(0),
                u_bound = numeric(0)
              )
            )
          }

          range <- data_object[[dataset]] %>%
            dplyr::select(!!sym(variable)) %>%
            dplyr::summarise(
              min = min(!!sym(variable), na.rm = TRUE),
              max = max(!!sym(variable), na.rm = TRUE)
            ) %>%
            dplyr::collect()
          min_val <- as.Date(range$min, origin = "1970-01-01")
          max_val <- as.Date(range$max, origin = "1970-01-01")
          if (min_val == max_val) {
            return(
              data.frame(
                level = "1",
                count = nrows,
                l_bound = min_val,
                u_bound = max_val
              )
            )
          }

          if (!is.null(extra_params$step)) {
            step <- extra_params$step
          }
          breaks <- seq.Date(min_val, max_val, by = step)
          if (rev(breaks)[1] != max_val) {
            breaks[length(breaks) + 1]  <- breaks[length(breaks)] + step
          }

          per_date_counts <- data_object[[dataset]] %>%
            dplyr::select(!!sym(variable)) %>%
            dplyr::group_by(!!sym(variable)) %>%
            dplyr::summarise(
              count = dplyr::n()
            ) %>%
            dplyr::collect() %>%
            dplyr::mutate(
              count = as.integer(count),
              !!(variable) := as.Date(!!sym(variable), origin = "1970-01-01")
            )

          data.frame(
            level = breaks,
            l_bound = breaks,
            u_bound = c(breaks[-1], breaks[length(breaks)])
          ) %>%
            dplyr::left_join(per_date_counts, by = c("level" = variable)) %>%
            dplyr::mutate(
              l_bound = breaks,
              u_bound = c(breaks[-1], breaks[length(breaks)]),
              count = ifelse(is.na(count), 0, count)
            )
        },
        n_data = if ("n_data" %in% name) nrows,
        n_missing = if ("n_missing" %in% name) {
          data_object[[dataset]] %>%
            dplyr::summarise(
              n = sum(as.integer(is.na(!!sym(variable))), na.rm = TRUE)
            ) %>%
            dplyr::collect() %>%
            dplyr::pull(n) %>%
            as.integer()
        }
      )
      if (length(name) == 1) {
        return(stats[[name]])
      } else {
        return(stats)
      }
    },
    plot_data = function(data_object) {
      if (nrow(data_object[[dataset]])) {
        data_object[[dataset]][[variable]] %>% graphics::hist()
      } else {
        graphics::barplot(0, ylim = c(0, 0.1), main = "No data")
      }
    },
    get_params = function(name) {
      params <- list(
        dataset = dataset,
        variable = variable,
        range = range,
        keep_na = keep_na,
        description = description,
        active = active,
        ...
      )
      if (!missing(name)) return(params[[name]])
      return(params)
    },
    get_data = function(data_object) {
      data_object[[dataset]][[variable]]
    },
    get_defaults = function(data_object, cache_object) {
      list(
        range = c(
          cache_object$frequencies$l_bound[1],
          rev(cache_object$frequencies$u_bound[1])
        )
      )
    }
  )
}
