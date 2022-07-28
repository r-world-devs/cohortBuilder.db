#' @inheritParams cohortBuilder::cb_filter.range.tblist
#' @rdname filter-source-types
#' @export
cb_filter.range.db <- function(
  source, type = "range", id = .gen_id(), name = id, variable, range = NA, dataset,
  keep_na = TRUE, ..., description = NULL, active = TRUE) {
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
      nrows <- data_object[[dataset]] %>%
        dplyr::summarise(n = n()) %>%
        dplyr::collect() %>%
        dplyr::pull(n) %>%
        as.integer()

      stats <- list(
        frequencies = if ("frequencies" %in% name) {
          step <- 1
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
          min_val <- as.numeric(range$min)
          max_val <- as.numeric(range$max)
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
          breaks <- seq(min_val, max_val, by = step)
          if (rev(breaks)[1] != max_val) {
            breaks[length(breaks) + 1]  <- breaks[length(breaks)] + step
          }
          breaks <- round(breaks, 2)
          bounds <- breaks
          breaks[1] <- breaks[1] - 0.01
          breaks[length(breaks)] <- breaks[length(breaks)] + 0.01

          data_object[[dataset]] %>%
            dplyr::select(!!sym(variable)) %>%
            dplyr::mutate(
              tmp_level = floor((!!sym(variable) - !!min(breaks)) / (!!breaks[2] - !!breaks[1])) + 1,
              level = ifelse(!!sym(variable) == !!max_val, !!length(breaks), tmp_level)
            ) %>%
            dplyr::group_by(level) %>% # todo group by is slow
            dplyr::summarise(
              count = dplyr::n()
            ) %>%
            dplyr::collect() %>%
            dplyr::mutate(
              level = factor(
                level,
                levels = 1:(length(breaks)),
                labels = as.character(1:(length(breaks)))
              ),
              count = as.integer(count)
            ) %>%
            tidyr::complete(level, fill = list(count = 0)) %>%
            dplyr::arrange(level) %>%
            dplyr::mutate(
              l_bound = bounds,
              u_bound = c(bounds[-1], bounds[length(bounds)])
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
        data_object[[dataset]][[variable]] %>% hist()
      } else {
        barplot(0, ylim = c(0, 0.1), main = "No data")
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
