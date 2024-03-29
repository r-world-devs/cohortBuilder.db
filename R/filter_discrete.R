#' Discrete filter definition
#'
#' This is a method for cohortBuilder [cb_filter.discrete()] generic.
#' It creates 'discrete' filter type definition for specified table database connection.
#'
#' @export
cb_filter.discrete.db <- function(
  source, type = "discrete", id = .gen_id(), name = id, variable, value = NA,
  dataset, keep_na = TRUE, ..., description = NULL, active = TRUE) {
  args <- list(...)

  def_filter(
    type = type,
    id = id,
    name = name,
    input_param = "value",
    filter_data = function(data_object) {

      selected_value <- value # code include
      if (keep_na && !identical(selected_value, NA)) {
        # keep_na !value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter(!!sym(variable) %in% !!selected_value | is.na(!!sym(variable)))
        # keep_na !value_na end
      }
      if (!keep_na && identical(selected_value, NA)) {
        # !keep_na value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter(!is.na(!!sym(variable)))
        # !keep_na value_na end
      }
      if (!keep_na && !identical(selected_value, NA)) {
        # !keep_na !value_na start
        data_object[[dataset]] <- data_object[[dataset]] %>%
          dplyr::filter(!!sym(variable) %in% !!selected_value)
        # !keep_na !value_na end
      }
      attr(data_object[[dataset]], "filtered") <- TRUE # code include
      return(data_object)
    },
    get_stats = function(data_object, name) {
      if (missing(name)) {
        name <- c("n_data", "choices", "n_missing")
      }
      # todo sometimes we don't need stats, maybe let's distinct stats_choices from choices (or even nothing if we define choices in filter definition)
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
            dplyr::summarise(n = n()) %>%
            dplyr::collect()
          as.integer(res$n)
        },
        n_missing = if ("n_missing" %in% name) {
          res <- data_object[[dataset]] %>%
            dplyr::select(!!sym(variable)) %>%
            dplyr::filter(is.na(!!sym(variable))) %>%
            dplyr::summarise(n = n()) %>%
            dplyr::collect()
          as.integer(res$n)
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
        data_object[[dataset]][[variable]] %>% table %>% prop.table() %>% barplot()
      } else {
        barplot(0, ylim = c(0, 0.1), main = "No data")
      }
    },
    get_params = function(name) {
      params <- list(
        dataset = dataset,
        variable = variable,
        value = value,
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
      list(value = names(cache_object$choices))
    }
  )
}
