#' Configure database tables connection
#'
#' The object should be used as `dtconn` argument of \link{set_source}.
#'
#' @param connection Database connection.
#' @param tables Vector of tables names to use in the source.
#' @param schema Name of the schema to connect to.
#'
#' @return Object of class 'db'.
#' @export
dbtables <- function(connection, tables, schema = "public") {
  structure(
    list(
      connection = connection,
      schema = schema,
      tables = tables
    ),
    class = "db"
  )
}

#' Create db-based Source
#'
#' This is a method for cohortBuilder [set_source()] generic.
#' It creates Source object which is based on connection to database tables.
#'
#' @param dtconn Object of class "db" defining connection to the database, schema and tables names.
#'     Should be created with \link{dbtables}.
#' @param primary_keys Definition of data `primary_keys`. See \link[cohortBuilder]{primary_keys}.
#' @param binding_keys Binding keys definition see \link[cohortBuilder]{binding-keys}.
#' @param source_code An expression presenting low-level code for creating source.
#'     When provided, used as a part of reproducible code output.
#' @param description A named list storing the source objects description.
#'     Can be accessed with \link{description} Cohort method.
#' @param ... Source type specific parameters. Available in `attributes` list of resulting object.
#' @export
set_source.db <- function(dtconn, primary_keys = NULL, binding_keys = NULL,
                          source_code = NULL, description = NULL, ...) {
  Source$new(
    dtconn, primary_keys = primary_keys, binding_keys = binding_keys,
    source_code = source_code, description = description,
    ...
  )
}

#' Source compatibility methods for "db" layer.
#'
#' These are methods for `cohortBuilder` generics used for integrity between source layers.
#' See \link[cohortBuilder]{source-layer}.
#'
#' @param source Source object of "db" type.
#' @param data_object Object that allows source data access.
#'   For "db" source it's a list of connections to temporary tables in connected database.
#' @param step_id Id of the currently used step.
#'
#' @name source-layer
#' @export
.init_step.db <- function(source) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      tbl_conn <- dplyr::tbl(
        source$dtconn$connection,
        dbplyr::in_schema(source$dtconn$schema, table)
      )
      tbl_conn
    }
  )
}

tmp_table_name <- function(name, suffix) {
  paste0(name, "_", suffix)
}

#' @rdname source-layer
#' @export
.pre_filtering.db <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      table_name <- tmp_table_name(table, step_id)
      DBI::dbRemoveTable(
        source$dtconn$conn, name = tmp_table_name(table, step_id),
        temporary = TRUE, fail_if_missing = FALSE
      )
      attr(data_object[[table]], "filtered") <- FALSE
      return(data_object[[table]])
    }
  )
}

#' @rdname source-layer
#' @export
.post_binding.db <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      tbl_filtered <- attr(data_object[[table]], "filtered")
      data_object[[table]] <- dplyr::compute(
        data_object[[table]],
        name = tmp_table_name(table, step_id)
      )
      attr(data_object[[table]], "filtered") <- tbl_filtered
      return(data_object[[table]])
    }
  )
}

#' @rdname source-layer
#' @export
.collect_data.db <- function(source, data_object) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    ~dplyr::collect(data_object[[.x]])
  )
}

#' @rdname source-layer
#' @export
.get_stats.db <- function(source, data_object) {
  dataset_names <- source$dtconn$tables
  dataset_names %>%
    purrr::map(
      ~ list(
        n_rows = data_object[[.x]] %>%
          dplyr::summarise(n = n()) %>%
          dplyr::collect() %>%
          dplyr::pull(n) %>%
          as.integer()
      )
    ) %>%
    stats::setNames(dataset_names)
}

#' @export
.run_binding.db <- function(source, binding_key, data_object_pre, data_object_post, ...) {
  binding_dataset <- binding_key$update$dataset
  dependent_datasets <- names(binding_key$data_keys)
  active_datasets <- data_object_post %>%
    purrr::keep(~ attr(., "filtered")) %>%
    names()

  if (!any(dependent_datasets %in% active_datasets)) {
    return(data_object_post)
  }

  key_values <- NULL
  common_key_names <- paste0("key_", seq_along(binding_key$data_keys[[1]]$key))
  for (dependent_dataset in dependent_datasets) {
    key_names <- stats::setNames(
      binding_key$data_keys[[dependent_dataset]]$key,
      common_key_names
    )
    tmp_key_values <- data_object_post[[dependent_dataset]] %>%
      dplyr::select(!!!key_names) %>%
      dplyr::distinct()
    if (is.null(key_values)) {
      key_values <- tmp_key_values
    } else {
      key_values <- dplyr::inner_join(key_values, tmp_key_values, by = common_key_names)
    }
  }

  data_object_post[[binding_dataset]] <- dplyr::inner_join(
    switch(
      as.character(binding_key$post),
      "FALSE" = data_object_pre[[binding_dataset]],
      "TRUE" = data_object_post[[binding_dataset]]
    ),
    key_values,
    by = stats::setNames(common_key_names, binding_key$update$key)
  )
  if (binding_key$activate) {
    attr(data_object_post[[binding_dataset]], "filtered") <- TRUE
  }

  return(data_object_post)
}

#' @export
.get_attrition_label.db <- `%:::%`("cohortBuilder", ".get_attrition_label.tblist")

#' @export
.get_attrition_count.db <- `%:::%`("cohortBuilder", ".get_attrition_count.tblist")
