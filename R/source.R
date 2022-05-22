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
      attr(tbl_conn, "tbl_name") <- table
      # probably not needed dplyr::copy_to(tbl_conn, tbl_conn, name = paste0(table, "_tmp_0"))
      tbl_conn
    }
  )
}

tmp_table_name <- function(name, id) {
  paste0(name, "_", id)
}

#' @rdname source-layer
#' @export
.pre_filtering.db <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      table_name <- tmp_table_name(attr(data_object[[table]], "tbl_name"), step_id)
      DBI::dbRemoveTable(source$dtconn$conn, table_name, temporary = TRUE, fail_if_missing = FALSE)
      data_object[[table]] <- dplyr::compute(
        data_object[[table]],
        name = table_name
      )
      attr(data_object[[table]], "filtered") <- FALSE
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
