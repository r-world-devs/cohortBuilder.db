#' cohortBuilder methods for database source
#'
#' Adds a `dbtables` source backend to `cohortBuilder`, allowing filtering
#' pipelines to run directly against database tables (via `dbplyr`).
#'
#' The package plugs into the `cohortBuilder` S7 filter generics
#' (`cb_filter_data()`, `cb_get_filter_stats()`, ...) and the S3 source-layer
#' generics (`.init_step()`, `.pre_filtering()`, ...) to describe how each
#' filter type and source operation behaves for a `"db"` source.
#'
#' The following filter types are supported: `discrete`, `discrete_text`,
#' `range`, `date_range`, `datetime_range`, `multi_discrete` and `query`.
#'
#' @section Options:
#' - `cb_db_query_max_values` - maximum number of distinct values collected per
#'   column when computing `query` filter specifications (500 by default), see
#'   [db-filter-query].
#'
#' @name cohortBuilder.db-package
#' @importFrom magrittr %>%
#' @importFrom dplyr sym
#' @import cohortBuilder
#' @import shinyCohortBuilder
"_PACKAGE"

utils::globalVariables(c(
  ":=", "!!", ".data", "name", "id",
  "dataset", "n", "count", "tmp_level", "level"
))

#' S7 class wrapper for the `db` source
#'
#' S7 representation of the `db` source class. Used as the source side of the
#' `cohortBuilder` S7 dual-dispatch filter generics when registering methods,
#' e.g. `S7::method(cb_filter_data, list(CbFilterDiscrete, db_class))`.
#'
#' @format An S7 S3-class wrapper created with [S7::new_S3_class()].
#' @keywords internal
#' @export
db_class <- S7::new_S3_class("db")

.onLoad <- function(libname, pkgname) {
  # Register the S7 methods defined in this package (filter x db_class).
  S7::methods_register()
}
