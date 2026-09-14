#' Step execution strategy
#'
#' Controls how each filtering step's result is handed to the next step, that is
#' what \link{.pre_filtering.db} and \link{.post_binding.db} do.
#'
#' The strategy is read from the \code{cohortBuilder.db.step_strategy} option on
#' every step run, so it can be changed at any time with
#' \code{options(cohortBuilder.db.step_strategy = "lazy")}.
#'
#' Available strategies:
#'
#' \describe{
#'   \item{"lazy" (default)}{No materialization at all. \code{.post_binding}
#'     returns the lazy \code{dbplyr} query unchanged and \code{.pre_filtering}
#'     only resets the per-table "filtered" flag, so no temporary tables are ever
#'     created or dropped. Each downstream collect or count is pushed down to the
#'     database as a single query. Best when the cohort is shallow (few steps) or
#'     the source tables are too large to copy; the trade-off is deeper query
#'     nesting as filters and bindings accumulate. This is also the only strategy
#'     that works on databases without a \code{pg_temp} schema (e.g. Amazon
#'     Redshift), because nothing is dropped.}
#'   \item{"materialize"}{The original behaviour. \code{.post_binding} writes
#'     every source table's result to a temporary table, so later steps read a
#'     materialized result instead of re-deriving it; \code{.pre_filtering} drops
#'     this step's temporary tables first via \link[DBI]{dbRemoveTable} so a
#'     re-run can recreate them. The trade-off is a full table scan per source
#'     table on every run, even for tables no filter touched. Note that
#'     \code{dbRemoveTable(temporary = TRUE)} is emitted by \code{RPostgres} as
#'     \code{DROP TABLE pg_temp.<name>}, which fails on Redshift.}
#'   \item{"materialize_selective"}{Like "materialize", but only tables whose row
#'     set actually changed in this step (a filter or an active binding touched
#'     them) are copied to a temporary table. Untouched tables are passed through
#'     unchanged, which points them at the previous step's data object. Temporary
#'     tables are dropped with a plain \code{DROP TABLE IF EXISTS}, so this
#'     strategy also works on Redshift.}
#' }
#'
#' An unset or unrecognised option falls back to "lazy" (with a warning in the
#' latter case).
#'
#' @name step-strategy
NULL

step_strategies <- c("lazy", "materialize", "materialize_selective")

#' Read and validate the configured step strategy.
#'
#' @return One of `step_strategies`.
#' @noRd
step_strategy <- function() {
  strategy <- getOption("cohortBuilder.db.step_strategy", "lazy")
  if (!is.character(strategy) || length(strategy) != 1L || !strategy %in% step_strategies) {
    warning(
      "Invalid `cohortBuilder.db.step_strategy` option; falling back to \"lazy\". ",
      "Valid values: ", paste0("\"", step_strategies, "\"", collapse = ", "), ".",
      call. = FALSE
    )
    strategy <- "lazy"
  }
  strategy
}

# Temporary table name used by the "materialize" strategy.
tmp_table_name <- function(name, suffix) {
  paste0(name, "_", suffix)
}

# Temporary table name used by the "materialize_selective" strategy. That
# strategy issues an unqualified `DROP TABLE IF EXISTS`, which resolves through
# the search path, so the distinctive prefix keeps it from ever naming a real
# table.
selective_tmp_table_name <- function(name, step_id) {
  paste0("cb_tmp_", name, "_", step_id)
}

# --- pre-filtering implementations -------------------------------------------

# Reset the per-table "filtered" flag and pass the lazy tables through. Nothing
# was materialized, so there is nothing to drop.
pre_filtering_lazy <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      attr(data_object[[table]], "filtered") <- FALSE
      data_object[[table]]
    }
  )
}

# Drop this step's temporary tables so `.post_binding` can recreate them on a
# re-run, then reset the "filtered" flag.
pre_filtering_materialize <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      DBI::dbRemoveTable(
        source$dtconn$connection, name = tmp_table_name(table, step_id),
        temporary = TRUE, fail_if_missing = FALSE
      )
      attr(data_object[[table]], "filtered") <- FALSE
      data_object[[table]]
    }
  )
}

# As above, but with a plain unqualified `DROP TABLE IF EXISTS` instead of
# `DBI::dbRemoveTable(temporary = TRUE)`, so no `pg_temp` schema is referenced.
# The drop is unconditional (not limited to tables materialized in this run) to
# clear anything left over by an earlier run.
pre_filtering_materialize_selective <- function(source, data_object, step_id) {
  connection <- source$dtconn$connection
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      table_name <- DBI::dbQuoteIdentifier(
        connection, selective_tmp_table_name(table, step_id)
      )
      DBI::dbExecute(connection, paste0("DROP TABLE IF EXISTS ", table_name))
      attr(data_object[[table]], "filtered") <- FALSE
      data_object[[table]]
    }
  )
}

# --- post-binding implementations --------------------------------------------

# Keep the filtered and bound result lazy.
post_binding_lazy <- function(source, data_object, step_id) {
  data_object
}

# Materialize every source table into a temporary table.
post_binding_materialize <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      tbl_filtered <- attr(data_object[[table]], "filtered")
      data_object[[table]] <- dplyr::compute(
        data_object[[table]],
        name = tmp_table_name(table, step_id)
      )
      attr(data_object[[table]], "filtered") <- tbl_filtered
      data_object[[table]]
    }
  )
}

# Materialize only the tables whose row set changed in this step, i.e. those a
# filter or an active binding touched (the "filtered" attribute, set by
# `cb_filter_data()` and `.run_binding()`). A table that was not touched is
# returned unchanged, and the object handed to `.post_binding` for such a table
# already is the previous step's data object, so pointing at it costs nothing.
# Bound tables must be materialized rather than reverted to the previous step,
# otherwise the binding join would be dropped.
post_binding_materialize_selective <- function(source, data_object, step_id) {
  purrr::map(
    stats::setNames(source$dtconn$tables, source$dtconn$tables),
    function(table) {
      if (!isTRUE(attr(data_object[[table]], "filtered"))) {
        return(data_object[[table]])
      }
      computed <- dplyr::compute(
        data_object[[table]],
        name = selective_tmp_table_name(table, step_id),
        temporary = TRUE
      )
      attr(computed, "filtered") <- TRUE
      computed
    }
  )
}
