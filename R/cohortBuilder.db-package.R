#' Create data source cohort
#'
#' @name cohortBuilder.db-package
#' @importFrom magrittr %>%
#' @importFrom dplyr sym
#' @import cohortBuilder
#' @import shinyCohortBuilder

globalVariables(c(":=", "!!", ".data"))

NULL

`%:::%` <- function (pkg, name) {
  pkg <- as.character(substitute(pkg))
  name <- as.character(substitute(name))
  get(name, envir = asNamespace(pkg), inherits = FALSE)
}
