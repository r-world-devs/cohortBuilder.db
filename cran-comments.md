## Submission

This is a new submission. `cohortBuilder.db` is a source layer extension for the
CRAN packages `cohortBuilder` and `shinyCohortBuilder`, adding support for
filtering data stored in a database.

## R CMD check results

0 errors | 0 warnings | 0 notes

Checked with `R CMD check --as-cran` on Ubuntu 24.04, R 4.5.1. The only
diagnostics reported locally are about the check machine missing `qpdf` and
`tidy`, not about the package itself.

## Notes

* Examples that need a live database connection are wrapped in `\dontrun{}`.
  All other examples, the tests and the vignette run against an in-memory
  `RSQLite` database and create no files outside the session temporary
  directory.
