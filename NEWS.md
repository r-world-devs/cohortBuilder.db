# cohortBuilder.db 1.0.0

* First CRAN release.
* Added configurable step strategies controlling how a step result is handed to
  the next step, selected with the `cohortBuilder.db.step_strategy` option
  (`?"step-strategy"`):
  * `"lazy"` (new default) keeps the result a lazy `dbplyr` query and never
    writes temporary tables,
  * `"materialize"` is the previous behaviour, computing every source table into
    a temporary table on each step,
  * `"materialize_selective"` computes only the tables a filter or an active
    binding touched, and drops temporary tables without referencing the
    `pg_temp` schema, which makes it usable on Amazon Redshift.
* Added the `vignette("step-strategy")` describing when to use each strategy.
* Adjusted the filter and source layers to `cohortBuilder` and
  `shinyCohortBuilder` 1.0.0, which moved filter methods to S7 dual dispatch.
* Fixed range filter break computation.

# cohortBuilder.db 0.2.0

* Added `shinyCohortBuilder` methods, so db sources can be used in the GUI.
* Added source-layer methods for binding keys, attrition and reproducible code.

# cohortBuilder.db 0.1.0

* Initial version providing `dbtables()` and the `db` source layer.
