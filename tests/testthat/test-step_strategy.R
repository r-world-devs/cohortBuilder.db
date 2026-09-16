strategy_source <- function() {
  set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn),
    binding_keys = bind_keys(
      bind_key(update = data_key("issues", "isbn"), data_key("books", "isbn"))
    )
  )
}

strategy_cohort <- function() {
  cohort(
    strategy_source(),
    step(
      filter("discrete", id = "author", variable = "author", dataset = "books", value = "Dan Brown")
    ),
    step(
      filter("discrete", id = "program", variable = "program", dataset = "borrowers", value = "premium")
    )
  )
}

remote_names <- function(data_object) {
  vapply(
    data_object,
    function(x) {
      name <- dbplyr::remote_name(x)
      if (is.null(name)) NA_character_ else as.character(name)
    },
    character(1)
  )
}

test_that("Strategy option is validated", {
  withr::local_options(cohortBuilder.db.step_strategy = NULL)
  expect_equal(step_strategy(), "lazy")

  for (strategy in step_strategies) {
    withr::local_options(cohortBuilder.db.step_strategy = strategy)
    expect_equal(step_strategy(), strategy)
  }

  withr::local_options(cohortBuilder.db.step_strategy = "nonexistent")
  expect_warning(expect_equal(step_strategy(), "lazy"), "Invalid")

  withr::local_options(cohortBuilder.db.step_strategy = c("lazy", "materialize"))
  expect_warning(expect_equal(step_strategy(), "lazy"), "Invalid")
})

test_that("Lazy strategy materializes nothing", {
  withr::local_options(cohortBuilder.db.step_strategy = "lazy")
  coh <- strategy_cohort()
  run(coh)

  expect_true(all(is.na(remote_names(coh$get_data(2, state = "post", collect = FALSE)))))
  expect_equal(unique(coh$get_data(2, state = "post", collect = TRUE)$books$author), "Dan Brown")
})

test_that("Materialize strategy computes every table in every step", {
  withr::local_options(cohortBuilder.db.step_strategy = "materialize")
  coh <- strategy_cohort()
  run(coh)

  expect_equal(
    remote_names(coh$get_data(1, state = "post", collect = FALSE)),
    c(books = "books_1", borrowers = "borrowers_1", issues = "issues_1")
  )
  expect_equal(
    remote_names(coh$get_data(2, state = "post", collect = FALSE)),
    c(books = "books_2", borrowers = "borrowers_2", issues = "issues_2")
  )
  expect_equal(unique(coh$get_data(2, state = "post", collect = TRUE)$books$author), "Dan Brown")

  # re-running drops and recreates the temporary tables
  expect_no_error(run(coh))
})

test_that("Selective strategy computes only the tables a step touched", {
  withr::local_options(cohortBuilder.db.step_strategy = "materialize_selective")
  coh <- strategy_cohort()
  run(coh)

  # `issues` is materialized too, as the binding key marks it as filtered;
  # `borrowers` is untouched, so it still points at the source table
  expect_equal(
    remote_names(coh$get_data(1, state = "post", collect = FALSE)),
    c(books = "cb_tmp_books_1", borrowers = "borrowers", issues = "cb_tmp_issues_1")
  )
  # step 2 only filters `borrowers`, the rest points at step 1 results
  expect_equal(
    remote_names(coh$get_data(2, state = "post", collect = FALSE)),
    c(books = "cb_tmp_books_1", borrowers = "cb_tmp_borrowers_2", issues = "cb_tmp_issues_1")
  )

  expect_no_error(run(coh))
})

test_that("All strategies return the same data", {
  collected <- lapply(
    stats::setNames(step_strategies, step_strategies),
    function(strategy) {
      withr::local_options(cohortBuilder.db.step_strategy = strategy)
      coh <- strategy_cohort()
      run(coh)
      lapply(coh$get_data(2, state = "post", collect = TRUE), function(x) x[order(x[[1]]), ])
    }
  )

  expect_equal(collected$lazy, collected$materialize)
  expect_equal(collected$lazy, collected$materialize_selective)
})
