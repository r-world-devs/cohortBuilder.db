test_that("Calling db type source returns valid structure list", {
  db_source <- set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn)
  )
  expect_equal(class(db_source), c("db", "Source", "R6"))
  expect_equal(names(db_source$dtconn), c("connection", "schema", "tables"))
})

test_that("DB layer works on connections", {
  db_source <- set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn)
  )
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "author", variable = "author", dataset = "books", value = "Dan Brown")
    )
  )
  expect_true(inherits(coh$get_data(1, state = "pre", collect = FALSE)$books, "tbl_sql"))
  run(coh)
  expect_true(inherits(coh$get_data(1, state = "post", collect = FALSE)$books, "tbl_sql"))
})

test_that("Filtering works fine", {
  db_source <- set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn)
  )
  # single step
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "author", variable = "author", dataset = "books", value = "Dan Brown"),
      filter("discrete", id = "program", variable = "program", dataset = "borrowers", value = "premium")
    )
  )
  expect_equal(nrow(coh$get_data(1, state = "pre", collect = TRUE)$books), nrow(librarian$books))
  run(coh)
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$books$author), "Dan Brown")
  expect_equal(
    coh$get_data(1, state = "post", collect = FALSE)$books$ops$x$x,
    dbplyr::ident("books_1")
  )

  # multiple steps
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "author", variable = "author", dataset = "books", value = "Dan Brown")
    ),
    step(
      filter("discrete", id = "program", variable = "program", dataset = "borrowers", value = "premium")
    )
  )
  run(coh)
  expect_equal(nrow(coh$get_data(1, state = "pre", collect = TRUE)$books), nrow(librarian$books))
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$books$author), "Dan Brown")
  expect_equal(unique(coh$get_data(2, state = "post", collect = TRUE)$borrowers$program), c("premium", NA))
  expect_equal(
    coh$get_data(1, state = "post", collect = FALSE)$books$ops$x$x,
    dbplyr::ident("books_1")
  )
  expect_equal(
    coh$get_data(2, state = "post", collect = FALSE)$books$ops$x$x,
    dbplyr::ident("books_2")
  )
})

test_that("Binding works fine", {
  # single relation
  db_source <- set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn),
    binding_keys = bind_keys(
      bind_key(update = data_key("issues", "isbn"), data_key("books", "isbn"))
    )
  )
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "isbn", variable = "isbn", dataset = "books", value = "0-676-97976-9")
    )
  )
  run(coh)
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$issues$isbn), "0-676-97976-9")

  # multiple separate relations
  db_source <- set_source(
    dbtables(tables = c("books", "borrowers", "issues"), schema = "cb", connection = conn),
    binding_keys = bind_keys(
      bind_key(update = data_key("issues", "isbn"), data_key("books", "isbn")),
      bind_key(update = data_key("issues", "id"), data_key("borrowers", "id"))
    )
  )
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "isbn", variable = "isbn", dataset = "books", value = "0-676-97976-9"),
      filter("discrete", id = "id", variable = "id", dataset = "borrowers", value = "000001")
    )
  )
  run(coh)
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$issues$isbn), "0-676-97976-9")
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$issues$id), "000001")

  # multiple dependent tables
  db_source <- set_source(
    dbtables(tables = c("borrowers", "issues", "returns"), schema = "cb", connection = conn),
    binding_keys = bind_keys(
      bind_key(update = data_key("borrowers", "id"), data_key("issues", "id"), data_key("returns", "id"))
    )
  )
  coh <- cohort(
    db_source,
    step(
      filter("discrete", id = "issue_id", variable = "id", dataset = "issues", value = c("000001", "000002")),
      filter("discrete", id = "return_id", variable = "id", dataset = "returns", value = c("000001", "000003"))
    )
  )
  run(coh)
  expect_equal(unique(coh$get_data(1, state = "post", collect = TRUE)$borrowers$id), "000001")
})
