# The db filter methods are expected to mirror the built-in `tblist` ones, so
# most assertions compare both source layers on the same data.

db_events_source <- function() {
  set_source(dbtables(tables = "events", schema = "cb", connection = conn))
}

tblist_events_source <- function() {
  set_source(as.tblist(list(events = events)))
}

# Run the same step on both source layers and return the filtered `isbn`s.
filtered_isbn <- function(flt) {
  result <- function(source) {
    coh <- cohort(source, step(flt))
    run(coh)
    sort(coh$get_data(1, state = "post", collect = TRUE)$events$isbn)
  }
  list(
    db = result(db_events_source()),
    tblist = result(tblist_events_source())
  )
}

test_that("multi_discrete filters db data the same way as tblist", {
  both <- filtered_isbn(
    filter(
      "multi_discrete",
      id = "md", dataset = "events",
      variables = c("author", "genre"),
      values = list(author = c("Dan Brown", "Ian McEwan")),
      keep_na = FALSE
    )
  )
  expect_equal(both$db, both$tblist)
  expect_length(both$db, 3)
})

test_that("multi_discrete constrains every listed variable", {
  both <- filtered_isbn(
    filter(
      "multi_discrete",
      id = "md", dataset = "events",
      variables = c("author", "genre"),
      values = list(
        author = "Dan Brown",
        genre = "Crime, Thriller & Adventure"
      ),
      keep_na = FALSE
    )
  )
  expect_equal(both$db, both$tblist)
  expect_length(both$db, 1)
})

test_that("multi_discrete respects keep_na", {
  args <- list(
    "multi_discrete",
    id = "md", dataset = "events",
    variables = "genre",
    values = list(genre = "Popular Science")
  )
  kept <- filtered_isbn(do.call(filter, c(args, keep_na = TRUE)))
  dropped <- filtered_isbn(do.call(filter, c(args, keep_na = FALSE)))

  expect_equal(kept$db, kept$tblist)
  expect_equal(dropped$db, dropped$tblist)
  # `events$genre[1]` is NA, so keeping missings adds exactly one row.
  expect_length(dropped$db, 1)
  expect_length(kept$db, 2)
})

test_that("multi_discrete returns no rows when nothing is selected", {
  coh <- cohort(
    db_events_source(),
    step(
      filter(
        "multi_discrete",
        id = "md", dataset = "events",
        variables = "genre", values = list(genre = NULL),
        keep_na = FALSE
      )
    )
  )
  run(coh)
  expect_equal(nrow(coh$get_data(1, state = "post", collect = TRUE)$events), 0)
})

test_that("multi_discrete statistics match the tblist ones", {
  flt <- filter(
    "multi_discrete",
    id = "md", dataset = "events",
    variables = c("author", "genre"), values = list(author = "Dan Brown")
  )
  db_coh <- cohort(db_events_source(), step(flt))
  tblist_coh <- cohort(tblist_events_source(), step(flt))
  run(db_coh)
  run(tblist_coh)

  db_stats <- db_coh$get_stats(1, "md", state = "pre")
  tblist_stats <- tblist_coh$get_stats(1, "md", state = "pre")

  expect_equal(db_stats$n_data, tblist_stats$n_data)
  expect_equal(db_stats$choices, tblist_stats$choices)
  expect_equal(
    db_stats$n_missing,
    purrr::map(tblist_stats$n_missing, as.integer)
  )
})

test_that("multi_discrete domain is read from the data", {
  source <- db_events_source()
  flt <- filter(
    "multi_discrete",
    id = "md", dataset = "events",
    variables = "genre", values = list(genre = "Popular Science")
  )
  domain <- cb_domain_from_data(flt, source, create_data_object(source))
  expect_named(domain, "genre")
  expect_setequal(domain$genre, stats::na.omit(unique(events$genre)))
})

test_that("datetime_range filters db data the same way as tblist", {
  both <- filtered_isbn(
    filter(
      "datetime_range",
      id = "dt", dataset = "events", variable = "stamp",
      range = c(
        as.POSIXct("2020-01-01 03:00:00", tz = "UTC"),
        as.POSIXct("2020-01-01 10:00:00", tz = "UTC")
      ),
      keep_na = FALSE
    )
  )
  expect_equal(both$db, both$tblist)
  expect_length(both$db, 8)
})

test_that("datetime_range respects keep_na", {
  args <- list(
    "datetime_range",
    id = "dt", dataset = "events", variable = "stamp",
    range = c(
      as.POSIXct("2020-01-01 03:00:00", tz = "UTC"),
      as.POSIXct("2020-01-01 10:00:00", tz = "UTC")
    )
  )
  kept <- filtered_isbn(do.call(filter, c(args, keep_na = TRUE)))
  dropped <- filtered_isbn(do.call(filter, c(args, keep_na = FALSE)))

  expect_equal(kept$db, kept$tblist)
  expect_equal(dropped$db, dropped$tblist)
  # `events$stamp[2]` is NA.
  expect_length(kept$db, length(dropped$db) + 1)
})

test_that("datetime_range statistics match the tblist ones", {
  flt <- filter("datetime_range", id = "dt", dataset = "events", variable = "stamp")
  db_coh <- cohort(db_events_source(), step(flt))
  tblist_coh <- cohort(tblist_events_source(), step(flt))
  run(db_coh)
  run(tblist_coh)

  db_stats <- db_coh$get_stats(1, "dt", state = "pre")
  tblist_stats <- tblist_coh$get_stats(1, "dt", state = "pre")

  # Datetime statistics are reported as numeric seconds by both layers.
  expect_type(db_stats$min, "double")
  expect_equal(db_stats$min, tblist_stats$min)
  expect_equal(db_stats$max, tblist_stats$max)
  expect_equal(db_stats$n_data, tblist_stats$n_data)
  expect_equal(db_stats$n_missing, as.integer(tblist_stats$n_missing))
  expect_equal(
    db_stats$frequencies$count,
    as.integer(tblist_stats$frequencies$count)
  )
  expect_equal(db_stats$frequencies$l_bound, tblist_stats$frequencies$l_bound)
  expect_equal(db_stats$frequencies$u_bound, tblist_stats$frequencies$u_bound)
})

test_that("datetime_range domain is POSIXct read from the data", {
  source <- db_events_source()
  flt <- filter("datetime_range", id = "dt", dataset = "events", variable = "stamp")
  domain <- cb_domain_from_data(flt, source, create_data_object(source))
  expect_s3_class(domain, "POSIXct")
  expect_equal(as.numeric(domain), as.numeric(range(events$stamp, na.rm = TRUE)))
})

test_that("query filters db data the same way as tblist", {
  # `cohort()` attaches the filter GUI, which needs `shinyQueryBuilder`.
  skip_if_not_installed("shinyQueryBuilder")
  both <- filtered_isbn(
    filter(
      "query",
      id = "q", dataset = "events",
      variables = c("author", "genre", "copies"),
      value = queryBuilder::queryGroup(
        condition = "AND",
        queryBuilder::queryRule("copies", "between", c(2, 12)),
        queryBuilder::queryGroup(
          condition = "OR",
          queryBuilder::queryRule("author", "in", c("Dan Brown", "Ian McEwan")),
          queryBuilder::queryRule("genre", "equal", "Popular Science")
        )
      ),
      keep_na = FALSE
    )
  )
  expect_equal(both$db, both$tblist)
  expect_gt(length(both$db), 0)
})

test_that("query respects keep_na", {
  skip_if_not_installed("shinyQueryBuilder")
  args <- list(
    "query",
    id = "q", dataset = "events", variables = "genre",
    value = queryBuilder::queryGroup(
      queryBuilder::queryRule("genre", "equal", "Popular Science")
    )
  )
  kept <- filtered_isbn(do.call(filter, c(args, keep_na = TRUE)))
  dropped <- filtered_isbn(do.call(filter, c(args, keep_na = FALSE)))

  expect_equal(kept$db, kept$tblist)
  expect_equal(dropped$db, dropped$tblist)
  # `events$genre[1]` is NA, so keeping missings adds exactly one row.
  expect_equal(length(kept$db), length(dropped$db) + 1)
})

test_that("query with no value keeps all the rows", {
  skip_if_not_installed("shinyQueryBuilder")
  both <- filtered_isbn(
    filter("query", id = "q", dataset = "events", variables = "genre")
  )
  expect_equal(both$db, both$tblist)
  expect_length(both$db, nrow(events))
})

test_that("query string operators are translated to SQL LIKE", {
  skip_if_not_installed("shinyQueryBuilder")
  # `LIKE` is used instead of `grepl()`/`startsWith()`, which dbplyr cannot
  # translate. Its case sensitivity is backend defined, so the result is not
  # compared against the tblist one.
  isbns <- function(operator, value) {
    coh <- cohort(
      db_events_source(),
      step(
        filter(
          "query",
          id = "q", dataset = "events", variables = "title",
          value = queryBuilder::queryGroup(
            queryBuilder::queryRule("title", operator, value)
          ),
          keep_na = FALSE
        )
      )
    )
    run(coh)
    coh$get_data(1, state = "post", collect = TRUE)$events$isbn
  }

  expect_setequal(
    isbns("contains", "Da Vinci"),
    events$isbn[grepl("Da Vinci", events$title)]
  )
  expect_setequal(
    isbns("begins_with", "Atonement"),
    events$isbn[startsWith(events$title, "Atonement")]
  )
})

test_that("query statistics match the tblist ones", {
  skip_if_not_installed("shinyQueryBuilder")
  flt <- filter(
    "query",
    id = "q", dataset = "events", variables = c("author", "genre", "copies")
  )
  db_coh <- cohort(db_events_source(), step(flt))
  tblist_coh <- cohort(tblist_events_source(), step(flt))
  run(db_coh)
  run(tblist_coh)

  db_stats <- db_coh$get_stats(1, "q", state = "pre")
  tblist_stats <- tblist_coh$get_stats(1, "q", state = "pre")

  expect_equal(db_stats$n_data, tblist_stats$n_data)
  expect_equal(
    db_stats$n_missing,
    purrr::map(tblist_stats$n_missing, as.integer)
  )
  expect_named(db_stats$specs, c("author", "genre", "copies"))
  expect_equal(db_stats$specs$copies, tblist_stats$specs$copies)
  expect_setequal(db_stats$specs$author$values, tblist_stats$specs$author$values)
})

test_that("query specifications respect the value limit", {
  flt <- filter("query", id = "q", dataset = "events", variables = "author")
  source <- db_events_source()
  data_object <- create_data_object(source)

  expect_warning(
    specs <- withr::with_options(
      list(cb_db_query_max_values = 2),
      cb_get_filter_stats(flt, source, data_object, "specs")
    ),
    "cb_db_query_max_values"
  )
  expect_length(specs$author$values, 2)
})

test_that("reproducible code is generated for the new filter types", {
  coh <- cohort(
    db_events_source(),
    step(
      filter(
        "multi_discrete",
        id = "md", dataset = "events", variables = "author",
        values = list(author = "Dan Brown"), keep_na = FALSE
      ),
      filter(
        "datetime_range",
        id = "dt", dataset = "events", variable = "stamp",
        range = c(
          as.POSIXct("2020-01-01 03:00:00", tz = "UTC"),
          as.POSIXct("2020-01-01 10:00:00", tz = "UTC")
        ),
        keep_na = FALSE
      )
    )
  )
  run(coh)
  # `code()` prints the reproducible script as a side effect.
  repro <- paste(utils::capture.output(code(coh)), collapse = "\n")

  expect_match(repro, 'author %in% "Dan Brown"', fixed = TRUE)
  # Datetime bounds are sent to the database as numbers, not as string literals.
  expect_match(repro, "stamp <= 1577872800", fixed = TRUE)
})

test_that("domains are propagated between steps like for tblist", {
  narrowed_domain <- function(source) {
    coh <- cohort(
      source,
      step(
        filter(
          "discrete",
          id = "genre", dataset = "events", variable = "genre",
          value = c("General & Literary Fiction", "Popular Science")
        )
      ),
      step(filter("discrete", id = "genre", dataset = "events", variable = "genre")),
      propagate_domains = "data"
    )
    coh$set_domain(2, "genre", sort(unique(stats::na.omit(events$genre))))
    run(coh)
    sort(coh$get_step(2)$filters$genre@domain)
  }

  db_domain <- narrowed_domain(db_events_source())
  expect_equal(db_domain, narrowed_domain(tblist_events_source()))
  expect_equal(db_domain, c("General & Literary Fiction", "Popular Science"))
})
