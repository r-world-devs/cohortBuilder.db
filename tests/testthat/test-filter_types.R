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
