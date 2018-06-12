context("rds over ssh")

test_that("storr spec", {
  create <- function(dr = NULL, ...) {
    if (is.null(dr)) {
      ops <- ssh_file_ops(test_ssh_connection(), random_path("storr_driver_"))
    } else {
      ops <- dr$ops
    }
    storr::driver_remote(ops, ...)
  }

  res <- storr::test_driver(create)
  expect_equal(sum(res$failed), 0)
})


test_that("user interface", {
  st <- storr_ssh(test_ssh_connection(), random_path("storr_remote_"))
  expect_is(st, "storr")
  expect_is(st$driver, "driver_remote")
  expect_is(st$driver$ops, "ssh_file_ops")
  expect_equal(st$driver$type(), "remote/ssh")
  expect_equal(st$driver$ops$type(), "ssh")
})


test_that("local cache works", {
  st <- storr_ssh(test_ssh_connection(), random_path("storr_remote_"))
  value <- 1:10
  h <- st$set("a", value)
  expect_true(st$driver$rds$del_object(h))
  expect_identical(st$get("a", use_cache = FALSE), value)
  expect_equal(st$list_hashes(), h)
  expect_equal(st$driver$rds$list_hashes(), h)
})
