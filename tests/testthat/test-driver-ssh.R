context("rds over ssh")

test_that("storr spec", {
  create <- function(dr = NULL, ...) {
    if (is.null(dr)) {
      ops <- ssh_file_ops(test_ssh_connection(), tempfile())
      rds <- storr::driver_rds(tempfile(), ...)
      driver_remote(ops, rds)
    } else {
      driver_remote(dr$ops, dr$rds)
    }
  }

  res <- storr::test_driver(create)
  expect_equal(sum(res$failed), 0)
})
