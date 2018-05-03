context("rds over ssh")

test_that("storr spec", {
  create <- function(dr = NULL, ...) {
    if (is.null(dr)) {
      ops <- ssh_file_ops(test_ssh_connection(), random_path("storr_driver_"))
      driver_remote(ops, ...)
    } else {
      driver_remote(dr$ops, ...)
    }
  }

  res <- storr::test_driver(create)
  expect_equal(sum(res$failed), 0)
})
