context("util (assert)")

test_that("assertions", {
  expect_error(assert_scalar(NULL), "must be a scalar")
  expect_error(assert_scalar(1:2), "must be a scalar")

  expect_error(assert_character(1:5), "must be character")

  expect_error(assert_is(1, "foo"), "must be a foo")

  expect_error(assert_nonmissing(NA), "must not be NA")
})
