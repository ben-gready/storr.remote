context("ssh_file_ops")

test_that("directory operations", {
  ## Not strictly unit tests, but integration tests of basic
  ## operations.
  root <- random_path("storr_ssh_root_")
  path <- random_path()

  ops <- ssh_file_ops(test_ssh_connection(), root)
  on.exit(ops$destroy())

  path <- random_path()
  expect_false(ops$exists(path))
  ops$create_dir(path)
  expect_true(ops$exists(path))
  expect_true(ops$exists_dir(path))
  expect_equal(ops$list_dir(path), character())

  ops$delete_file(path)
  expect_true(ops$exists_dir(path))
  ops$delete_dir(path)
  expect_false(ops$exists_dir(path))
})


test_that("single file, directoryless io", {
  root <- random_path("storr_ssh_root_")
  path <- random_path()
  bytes <- charToRaw("hello world")

  ops <- ssh_file_ops(test_ssh_connection(), root)
  on.exit(ops$destroy())

  expect_error(ops$read_bytes(path),
               "Error downloading file")
  ops$write_bytes(bytes, path)
  expect_equal(ops$list_dir("."), path)

  expect_true(ops$exists(path))
  expect_equal(ops$read_bytes(path), bytes)

  expect_true(ops$delete_file(path))
  expect_false(ops$exists(path))
  expect_false(ops$delete_file(path))
  expect_equal(ops$list_dir("."), character(0))
})


test_that("directories are created automatically", {
  root <- random_path("storr_ssh_root_")
  path <- file.path("a", "b", "c")
  bytes <- charToRaw("hello world")

  ops <- ssh_file_ops(test_ssh_connection(), root)
  on.exit(ops$destroy())

  expect_error(ops$read_bytes(path),
               "Error downloading file")
  ops$write_bytes(bytes, path)

  expect_equal(ops$list_dir("."), "a")
  expect_equal(ops$list_dir("a"), "b")
  expect_equal(ops$list_dir("a/b"), "c")

  expect_true(ops$exists(path))
  expect_equal(ops$read_bytes(path), bytes)

  expect_true(ops$delete_file(path))
  expect_false(ops$exists(path))
  expect_false(ops$delete_file(path))
  expect_equal(ops$list_dir("."), "a")
  expect_equal(ops$list_dir("a"), "b")
  expect_equal(ops$list_dir("a/b"), character(0))
})
