HAS_SSHD_SERVER <- FALSE
HERE <- getwd()


start_sshd_server <- function() {
  testthat::skip_on_cran()
  if (!identical(Sys.getenv("STORR_REMOTE_USE_SSHD"), "true")) {
    skip("Set 'STORR_REMOTE_USE_SSHD' to 'true' to enable sshd tests")
  }

  ## Is there an existing server running still?
  res <- system3("docker", c("inspect", "storr-remote-sshd"))
  if (res$success) {
    HAS_SSHD_SERVER <<- TRUE
    return(TRUE)
  }

  ## Can we start a new one?
  res <- system3("sshd/server.sh", check = FALSE)
  if (res$success) {
    HAS_SSHD_SERVER <<- TRUE
    return(TRUE)
  }

  FALSE
}


stop_sshd_server <- function(force = FALSE) {
  if (HAS_SSHD_SERVER || force) {
    system3("docker", c("stop", "storr-remote-sshd"))
    HAS_SSHD_SERVER <<- FALSE
  }
}


use_sshd_server <- function() {
  if (HAS_SSHD_SERVER || start_sshd_server()) {
    return(TRUE)
  }
  testthat::skip("sshd server not available")
}


system3 <- function(command, args = character(), check = FALSE) {
  res <- suppressWarnings(system2(command, args, stdout = TRUE, stderr = TRUE))
  status <- attr(res, "status")
  code <- if (is.null(status)) 0 else status
  attr(res, "status") <- NULL
  ret <- list(success = code == 0,
              code = code,
              output = res)
  if (check && !ret$success) {
    stop("Command failed: ", paste(ret$output, collapse = "\n"))
  }
  ret
}


test_ssh_connection <- function() {
  use_sshd_server()
  for (i in 1:10) {
    con <- tryCatch(
      ssh::ssh_connect("root@127.0.0.1:10022",
                       file.path(HERE, "sshd/keys/id_rsa")),
      error = function(e) NULL)
    if (inherits(con, "ssh_session")) {
      return(con)
    }
    ## server may not yet be up:
    Sys.sleep(0.1)
  }
  testthat::skip("Failed to make connection")
}


random_path <- function(prefix = "storr_remote", fileext = "") {
  basename(tempfile(prefix, fileext = fileext))
}
