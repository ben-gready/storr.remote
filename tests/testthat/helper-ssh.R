HAS_SSHD_SERVER <- FALSE


start_sshd_server <- function() {
  testthat::skip_on_cran()
  if (!identical(Sys.getenv("STORR_REMOTE_USE_SSHD"), "true")) {
    skip("Set 'STORR_REMOTE_USE_SSHD' to 'true' to enable sshd tests")
  }
  res <- system3("sshd/server.sh", check = FALSE)
  if (res$success) {
    HAS_SSHD_SERVER <<- TRUE
  }
  res$success
}


stop_sshd_server <- function() {
  if (HAS_SSHD_SERVER) {
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
  code <- attr(res, "status") %||% 0
  attr(res, "status") <- NULL
  ret <- list(success = code == 0,
              code = code,
              output = res)
  if (check && !ret$success) {
    stop("Command failed: ", paste(ret$output, collapse = "\n"))
  }
  ret
}
