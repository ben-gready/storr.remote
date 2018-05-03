ssh_file_ops <- function(session, root) {
  R6_ssh_file_ops$new(session, root)
}


## So the issues here are
##
## 1. listing files is annoying and the test-for-existance bit needs
## not doing
##
## 2. tunelling something over ssh will be better!
##
## 3. docs for throw/not throw
##
## we can vectorise most of the read/write/exists operations but that
## is going to require a little support.  Perhaps the simplest
## approach would be to set up a directory of scripts on the remote
## machine so that we don't have to communicate only with exit codes
## but could work with json directly?
##
## This assumes that we are "in control" of a directory.  I'm going to
## assume that nothing else writes to this directory ever - anything
## that does could cause this to fail in unexpected ways!
R6_ssh_file_ops <- R6::R6Class(
  "ssh_file_ops",

  public = list(
    session = NULL,
    root = NULL,

    initialize = function(session, root) {
      assert_is(session, "ssh_session")
      assert_scalar_character(root)
      self$session <- session
      self$root <- root
    },

    type = function() {
      "ssh"
    },

    ## The two use cases that we can do easily are:
    ##
    ## upload arbitrary bytes to an arbitrary filename
    ## upload a file from a path here to an identical path there

    ## local file to remote file
    write_file = function(file, dest_dir) {
      if (dest_dir == ".") {
        path_remote <- self$root
      } else {
        path_remote <- file.path(self$root, dest_dir)
        self$create_dir(dest_dir)
      }
      ssh::scp_upload(self$session, file, path_remote, verbose = FALSE)
      invisible(file.path(dest_dir, basename(file)))
    },

    write_bytes = function(bytes, dest_file) {
      tmp <- tempfile()
      dir.create(tmp)
      on.exit(unlink(tmp, recursive = TRUE))
      path_local <- file.path(tmp, basename(dest_file))
      writeBin(bytes, path_local)
      self$write_file(path_local, dirname(dest_file))
    },

    read_file = function(file, dest) {
      file_remote <- file.path(self$root, file)
      ## TODO: This really should be an error on failure in the ssh
      ## package imo (or have the option to convert to one) - the file
      ## does not exist!  I am promoting this to error here, but there
      ## may be other warnings that this lets through.
      tmp <- tempfile()
      dir.create(tmp)
      on.exit(unlink(tmp, recursive = TRUE))
      file_local <- file.path(tmp, basename(file))

      res <- tryCatch(
        ssh::scp_download(self$session, file_remote, tmp, verbose = FALSE),
        warning = identity)
      if (!file.exists(file_local)) {
        stop("Error downloading file: ", res$message)
      }
      if (is.null(dest)) {
        read_binary(file_local)
      } else {
        dir.create(dirname(dest), FALSE, TRUE)
        file.copy(file_local, dest)
        dest
      }
    },

    read_bytes = function(file) {
      self$read_file(file, NULL)
    },

    ## TODO: not vectorised
    exists = function(path, type = c("any", "file", "directory")) {
      if (length(path) != 1L) {
        return(vapply(path, self$exists, logical(1), type,
                      USE.NAMES = FALSE))
      }
      flag <- c(file = "f", directory = "d", any = "e")[[match.arg(type)]]
      path_remote <- file.path(self$root, path)
      cmd <- sprintf("test -%s %s", flag, shQuote(path_remote))
      ssh::ssh_exec_internal(self$session, cmd, error = FALSE)$status == 0L
    },

    create_dir = function(path) {
      path_remote <- file.path(self$root, path)
      ssh::ssh_exec_wait(self$session,
                         sprintf("mkdir -p %s", shQuote(path_remote)))
    },

    list_dir = function(path) {
      path_remote <- file.path(self$root, path)
      res <- ssh::ssh_exec_internal(self$session,
                                    sprintf("ls -1 %s", shQuote(path_remote)))
      strsplit(rawToChar(res$stdout), "\n", fixed = TRUE)[[1L]]
    },

    delete_file = function(path) {
      ## NOTE: this should be farmed out to a set of scripts that we
      ## move into the remote storr, then we can do this in one single
      ## remote call.  A bash script can cake a path as args and
      ## return a vector of 0/1?  Path length issues will be a problem
      ## and we might need to use a temporary file?
      exists <- self$exists(path)
      for (p in path[exists]) {
        path_remote <- file.path(self$root, path)
        res <- ssh::ssh_exec_internal(
          self$session,
          sprintf("rm -f %s", shQuote(path_remote)),
          error = FALSE)
      }
      exists
    },

    delete_directory = function(path) {
      path_remote <- file.path(self$root, path)
      ssh::ssh_exec_wait(self$session,
                         sprintf("rm -rf %s", shQuote(path_remote)))
    },

    destroy = function() {
      ssh::ssh_exec_wait(self$session,
                         sprintf("rm -rf %s", shQuote(self$root)))
    }
  ))
