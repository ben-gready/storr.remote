##' ssh backend for storr
##'
##' @title ssh storr
##'
##' @param session A ssh session object (see \code{ssh::ssh_connect})
##' @inheritParams storr_s3
##' @export
storr_ssh <- function(session, remote_root, ..., path_local = NULL,
                      default_namespace = "default_namespace") {
  dr <- driver_ssh(session, remote_root, ..., path_local = path_local)
  storr::storr(dr, default_namespace)
}


##' @export
##' @rdname storr_ssh
driver_ssh <- function(session, remote_root, ..., path_local = NULL) {
  ops <- ssh_file_ops(session, remote_root)
  storr::driver_remote(ops, ..., path_local = path_local)
}


ssh_file_ops <- function(session, root) {
  R6_ssh_file_ops$new(session, root)
}


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

    ## A human-readable scalar character that storr will use
    type = function() {
      "ssh"
    },

    ## Totally remove whereever we are storing files
    destroy = function() {
      self$delete_dir(NULL)
      self$session <- NULL
      self$root <- NULL
    },

    ## Create a directory (or something that will act as one)
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

    exists_remote = function(path, is_directory) {
      if (length(path) != 1L) {
        ## TODO: vectorise via a remote script?
        return(vapply(path, self$exists_remote, logical(1), is_directory,
                      USE.NAMES = FALSE))
      }
      flag <- if (is_directory) "d" else "e"
      path_remote <- file.path(self$root, path)
      cmd <- sprintf("test -%s %s", flag, shQuote(path_remote))
      ssh::ssh_exec_internal(self$session, cmd, error = FALSE)$status == 0L
    },

    exists = function(path) {
      self$exists_remote(path, FALSE)
    },

    exists_dir = function(path) {
      self$exists_remote(path, TRUE)
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

    delete_dir = function(path) {
      path_remote <-
        if (is.null(path)) self$root else file.path(self$root, path)
      ssh::ssh_exec_wait(self$session,
                         sprintf("rm -rf %s", shQuote(path_remote)))
    },

    ## local file to remote file
    upload_file = function(file, dest_dir) {
      if (dest_dir == ".") {
        path_remote <- self$root
      } else {
        path_remote <- file.path(self$root, dest_dir)
        self$create_dir(dest_dir)
      }
      ssh::scp_upload(self$session, file, path_remote, verbose = FALSE)
      invisible(file.path(dest_dir, basename(file)))
    },

    download_file = function(file, dest_dir) {
      file_remote <- file.path(self$root, file)
      tmp <- tempfile()
      dir.create(tmp)
      on.exit(unlink(tmp, recursive = TRUE))
      file_local <- file.path(tmp, basename(file))

      ## TODO: This really should be an error on failure in the ssh
      ## package imo (or have the option to convert to one) - the file
      ## does not exist!  I am promoting this to error here, but there
      ## may be other warnings that this catches undesirably.
      res <- tryCatch(
        ssh::scp_download(self$session, file_remote, tmp, verbose = FALSE),
        warning = identity)
      if (!file.exists(file_local)) {
        stop("Error downloading file: ", res$message)
      }
      if (is.null(dest_dir)) {
        readBin(file_local, raw(), file.size(file_local))
      } else {
        dir.create(dest_dir, FALSE, TRUE)
        file.copy(file_local, dest_dir, overwrite = TRUE)
        file.path(dest_dir, basename(file))
      }
    },

    write_bytes = function(bytes, dest_file) {
      tmp <- tempfile()
      dir.create(tmp)
      on.exit(unlink(tmp, recursive = TRUE))
      path_local <- file.path(tmp, basename(dest_file))
      writeBin(bytes, path_local)
      self$upload_file(path_local, dirname(dest_file))
    },


    read_bytes = function(file) {
      self$download_file(file, NULL)
    },

    ## NOTE: I am adding a trailing newline here so that later on R's
    ## readLines copes with these files better - this means that the
    ## remote storr is able to be used as a local storr (except for
    ## some issues with configuration).
    write_string = function(string, dest_file) {
      self$write_bytes(charToRaw(paste0(string, "\n")), dest_file)
    },

    read_string = function(file) {
      sub("\n$", "", rawToChar(self$read_bytes(file)))
    }
  ))
