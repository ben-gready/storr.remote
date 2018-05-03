##' @title S3 backend for rds object cache driver
##'
##' @param bucket Name of the S3 bucket for which you wish to create
##'   of connect store
##'
##' @param remote_root Base path on the remote bucket
##'
##' @param ... Additional arguments passed through to
##'   \code{storr::driver_rds}
##'
##' @param path_local Optional path to a local cache (see
##'   \code{storr::driver_remote})
##' @export
storr_s3 <- function(bucket, remote_root, ..., path_local = NULL) {
  dr <- driver_s3(bucket, remote_root, ..., path_local = path_local)
  storr::storr(dr, default_namespace)
}


##' @export
##' @rdname storr_s3
driver_s3 <- function(bucket, remote_root, ..., path_local = NULL) {
  ops <- s3_file_ops(bucket, remote_root)
  storr::driver_remote(ops, ..., path_local = path_local)
}


s3_file_ops <- function(bucket, root) {
  R6_s3_file_ops$new(bucket, root)
}


R6_s3_file_ops <- R6::R6Class(
  "s3_file_ops",

  public = list(
    bucket = NULL,

    initialize = function(bucket, root) {
      self$bucket <- bucket
      self$root <- root
    },

    type = function() {
      "s3"
    },

    destroy = function() {
      ## TODO: not sure if this is right?
      self$file_ops$delete_dir(path = self$root)
    },

    create_dir = function(path) {
      path_remote <- file.path(self$root, path)
      aws.s3::put_folder(folder = path_remote, bucket = self$bucket)
    },

    list_dir = function(path) {
      if (substr(path, nchar(path), nchar(path)) != "/") {
        path <- paste0(path, "/")
      }
      path_remote <- file.path(self$root)
      files_table <- aws.s3::get_bucket_df(bucket = self$bucket,
                                           prefix = path_root,
                                           max = Inf)
      keys <- files_table[files_table$Size > 0,]$Key
      ## TODO: this should really be stripped off the front rather
      ## than gsub?  And probably fixed replacement to avoid
      ## regexp-sensitive characters messing things up?
      files <- gsub(pattern = path_root, replacement = "", x = keys)
      split_names <- strsplit(files, "/")
      ## first element of each split name is the file or directory
      ## within path, take unique of these, so that directories only
      ## appear once
      ##
      ## TODO: I _think_ I've done the translation from
      ## unique/unlist/lapply to unique/vapply here, but I don't
      ## really get what is going on.  It's quite likely that what you
      ## really want is
      ##
      ##   unique(basename(files))
      ##
      ## perhaps?
      unique(vapply(split_names, function(x) x[[1L]], character(1)))
    },

    exists = function(path) {
      path_remote <- file.path(self$root, path)
      ## QUESTION: What messages are produced by this?
      suppressMessages(
        aws.s3::head_object(object = path_remote, bucket = self$bucket)[[1]])
    },

    exists_dir = function(path) {
      ## TODO: If it is possible to test if the remote thing is a
      ## directory (e.g., one of the bits of head_object?) that would
      ## be nice to get here too...
      self$exists(path)
    },

    delete_file = function(path) {
      if (length(path) != 1L) {
        ## TODO: this is the most naive way of doing vectorisation,
        ## but if aws.s3 can delete multiple objects (the docs suggest
        ## it can) you can probably do better.  This function must
        ## return a logical vector the same length as "path"
        ## indicating if deletion actually happened.
        return(vapply(path, self$delete_file, logical(1)))
      }
      path_remote <- file.path(self$root, path)
      ## TODO: I've copied the contents of your previous function
      ## here, but stripped it down to ony take the if_dir ==
      ## "del_only_key" path because that was what looked like what
      ## was implemented.  I have not tested this!
      files <- aws.s3::get_bucket_df(bucket = self$bucket,
                                     prefix = path_remote,
                                     max = Inf)[["Key"]]
      aws.s3::delete_object(object = path_remote, bucket = self$bucket)
    },

    delete_dir = function(path) {
      path_remote <- file.path(self$root, path)
      files <- aws.s3::get_bucket_df(bucket = self$bucket,
                                     prefix = path_remote, max = Inf)[["Key"]]
      for (x in files) {
        aws.s3::delete_object(x, self$bucket)
      }
    },

    upload_file = function(file, dest_dir) {
      ## TODO: implement me!  Upload a file with full local name
      ## `file` into a somewhere with directory prefix `dest_dir` -
      ## that's going to be `data` here really.  I have taken a guess
      ## here.
      path_remote <- file.path(self$root, dest_dir, basename(file))
      aws.s3::put_object(file = file, object = path_remote,
                         bucket = self$bucket)
    },

    download_file = function(file, dest_dir) {
      file_remote <- file.path(self$root, file)
      dest_file <- file.path(dest_dir, basename(file))
      aws.s3::save_object(file_remote, self$bucket, file = dest_file,
                          overwrite = TRUE)
    },

    write_string = function(string, dest_file) {
      file_remote <- filepath(self$root, dest_file)
      aws.s3::s3write_using(x = string, FUN = writeLines,
                            object = file_remote, bucket = self$bucket)
    },

    read_string = function(file) {
      file_remote <- file.path(self$root, dest_file)
      aws.s3::s3read_using(FUN = readLines, object = file_remote,
                           bucket = self$bucket)
    }
  ))
