##' @title S3 file op equivalents
##' @param bucket Name of the S3 bucket for which you wish to create or connect
##' store
s3_file_ops <- function(bucket){
  R6_s3_file_ops$new(bucket)
}

##' @export
##' @rdname s3_file_ops
R6_s3_file_ops <- R6::R6Class(
  "s3_file_ops",
  public = list(
    ## TODO: things like hash_algorithm: do they belong in traits?
    ## This needs sorting before anyone writes their own driver!

    bucket = NULL,

    initialize = function(bucket) {

      self$bucket <- bucket

    },

    write_serialized_rds = function(value, filename, compress) {
      # write_serialized_rds(value, self$name_hash(hash), self$compress)
      aws.s3::s3write_using(x = value,
                            FUN = function(v, f) storr:::write_serialized_rds(value = v, filename = f, compress=compress),
                            object = filename,
                            bucket = self$bucket)
    },

    readObject = function(path){
      aws.s3::s3readRDS(object = path, bucket = self$bucket)
    },

    writeLines = function(text, path) {
      aws.s3::s3write_using(x = text, FUN = writeLines, object = path, bucket = self$bucket)
    },

    readLines = function(path) {
      aws.s3::s3read_using(FUN = readLines, object = path, bucket = self$bucket)
    },

    create_dir = function(path) {
      aws.s3::put_folder(folder = path, bucket = self$bucket)
    },

    object_exists = function(path) {
      suppressMessages(aws.s3::head_object(object = path, bucket = self$bucket)[1])
    },

    list_dir = function(path) {

      files_table <- aws.s3::get_bucket_df(bucket = self$bucket, prefix = path, max = Inf)
      keys <- files_table[files_table$Size > 0,]$Key
      files <- gsub(pattern = path, replacement = "", x = keys)
      split_names <- strsplit(files, "/")
      # first element of each split name is the file or directory within path,
      # take unique of these, so that directories only appear once
      unique(unlist(lapply(split_names, function(x) x[1])))
    },

    delete_recursive = function(path, force=FALSE) {

      files <- aws.s3::get_bucket_df(bucket = self$bucket, prefix = path, max = Inf)[["Key"]]
      invisible(lapply(files, function(x) aws.s3::delete_object(x, self$bucket)))
    },

    delete_file = function(path, if_dir = c("stop", "del_only_key", "del_recursive")) {

      files <- aws.s3::get_bucket_df(bucket = self$bucket, prefix = path, max = Inf)[["Key"]]

      if(length(files) > 1){
        if_dir == match.arg(if_dir) # only need this if we get inside this loop

        if(if_dir == "stop"){
          stop("You are trying to delete 1 file, but it looks like it is setup like
               a directory")
        } else if(if_dir == "del_only_key"){
          warning("You are trying to delete 1 file, but it looks like it is setup
                  like a directory. Deleted specific path you requested")
          invisible(aws.s3::delete_object(object = path, bucket = self$bucket))
        } else if(if_dir == "del_recursive"){
          warning("You are trying to delete 1 file, but it looks like it is setup
                  like a directory. Deleting recursively everyting below the path
                  you specified")
          s3_delete_recursive(bucket, path)
        }
      } else{
        invisible(aws.s3::delete_object(object = path, bucket = self$bucket))
      }
    }

  )
)
