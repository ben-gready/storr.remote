##' @title S3 backend for rds object cache driver
##' @param bucket Name of the S3 bucket for which you wish to create or connect
##' store
##' @inheritParams storr::storr_rds
storr_rds_s3 <- function(bucket, path, compress = NULL, mangle_key = NULL,
                         mangle_key_pad = NULL, hash_algorithm = NULL,
                         default_namespace = "objects") {

  storr::storr(driver_rds_s3(bucket, path, compress, mangle_key, mangle_key_pad, hash_algorithm),
               default_namespace)
}

##' @export
##' @rdname storr_rds_s3
driver_rds_s3 <- function(bucket, path, compress = NULL, mangle_key = NULL,
                          mangle_key_pad = NULL, hash_algorithm = NULL) {

  R6_driver_rds_remote$new(s3_file_ops(bucket), path, compress, mangle_key, mangle_key_pad, hash_algorithm)
}
