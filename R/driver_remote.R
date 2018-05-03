driver_remote <- function(ops, rds) {
  R6_driver_remote$new(ops, rds)
}

R6_driver_remote <- R6::R6Class(
  "driver_remote",

  public = list(
    ops = NULL,
    rds = NULL,
    traits = NULL,
    hash_algorithm = "md5",

    initialize = function(ops, rds) {
      ## For the configuration, we should pass along all dots to the
      ## remote storr and check?  Or do that when controlling the
      ## local rds.  So this is going to get a lot of dots and do the
      ## build itself.
      self$ops <- ops
      self$rds <- rds
      self$ops$create_dir("data")
      self$ops$create_dir("keys")
      self$traits <- self$rds$traits
      ## TODO: deal with this when configuration is done:
      self$traits$hash_algorithm <- FALSE
      self$hash_algorithm <- "md5"
    },

    type = function() {
      sprintf("remote/%s", self$ops$type())
    },

    destroy = function() {
      self$ops$destroy()
      self$ops <- NULL
    },

    get_hash = function(key, namespace) {
      self$ops$read_string(self$name_key(key, namespace))
    },

    set_hash = function(key, namespace, hash) {
      self$ops$write_string(hash, self$name_key(key, namespace))
    },

    get_object = function(hash) {
      filename_local <- self$rds$name_hash(hash)
      if (!self$rds$exists_object(hash)) {
        filename_remote <- self$name_hash(hash)
        self$ops$download_file(filename_remote, filename_local)
      }
      readRDS(filename_local)
    },

    set_object = function(hash, value) {
      filename_remote <- self$name_hash(hash)
      if (!self$ops$exists(filename_remote)) {
        filename_local <- self$rds$name_hash(hash)
        if (!file.exists(filename_local)) {
          self$rds$set_object(hash, value)
        }
        self$ops$upload_file(filename_local, dirname(filename_remote))
      }
    },

    exists_hash = function(key, namespace) {
      self$ops$exists(self$name_key(key, namespace))
    },

    exists_object = function(hash) {
      self$ops$exists(self$name_hash(hash))
    },


    del_hash = function(key, namespace) {
      self$ops$delete_file(self$name_key(key, namespace))
    },

    del_object = function(hash) {
      self$ops$delete_file(self$name_hash(hash))
    },

    list_hashes = function() {
      sub("\\.rds$", "", self$ops$list_dir("data"))
    },

    list_namespaces = function() {
      self$ops$list_dir("keys")
    },

    list_keys = function(namespace) {
      path <- file.path("keys", namespace)
      if (!self$ops$exists(path, "directory")) {
        return(character(0))
      }
      ret <- self$ops$list_dir(path)
      if (self$rds$mangle_key) storr::decode64(ret, TRUE) else ret
    },

    ## These functions could be done better if driver_rds takes a
    ## 'relative' argument
    name_hash = function(hash) {
      p <- self$rds$name_hash(hash)
      file.path(basename(dirname(p)), basename(p))
    },

    name_key = function(key, namespace) {
      p <- self$rds$name_key(key, namespace)
      file.path(
        basename(dirname(dirname(p))),
        basename(dirname(p)),
        basename(p))
    }))
