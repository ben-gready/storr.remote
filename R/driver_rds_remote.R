R6_driver_rds_remote <- R6::R6Class(
  "driver_rds_s3",
  public = list(
    ## TODO: things like hash_algorithm: do they belong in traits?
    ## This needs sorting before anyone writes their own driver!
    file_ops = NULL,
    path = NULL,
    compress = NULL,
    mangle_key = NULL,
    mangle_key_pad = NULL,
    hash_algorithm = NULL,
    traits = list(accept = "raw"),

    initialize = function(file_ops, path, compress, mangle_key, mangle_key_pad,
                          hash_algorithm) {

      is_new <- !file_ops$object_exists(path = file.path(path, "config"))
      file_ops$create_dir(path = path)
      file_ops$create_dir(path = file.path(path, "data"))
      file_ops$create_dir(path = file.path(path, "keys"))
      file_ops$create_dir(path = file.path(path, "config"))

      self$file_ops <- file_ops
      self$path <- path

      ## This is a bit of complicated dancing around to mantain
      ## backward compatibility while allowing better defaults in
      ## future versions.  I'm writing out a version number here that
      ## future versions of driver_rds can use to patch, warn or
      ## change behaviour with older versions of the storr.
      if (!is_new && !file_ops$object_exists(path = storr:::driver_rds_config_file(path, "version"))) {
        write_if_missing("1.0.1", path = storr:::driver_rds_config_file(path, "version"), file_ops = file_ops)
        write_if_missing("TRUE", path =  storr:::driver_rds_config_file(path, "mangle_key_pad"), file_ops = file_ops)
        write_if_missing("TRUE", path =  storr:::driver_rds_config_file(path, "compress"), file_ops = file_ops)
        write_if_missing("md5", path =  storr:::driver_rds_config_file(path, "hash_algorithm"), file_ops = file_ops)
      }
      ## Then write out the version number:
      write_if_missing(value = as.character(packageVersion("storr")),
                       path = storr:::driver_rds_config_file(path, "version"),
                       file_ops = file_ops)

      if (!is.null(mangle_key)) {
        storr:::assert_scalar_logical(mangle_key)
      }
      self$mangle_key <- driver_rds_config(file_ops, path, "mangle_key", mangle_key,
                                           FALSE, TRUE)

      if (!is.null(mangle_key_pad)) {
        storr:::assert_scalar_logical(mangle_key_pad)
      }
      self$mangle_key_pad <-driver_rds_config(file_ops, path, "mangle_key_pad",
                                              mangle_key_pad, FALSE, TRUE)

      if (!is.null(compress)) {
        storr:::assert_scalar_logical(compress)
      }
      self$compress <- driver_rds_config(file_ops, path, "compress", compress,
                                         TRUE, FALSE)

      if (!is.null(hash_algorithm)) {
        storr:::assert_scalar_character(hash_algorithm)
      }
      self$hash_algorithm <- driver_rds_config(file_ops, path, "hash_algorithm",
                                               hash_algorithm, "md5", TRUE)
    },

    type = function() {
      "s3"
    },

    destroy = function() {
      self$file_ops$delete_recursive(path = self$path)
    },

    get_hash = function(key, namespace) {
      self$file_ops$readLines(path = self$name_key(key, namespace))
    },

    set_hash = function(key, namespace, hash) {
      self$file_ops$create_dir(path = self$name_key("", namespace))
      self$file_ops$writeLines(text = hash, path = self$name_key(key, namespace))
      #*** should be making use of (or making an equivalent version of) the
      #write_lines function within the storr package here (I think it deletes
      #file if the write fails)
    },

    get_object = function(hash) {
      self$file_ops$readObject(path = self$name_hash(hash))
    },

    set_object = function(hash, value) {
      ## NOTE: this takes advantage of having the serialized value
      ## already and avoids seralising twice.
      storr:::assert_raw(value)

      self$file_ops$write_serialized_rds(value = value,
                                         filename = self$name_hash(hash),
                                         compress = self$compress)
    },

    exists_hash = function(key, namespace) {
      self$file_ops$object_exists(self$name_key(key, namespace))
    },

    exists_object = function(hash) {
      self$file_ops$object_exists(self$name_hash(hash))
    },

    del_hash = function(key, namespace) {
      #self$file_ops$delete_file(bucket = self$bucket, path = self$name_key(key, namespace))
      ## above deletes just one file (s3 key).
      ## However it will throw an error if the file we are trying to delete
      ## looks like a directory.
      ## S3 has no actual notion of directory, we just fake it using "/". As a
      ## result, it's possible to get into a muddle. To play it safe, line below
      ## can be uncommented to force it to delete just the path given, but throw
      ## a warning, if it does look like a directory can also change to
      ## if_dir = "del_recursive" to delete the whole directory with a warning.
      ## May never actually show up as an issue, this is just a note.
      self$file_ops$delete_file(path = self$name_key(key, namespace), if_dir = "del_only_key")
    },

    del_object = function(hash) {
      # see above note which also applies here
      self$file_ops$delete_file(path = self$name_hash(key, namespace), if_dir = "del_only_key")
    },

    list_hashes = function() {
      sub("\\.rds$", "", self$file_ops$list_dir(path = file.path(self$path, "data")))
    },

    list_namespaces = function() {
      self$file_ops$list_dir(path = file.path(self$path, "keys"))
    },

    list_keys = function(namespace) {
      ret <- self$file_ops$list_dir(path = file.path(self$path, "keys", namespace))
      if (self$mangle_key) decode64(ret, TRUE) else ret
    },

    name_hash = function(hash) {
      if (length(hash) > 0L) {
        file.path(self$path, "data", paste0(hash, ".rds"))
      } else {
        character(0)
      }
    },

    name_key = function(key, namespace) {
      if (self$mangle_key) {
        key <- encode64(key, pad = self$mangle_key_pad)
      }
      file.path(self$path, "keys", namespace, key)
    }

  ))

## This attempts to check that we are connecting to a storr of
## appropriate mangledness.  There's a lot of logic here, but it's
## actually pretty simple in practice and tested in test-driver-rds.R:
##
##   if mangle_key is NULL we take the mangledless of the
##   existing storr or set up for no mangling.
##
##   if mangle_key is not NULL then it is an error if it differs
##   from the existing storr's mangledness.
driver_rds_config <- function(file_ops, path, name, value, default, must_agree) {
  path_opt <- storr:::driver_rds_config_file(path, name)

  load_value <- function() {
    if (file_ops$object_exists(path_opt)) {
      value <- file_ops$readLines(path_opt)
      storage.mode(value) <- storage.mode(default)
    } else {
      value <- default
    }
    value
  }

  if (is.null(value)) {
    value <- load_value()
  } else if (must_agree && file_ops$object_exists(path = path_opt)) {
    value_prev <- load_value()
    if (value != value_prev) {
      stop(ConfigError(name, value_prev, value))
    }
  }
  if (!file_ops$object_exists(path = path_opt)) {
    file_ops$writeLines(text = as.character(value), path = path_opt)
  }

  value
}

write_if_missing <- function(value, path, file_ops) {
  if (file_ops$object_exists(path)) {
    file_ops$writeLines(text, path)
  }
}
