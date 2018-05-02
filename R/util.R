`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}


read_binary <- function(x) {
  readBin(x, raw(), file.size(x))
}
