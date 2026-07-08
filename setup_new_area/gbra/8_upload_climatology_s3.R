# 8_upload_climatology_s3.R
# Upload locally built climatology parquet files to the dashboard config folder in S3.
#
# This script is intentionally simple:
#   - reads AWS credentials/settings from .Renviron at the current working root, if present
#   - uploads zero, one, or both local parquet files based on config flags
#   - does not create/modify local climatology files
#   - does not upload CSV files

suppressPackageStartupMessages({
  library(paws)
})

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

# Optional but useful for local dev.
# The script will look for .Renviron in the current working directory.
PROJECT_ROOT <- getwd()
RENVRION_PATH <- file.path(PROJECT_ROOT, ".Renviron")

AWS_REGION <- "us-east-2"

# Local parquet files built by scripts 6 and 7.
LOCAL_VALUES_PARQUET <- "F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_values_ea-rchg-zn_2002_2025.parquet"
LOCAL_SUMMARY_PARQUET <- "F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_summary_ea-rchg-zn_2002_2025.parquet"

# Upload switches.
UPLOAD_VALUES_PARQUET <- TRUE
UPLOAD_SUMMARY_PARQUET <- TRUE

# Destination.
BUCKET <- "stg4-24hr-aws-ea-rchg-zn"
S3_PREFIX <- "config/dashboard_config"

# If TRUE, print what would upload but do not call S3.
DRY_RUN <- FALSE

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

load_local_renviron <- function(path) {
  if (file.exists(path)) {
    readRenviron(path)
    message("Loaded .Renviron: ", normalizePath(path, winslash = "/", mustWork = FALSE))
  } else {
    message("No .Renviron found at: ", normalizePath(path, winslash = "/", mustWork = FALSE))
  }

  invisible(TRUE)
}

clean_s3_prefix <- function(x) {
  x <- gsub("\\\\", "/", x)
  x <- gsub("^/+", "", x)
  x <- gsub("/+$", "", x)
  x
}

make_s3_key <- function(prefix, local_file) {
  prefix <- clean_s3_prefix(prefix)
  key <- paste(prefix, basename(local_file), sep = "/")
  gsub("\\\\", "/", key)
}

format_file_size <- function(bytes) {
  if (is.na(bytes)) return("unknown size")

  units <- c("B", "KB", "MB", "GB")
  size <- as.numeric(bytes)
  unit <- 1L

  while (size >= 1024 && unit < length(units)) {
    size <- size / 1024
    unit <- unit + 1L
  }

  sprintf("%.2f %s", size, units[[unit]])
}

upload_one_parquet <- function(s3, label, local_file, upload_flag, bucket, prefix, dry_run = FALSE) {
  if (!isTRUE(upload_flag)) {
    message("Skipping ", label, " parquet because upload flag is FALSE.")
    return(invisible(FALSE))
  }

  if (!file.exists(local_file)) {
    stop("Configured ", label, " parquet does not exist: ", local_file)
  }

  file_info <- file.info(local_file)
  key <- make_s3_key(prefix, local_file)
  s3_uri <- paste0("s3://", bucket, "/", key)

  message("")
  message("Preparing ", label, " parquet upload")
  message("  Local: ", normalizePath(local_file, winslash = "/", mustWork = TRUE))
  message("  Size:  ", format_file_size(file_info$size))
  message("  S3:    ", s3_uri)

  if (isTRUE(dry_run)) {
    message("  DRY_RUN is TRUE; not uploading.")
    return(invisible(TRUE))
  }

  s3$put_object(
    Bucket      = bucket,
    Key         = key,
    Body        = local_file,
    ContentType = "application/vnd.apache.parquet"
  )

  message("Uploaded ", label, " parquet to: ", s3_uri)

  invisible(TRUE)
}

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

load_local_renviron(RENVRION_PATH)

Sys.setenv(
  AWS_REGION = AWS_REGION,
  AWS_DEFAULT_REGION = AWS_REGION
)

if (!isTRUE(UPLOAD_VALUES_PARQUET) && !isTRUE(UPLOAD_SUMMARY_PARQUET)) {
  message("Both upload flags are FALSE. Nothing to upload.")
  quit(save = "no", status = 0)
}

s3 <- paws::s3()

upload_one_parquet(
  s3 = s3,
  label = "values",
  local_file = LOCAL_VALUES_PARQUET,
  upload_flag = UPLOAD_VALUES_PARQUET,
  bucket = BUCKET,
  prefix = S3_PREFIX,
  dry_run = DRY_RUN
)

upload_one_parquet(
  s3 = s3,
  label = "summary",
  local_file = LOCAL_SUMMARY_PARQUET,
  upload_flag = UPLOAD_SUMMARY_PARQUET,
  bucket = BUCKET,
  prefix = S3_PREFIX,
  dry_run = DRY_RUN
)

message("")
message("Done.")
