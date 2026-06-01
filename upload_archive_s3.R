# upload_texas_mrb_archive_to_s3.R
# Uploads local canonical parquet trees to S3 (no file modifications).
# Writes QA/metadata logs to: F:/texas_mrb_archive_build/QA_results

suppressPackageStartupMessages({
  library(paws)
  library(jsonlite)
})

# ============================================================
# CONFIG (edit here only)
# ============================================================
READ_RENVIRON_PATH <- ".Renviron"   # you said you'll have this in the working dir

AWS_REGION <- "us-east-2"

QA_DIR <- "F:/labatt_archive_build/QA_results"

# Local roots (canonical parquet trees)
LOCAL <- list(
  precip_parquet      = "F:/labatt_archive_build/precip/precip_parquet",
  derived_ytd_precip  = "F:/labatt_archive_build/derived_ytd_precip",
  stats_daily         = "F:/labatt_archive_build/stats/daily",
  stats_ytd           = "F:/labatt_archive_build/stats/ytd"
)

# Target S3 prefixes (must end with /)
S3 <- list(
  precip_parquet      = "s3://stg4-24hr-aws-pipeline/CONUS_subset/production_areas/labatt/precip/precip_parquet/",
  derived_ytd_precip  = "s3://stg4-24hr-aws-pipeline/CONUS_subset/production_areas/labatt/derived_ytd_precip/",
  stats_daily         = "s3://stg4-24hr-aws-pipeline/CONUS_subset/production_areas/labatt/stats/daily/",
  stats_ytd           = "s3://stg4-24hr-aws-pipeline/CONUS_subset/production_areas/labatt/stats/ytd/"
)

# Behavior
DRY_RUN <- FALSE                     # TRUE = no uploads, just QA plan
SKIP_IF_EXISTS_SAME_SIZE <- TRUE     # uses HEAD; skips if ContentLength matches
VERIFY_S3_COUNTS_AFTER <- FALSE      # lists all objects after; can be slow for huge archives

# ============================================================
# Helpers
# ============================================================
parse_s3_uri <- function(uri) {
  if (!startsWith(uri, "s3://")) stop("Not s3:// URI: ", uri)
  x <- sub("^s3://", "", uri)
  parts <- strsplit(x, "/", fixed = TRUE)[[1]]
  bucket <- parts[1]
  key <- paste(parts[-1], collapse = "/")
  list(bucket = bucket, key = key)
}

norm_slash <- function(p) {
  p <- normalizePath(p, winslash = "/", mustWork = FALSE)
  sub("/+$", "", p)
}

rel_path <- function(full, root) {
  full <- norm_slash(full)
  root <- norm_slash(root)
  if (!startsWith(tolower(full), tolower(root))) {
    stop("File not under root.\n  file=", full, "\n  root=", root)
  }
  rp <- substring(full, nchar(root) + 2)  # +1 for slash, +1 for 1-index
  gsub("\\\\", "/", rp)
}

ensure_dir <- function(p) dir.create(p, recursive = TRUE, showWarnings = FALSE)

utc_stamp <- function() format(Sys.time(), tz = "UTC", usetz = TRUE)

list_s3_counts <- function(s3, bucket, prefix) {
  # returns list(count, bytes) for all objects under prefix
  token <- NULL
  n <- 0L
  bytes <- 0
  repeat {
    resp <- s3$list_objects_v2(Bucket = bucket, Prefix = prefix, ContinuationToken = token)
    if (!is.null(resp$Contents) && length(resp$Contents) > 0) {
      n <- n + length(resp$Contents)
      bytes <- bytes + sum(vapply(resp$Contents, function(x) as.numeric(x$Size), numeric(1)))
    }
    if (isTRUE(resp$IsTruncated)) token <- resp$NextContinuationToken else break
  }
  list(count = n, bytes = bytes)
}

# ============================================================
# Init
# ============================================================
if (file.exists(READ_RENVIRON_PATH)) readRenviron(READ_RENVIRON_PATH)

Sys.setenv(AWS_REGION = AWS_REGION, AWS_DEFAULT_REGION = AWS_REGION)
s3_client <- paws::s3(config = list(region = AWS_REGION))

ensure_dir(QA_DIR)

run_id <- format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")
qa_csv  <- file.path(QA_DIR, paste0("upload_manifest_", run_id, ".csv"))
qa_json <- file.path(QA_DIR, paste0("upload_summary_",  run_id, ".json"))
qa_log  <- file.path(QA_DIR, paste0("upload_log_",      run_id, ".txt"))

sink(qa_log, split = TRUE)
cat("Run ID: ", run_id, "\n")
cat("UTC: ", utc_stamp(), "\n")
cat("DRY_RUN: ", DRY_RUN, "\n")
cat("SKIP_IF_EXISTS_SAME_SIZE: ", SKIP_IF_EXISTS_SAME_SIZE, "\n")
cat("VERIFY_S3_COUNTS_AFTER: ", VERIFY_S3_COUNTS_AFTER, "\n\n")

# ============================================================
# Build upload plan
# ============================================================
datasets <- names(LOCAL)
for (nm in datasets) {
  if (!dir.exists(LOCAL[[nm]])) stop("Local directory missing: ", LOCAL[[nm]])
}

plan <- list()
for (nm in datasets) {
  files <- list.files(LOCAL[[nm]], pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  plan[[nm]] <- files
  cat("Found ", length(files), " parquet files under ", nm, ": ", LOCAL[[nm]], "\n", sep = "")
}
cat("\n")

# ============================================================
# Upload loop
# ============================================================
manifest_rows <- list()
summary <- list(
  run_id = run_id,
  created_utc = utc_stamp(),
  aws_region = AWS_REGION,
  dry_run = DRY_RUN,
  skip_if_exists_same_size = SKIP_IF_EXISTS_SAME_SIZE,
  datasets = list()
)

for (nm in datasets) {
  
  cat("==== DATASET: ", nm, " ====\n", sep = "")
  local_root <- norm_slash(LOCAL[[nm]])
  s3_uri <- S3[[nm]]
  p <- parse_s3_uri(s3_uri)
  
  # ensure prefix ends with /
  s3_prefix <- p$key
  if (!endsWith(s3_prefix, "/")) s3_prefix <- paste0(s3_prefix, "/")
  
  files <- plan[[nm]]
  if (length(files) == 0) {
    cat("No files; skipping dataset.\n\n")
    next
  }
  
  ds_stats <- list(
    local_root = local_root,
    s3_uri = s3_uri,
    local_count = length(files),
    local_bytes = sum(file.info(files)$size, na.rm = TRUE),
    uploaded = 0L,
    skipped = 0L,
    failed = 0L
  )
  
  for (i in seq_along(files)) {
    f <- files[i]
    f2 <- norm_slash(f)
    rp <- rel_path(f2, local_root)
    key <- paste0(s3_prefix, rp)
    
    sz <- file.info(f2)$size
    if (is.na(sz) || sz <= 0) {
      ds_stats$failed <- ds_stats$failed + 1L
      manifest_rows[[length(manifest_rows) + 1]] <- data.frame(
        dataset = nm, local_file = f2, s3_bucket = p$bucket, s3_key = key,
        size_bytes = NA_real_, action = "FAIL", reason = "local_size_invalid",
        etag = NA_character_, stringsAsFactors = FALSE
      )
      next
    }
    
    # Optional skip check via HEAD
    if (!DRY_RUN && isTRUE(SKIP_IF_EXISTS_SAME_SIZE)) {
      hd <- try(s3_client$head_object(Bucket = p$bucket, Key = key), silent = TRUE)
      if (!inherits(hd, "try-error") && !is.null(hd$ContentLength) && as.numeric(hd$ContentLength) == as.numeric(sz)) {
        ds_stats$skipped <- ds_stats$skipped + 1L
        manifest_rows[[length(manifest_rows) + 1]] <- data.frame(
          dataset = nm, local_file = f2, s3_bucket = p$bucket, s3_key = key,
          size_bytes = as.numeric(sz), action = "SKIP", reason = "exists_same_size",
          etag = if (!is.null(hd$ETag)) as.character(hd$ETag) else NA_character_,
          stringsAsFactors = FALSE
        )
        if (i %% 250 == 0) cat("Progress ", i, "/", length(files), " (", nm, ")\n", sep = "")
        next
      }
    }
    
    if (DRY_RUN) {
      ds_stats$skipped <- ds_stats$skipped + 1L
      manifest_rows[[length(manifest_rows) + 1]] <- data.frame(
        dataset = nm, local_file = f2, s3_bucket = p$bucket, s3_key = key,
        size_bytes = as.numeric(sz), action = "DRY_RUN", reason = "",
        etag = NA_character_, stringsAsFactors = FALSE
      )
      if (i %% 250 == 0) cat("Progress ", i, "/", length(files), " (", nm, ")\n", sep = "")
      next
    }
    
    # Upload (single PUT; reads file into raw)
    raw <- readBin(f2, what = "raw", n = sz)
    resp <- try(
      s3_client$put_object(
        Bucket = p$bucket,
        Key = key,
        Body = raw,
        ContentType = "application/octet-stream"
      ),
      silent = TRUE
    )
    
    if (inherits(resp, "try-error")) {
      ds_stats$failed <- ds_stats$failed + 1L
      manifest_rows[[length(manifest_rows) + 1]] <- data.frame(
        dataset = nm, local_file = f2, s3_bucket = p$bucket, s3_key = key,
        size_bytes = as.numeric(sz), action = "FAIL",
        reason = substr(as.character(resp), 1, 500),
        etag = NA_character_, stringsAsFactors = FALSE
      )
    } else {
      ds_stats$uploaded <- ds_stats$uploaded + 1L
      manifest_rows[[length(manifest_rows) + 1]] <- data.frame(
        dataset = nm, local_file = f2, s3_bucket = p$bucket, s3_key = key,
        size_bytes = as.numeric(sz), action = "UPLOAD", reason = "",
        etag = if (!is.null(resp$ETag)) as.character(resp$ETag) else NA_character_,
        stringsAsFactors = FALSE
      )
    }
    
    if (i %% 250 == 0) cat("Progress ", i, "/", length(files), " (", nm, ")\n", sep = "")
  }
  
  # Optional verify counts after upload
  if (!DRY_RUN && isTRUE(VERIFY_S3_COUNTS_AFTER)) {
    cat("Verifying S3 counts for prefix: s3://", p$bucket, "/", s3_prefix, "\n", sep = "")
    cts <- list_s3_counts(s3_client, p$bucket, s3_prefix)
    ds_stats$s3_count_after <- cts$count
    ds_stats$s3_bytes_after <- cts$bytes
  }
  
  summary$datasets[[nm]] <- ds_stats
  
  cat("Dataset summary: ", nm, "\n", sep = "")
  cat("  local_count   : ", ds_stats$local_count, "\n", sep = "")
  cat("  local_bytes   : ", format(ds_stats$local_bytes, big.mark=","), "\n", sep = "")
  cat("  uploaded      : ", ds_stats$uploaded, "\n", sep = "")
  cat("  skipped       : ", ds_stats$skipped, "\n", sep = "")
  cat("  failed        : ", ds_stats$failed, "\n", sep = "")
  if (!is.null(ds_stats$s3_count_after)) {
    cat("  s3_count_after: ", ds_stats$s3_count_after, "\n", sep = "")
    cat("  s3_bytes_after: ", format(ds_stats$s3_bytes_after, big.mark=","), "\n", sep = "")
  }
  cat("\n")
}

# ============================================================
# Write QA outputs
# ============================================================
manifest_df <- do.call(rbind, manifest_rows)
write.csv(manifest_df, qa_csv, row.names = FALSE)

writeLines(jsonlite::toJSON(summary, auto_unbox = TRUE, pretty = TRUE), qa_json)

cat("Wrote QA manifest CSV: ", qa_csv, "\n", sep = "")
cat("Wrote QA summary JSON : ", qa_json, "\n", sep = "")
cat("Wrote log            : ", qa_log, "\n", sep = "")

sink()
message("DONE. QA outputs in: ", normalizePath(QA_DIR, winslash = "/"))