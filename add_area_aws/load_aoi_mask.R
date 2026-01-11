suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(arrow)
  library(paws)
})

bucket <- "stg4-24hr-aws-pipeline"
prefix <- "CONUS_subset/config/area-masks"  # no leading slash

# If you're in ECS, these are usually already set, but doesn't hurt:
Sys.setenv(AWS_REGION = "us-east-2", AWS_DEFAULT_REGION = "us-east-2")

s3 <- paws::s3()

write_mask_parquet_and_upload <- function(csv_path, bucket, prefix) {
  area_name <- tools::file_path_sans_ext(basename(csv_path))  # e.g., "EA-rchg-zn", "Nueces"
  key <- file.path(prefix, paste0(area_name, ".parquet"))
  
  # Read + enforce column types
  df <- readr::read_csv(
    csv_path,
    show_col_types = FALSE,
    col_types = cols(
      grib_id  = col_integer(),
      hrap_x   = col_integer(),
      hrap_y   = col_integer(),
      bin_area = col_double()
    )
  )
  
  # Validate required columns
  req <- c("grib_id", "hrap_x", "hrap_y", "bin_area")
  missing <- setdiff(req, names(df))
  if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))
  
  # Optional: drop duplicates (keeps first occurrence)
  df <- df %>%
    select(all_of(req)) %>%
    distinct(grib_id, hrap_x, hrap_y, .keep_all = TRUE)
  
  # Write parquet locally
  tmp_parq <- tempfile(pattern = paste0(area_name, "_"), fileext = ".parquet")
  arrow::write_parquet(df, tmp_parq, compression = "zstd")
  
  # Upload to S3
  s3$put_object(
    Bucket = bucket,
    Key    = key,
    Body   = tmp_parq
  )
  
  message("Uploaded: s3://", bucket, "/", key, "  (rows=", nrow(df), ")")
  invisible(list(area_name = area_name, s3_key = key, n = nrow(df)))
}

# ---- run for your two files ----
csv_files <- c(
  "./add_area_aws/nh_cf.csv"
)

results <- lapply(csv_files, write_mask_parquet_and_upload, bucket = bucket, prefix = prefix)
