# Upload AOI "area-vol-calc" CSVs -> S3 as Parquet:
# s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/<aoi>/<aoi>-area-vol-calc-masks.parquet

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(arrow)
  library(paws)
})

# ---- inputs ----
local_dir <- "C:/stg4/stg4-hrap-gis/add_area_aws/vol_calc_areas_csv"
aois <- c("ea-rchg-zn", "nh_cf", "nueces", "scvwd")

# ---- S3 target ----
bucket <- "stg4-24hr-aws-pipeline"
s3_prefix <- "CONUS_subset/config/aoi"  # no leading slash

Sys.setenv(AWS_REGION = "us-east-2", AWS_DEFAULT_REGION = "us-east-2")
s3 <- paws::s3()

# Helpers ----------------------------------------------------------

infer_aoi_from_filename <- function(fname, aois) {
  hits <- aois[str_detect(fname, fixed(aois))]
  if (length(hits) == 1) return(hits)
  if (length(hits) == 0) stop("Could not infer AOI from filename: ", fname)
  stop("Filename matches multiple AOIs (ambiguous): ", fname, "  hits=", paste(hits, collapse = ", "))
}

# IMPORTANT: don't use file.path() for S3 keys on Windows (it uses backslashes)
make_s3_key <- function(prefix, aoi) {
  paste(prefix, aoi, paste0(aoi, "-area-vol-calc-masks.parquet"), sep = "/")
}

upload_area_vol_calc <- function(csv_path) {
  fname <- basename(csv_path)
  aoi <- infer_aoi_from_filename(fname, aois)
  key <- make_s3_key(s3_prefix, aoi)
  
  # Read + enforce column types
  df <- readr::read_csv(
    csv_path,
    show_col_types = FALSE,
    col_types = cols(
      grib_id    = col_integer(),
      hrap_x     = col_integer(),
      hrap_y     = col_integer(),
      bin_area   = col_double(),
      area_name  = col_character()
    )
  )
  
  # Validate required columns (in your desired order)
  req <- c("grib_id", "hrap_x", "hrap_y", "bin_area", "area_name")
  missing <- setdiff(req, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "), "\nFile: ", csv_path)
  }
  
  # Keep only required columns and drop exact duplicate rows
  df <- df %>%
    select(all_of(req)) %>%
    distinct()
  
  # Write parquet locally
  tmp_parq <- tempfile(pattern = paste0(aoi, "_area_vol_calc_"), fileext = ".parquet")
  arrow::write_parquet(df, tmp_parq, compression = "zstd")
  
  # Upload parquet bytes to S3
  raw_body <- readBin(tmp_parq, what = "raw", n = file.info(tmp_parq)$size)
  
  s3$put_object(
    Bucket = bucket,
    Key    = key,
    Body   = raw_body
  )
  
  message("Uploaded: s3://", bucket, "/", key, "  (rows=", nrow(df), ")")
  invisible(list(aoi = aoi, s3_key = key, n = nrow(df)))
}

# ---- run for all CSVs in the folder ----
csv_files <- list.files(local_dir, pattern = "\\.csv$", full.names = TRUE)
if (length(csv_files) == 0) stop("No .csv files found in: ", local_dir)

results <- lapply(csv_files, upload_area_vol_calc)

# Summary (plain text)
summary_df <- do.call(rbind, lapply(results, as.data.frame))
print(summary_df)
