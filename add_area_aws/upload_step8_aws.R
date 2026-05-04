# This script uploads your 1) boundary mask, 2) area vol calc mask, and 3) cells.gpkg


suppressPackageStartupMessages({
  library(paws)
})

# Load AWS creds (local)
readRenviron(".Renviron")

# Optional: sanitize env vars in case .Renviron had quotes/spaces
clean_env <- function(name) {
  v <- Sys.getenv(name, unset = "")
  v <- trimws(v)
  v <- sub('^"', "", v)
  v <- sub('"$', "", v)
  do.call(Sys.setenv, setNames(list(v), name))
}
clean_env("AWS_ACCESS_KEY_ID")
clean_env("AWS_SECRET_ACCESS_KEY")
clean_env("AWS_SESSION_TOKEN")

# Region
Sys.setenv(AWS_REGION="us-east-2", AWS_DEFAULT_REGION="us-east-2")
s3 <- paws::s3(config = list(region = "us-east-2"))

bucket <- "stg4-24hr-aws-pipeline"

# ---------- Upload 1 ----------

local_1 <- "C:/stg4-hrap-gis/layers/texas_mrb/area_setup/prepped/aws/texas_mrb-boundary-mask.parquet"
key_1   <- "CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-boundary-mask.parquet"

raw_1 <- readBin(local_1, what = "raw", n = file.info(local_1)$size)
s3$put_object(
  Bucket = bucket,
  Key    = key_1,
  Body   = raw_1,
  ContentType = "application/octet-stream"
)
message("Uploaded: s3://", bucket, "/", key_1)

# ---------- Upload 2 ----------
local_2 <- "C:/stg4-hrap-gis/layers/texas_mrb/area_setup/prepped/aws/texas_mrb-area-vol-calc-masks.parquet"
key_2   <- "CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-area-vol-calc-masks.parquet"

raw_2 <- readBin(local_2, what = "raw", n = file.info(local_2)$size)
s3$put_object(
  Bucket = bucket,
  Key    = key_2,
  Body   = raw_2,
  ContentType = "application/octet-stream"
)
message("Uploaded: s3://", bucket, "/", key_2)

# ---------- Upload 3 ----------
# (GeoPackage: cells.gpkg)

local_3 <- "C:/stg4-hrap-gis/layers/texas_mrb/area_setup/prepped/cells.gpkg"  # <-- fill in
key_3   <- "CONUS_subset/config/aoi/texas_mrb/assets/cells.gpkg"  # <-- fill in

raw_3 <- readBin(local_3, what = "raw", n = file.info(local_3)$size)
s3$put_object(
  Bucket = bucket,
  Key    = key_3,
  Body   = raw_3,
  ContentType = "application/geopackage+sqlite3"
  # If that content-type ever gives you grief, swap to:
  # ContentType = "application/octet-stream"
)
message("Uploaded: s3://", bucket, "/", key_3)