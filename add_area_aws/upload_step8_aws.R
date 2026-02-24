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
local_1 <- "C:/stg4-hrap-gis/layers/cape_fear3/prepped/aws/nh_cf-boundary-mask.parquet"
key_1   <- "CONUS_subset/config/aoi/nh_cf/assets/nh_cf-boundary-mask.parquet"

raw_1 <- readBin(local_1, what = "raw", n = file.info(local_1)$size)
s3$put_object(
  Bucket = bucket,
  Key    = key_1,
  Body   = raw_1,
  ContentType = "application/octet-stream"
)
message("Uploaded: s3://", bucket, "/", key_1)

# ---------- Upload 2 ----------
local_2 <- "C:/stg4-hrap-gis/layers/cape_fear3/prepped/aws/nh_cf-area-vol-calc-masks.parquet"
key_2   <- "CONUS_subset/config/aoi/nh_cf/assets/nh_cf-area-vol-calc-masks.parquet"

raw_2 <- readBin(local_2, what = "raw", n = file.info(local_2)$size)
s3$put_object(
  Bucket = bucket,
  Key    = key_2,
  Body   = raw_2,
  ContentType = "application/octet-stream"
)
message("Uploaded: s3://", bucket, "/", key_2)