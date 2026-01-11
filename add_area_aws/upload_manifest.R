suppressPackageStartupMessages({
  library(paws)
})

# optional (useful for local dev)
Sys.setenv(AWS_REGION = "us-east-2", AWS_DEFAULT_REGION = "us-east-2")

local_json <- "./add_area_aws/aoi_manifest.json"
if (!file.exists(local_json)) stop("Manifest not found at: ", local_json)

bucket <- "stg4-24hr-aws-pipeline"
key    <- "CONUS_subset/config/aoi_manifest.json"

s3 <- paws::s3()

s3$put_object(
  Bucket      = bucket,
  Key         = key,
  Body        = local_json,
  ContentType = "application/json"
)

message("Uploaded manifest to: s3://", bucket, "/", key)
