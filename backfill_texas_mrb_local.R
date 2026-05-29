suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(paws)
  library(arrow)
  library(jsonlite)
})

# ============================================================
# CONFIG (edit here only)
# ============================================================
AREA_ID <- "texas_mrb"

# Local CONUS parquet archive (input)
CONUS_LOCAL_ROOT <- "F:/conus_archive_build/stg4_24hr_conus_archive/parquet"

# Output roots (local)
OUT_ROOT <- "F:/texas_mrb_archive_build"
OUT_PRECIP_ROOT <- file.path(OUT_ROOT, "precip")
OUT_STATS_ROOT  <- file.path(OUT_ROOT, "stats")
OUT_DYTD_ROOT   <- file.path(OUT_ROOT, "derived_ytd_precip")

# Test run: only process first N days found (chronological)
max_days <- Inf

# Optional: constrain to a year/date range (leave as NA to ignore)
DATE_START <- as.Date(NA)   # e.g., as.Date("2026-03-01")
DATE_END   <- as.Date(NA)   # e.g., as.Date("2026-05-03")

# S3 assets (download ONCE, then reuse local cache files)
PIPELINE_BUCKET <- "stg4-24hr-aws-pipeline"
MASK_URI <- "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-boundary-mask.parquet"
VOL_URI  <- "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-area-vol-calc-masks.parquet"

# Where to cache downloaded assets locally
CACHE_DIR <- file.path(OUT_ROOT, "_cache")

# ============================================================
# AWS creds only used to download mask/vol assets
# ============================================================
readRenviron(".Renviron")
Sys.setenv(AWS_REGION="us-east-2", AWS_DEFAULT_REGION="us-east-2")
s3 <- paws::s3(config = list(region="us-east-2"))

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_PRECIP_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_STATS_ROOT,  recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DYTD_ROOT,   recursive = TRUE, showWarnings = FALSE)

log_msg <- function(...) {
  ts <- format(Sys.time(), tz="UTC", usetz=TRUE)
  message(sprintf("[%s][TEXAS_MRB_BACKFILL] %s", ts, paste0(..., collapse="")))
}

# ============================================================
# Helpers
# ============================================================
parse_s3_uri <- function(uri) {
  x <- sub("^s3://", "", uri)
  parts <- strsplit(x, "/", fixed=TRUE)[[1]]
  list(bucket = parts[1], key = paste(parts[-1], collapse="/"))
}

download_s3_cached <- function(s3, s3_uri, local_path) {
  if (file.exists(local_path) && file.info(local_path)$size > 0) {
    log_msg("Cache hit: ", local_path)
    return(local_path)
  }
  p <- parse_s3_uri(s3_uri)
  log_msg("Downloading asset: ", s3_uri)
  obj <- s3$get_object(Bucket=p$bucket, Key=p$key)
  writeBin(obj$Body, local_path)
  if (file.info(local_path)$size < 10*1024) stop("Downloaded asset too small: ", local_path)
  log_msg("Cached: ", local_path, " (", file.info(local_path)$size, " bytes)")
  local_path
}

detect_rain_col <- function(df, cycle_eff) {
  candidates <- names(df)[grepl("^rain_\\d{10}_mm$", names(df))]
  if (length(candidates) == 0) stop("No rain_YYYYMMDDHH_mm column found in CONUS parquet.")
  if (length(candidates) == 1) return(candidates)
  exact <- paste0("rain_", cycle_eff, "_mm")
  if (exact %in% candidates) return(exact)
  stop("Multiple rain cols found; none match cycle_eff=", cycle_eff, " found=", paste(candidates, collapse=", "))
}

# parse date from local conus path: .../year=YYYY/month=MM/day=DD/part-0.parquet
parse_date_from_path <- function(p) {
  m <- str_match(p, "year=(\\d{4})[/\\\\]month=(\\d{2})[/\\\\]day=(\\d{2})[/\\\\]part-0\\.parquet$")
  if (any(is.na(m))) return(as.Date(NA))
  as.Date(sprintf("%s-%s-%s", m[2], m[3], m[4]))
}

out_day_path <- function(root, d) {
  file.path(root,
            paste0("year=", format(d, "%Y")),
            paste0("month=", format(d, "%m")),
            paste0("day=", format(d, "%d")),
            "part-0.parquet")
}

ensure_dir_for_file <- function(p) dir.create(dirname(p), recursive=TRUE, showWarnings=FALSE)

# ============================================================
# 1) Download mask + vol masks from S3 once
# ============================================================
mask_cache <- file.path(CACHE_DIR, paste0(AREA_ID, "-boundary-mask.parquet"))
vol_cache  <- file.path(CACHE_DIR, paste0(AREA_ID, "-area-vol-calc-masks.parquet"))

download_s3_cached(s3, MASK_URI, mask_cache)
download_s3_cached(s3, VOL_URI,  vol_cache)

mask <- arrow::read_parquet(mask_cache) %>%
  transmute(
    grib_id  = as.integer(grib_id),
    hrap_x   = as.integer(hrap_x),
    hrap_y   = as.integer(hrap_y),
    bin_area = as.numeric(bin_area)
  ) %>%
  distinct(grib_id, .keep_all = TRUE)

vol_masks <- arrow::read_parquet(vol_cache) %>%
  transmute(
    grib_id   = as.integer(grib_id),
    hrap_x    = as.integer(hrap_x),
    hrap_y    = as.integer(hrap_y),
    bin_area  = as.numeric(bin_area),
    area_name = as.character(area_name)
  ) %>%
  distinct(grib_id, hrap_x, hrap_y, bin_area, area_name, .keep_all = TRUE)

log_msg("Mask rows: ", nrow(mask))
log_msg("Vol-mask rows: ", nrow(vol_masks), " areas=", dplyr::n_distinct(vol_masks$area_name))

# ============================================================
# 2) Find local CONUS parquet files to process (chronological)
# ============================================================
conus_files <- list.files(CONUS_LOCAL_ROOT, pattern="part-0\\.parquet$", recursive=TRUE, full.names=TRUE)
if (length(conus_files) == 0) stop("No CONUS parquets found under: ", CONUS_LOCAL_ROOT)

conus_dates <- as.Date(vapply(conus_files, parse_date_from_path, as.Date(NA)))
ok <- !is.na(conus_dates)

conus_files <- conus_files[ok]
conus_dates <- conus_dates[ok]

if (!is.na(DATE_START)) {
  keep <- conus_dates >= DATE_START
  conus_files <- conus_files[keep]; conus_dates <- conus_dates[keep]
}
if (!is.na(DATE_END)) {
  keep <- conus_dates <= DATE_END
  conus_files <- conus_files[keep]; conus_dates <- conus_dates[keep]
}

ord <- order(conus_dates)
conus_files <- conus_files[ord]
conus_dates <- conus_dates[ord]

if (length(conus_files) == 0) stop("No CONUS files left after date filtering.")

if (!is.null(max_days) && is.finite(max_days) && length(conus_files) > max_days) {
  conus_files <- conus_files[1:max_days]
  conus_dates <- conus_dates[1:max_days]
}

log_msg("Days selected: ", length(conus_files),
        " (", as.character(min(conus_dates)), " .. ", as.character(max(conus_dates)), ")")

# ============================================================
# 3) Process each day: precip parquet + daily stats + derived_ytd_precip + ytd stats
#    (YTD built incrementally because we are running in chronological order)
# ============================================================

# Running cumulative per-cell (for derived_ytd_precip)
cum_ytd <- NULL  # data.frame with grib_id, rain_ytd_mm

# Running cumulative per-basin volume (for YTD stats)
cum_vol <- NULL  # data.frame with area_name, area_m2, ytd_vol_m3

for (i in seq_along(conus_files)) {
  
  f <- conus_files[i]
  d <- conus_dates[i]
  cycle_eff <- paste0(format(d, "%Y%m%d"), "12")
  
  log_msg("[", i, "/", length(conus_files), "] Reading CONUS: ", f)
  
  conus <- arrow::read_parquet(f)
  
  rain_col <- detect_rain_col(conus, cycle_eff)
  
  conus2 <- conus %>%
    mutate(
      cycle = cycle_eff,
      rain_mm = .data[[rain_col]]
    ) %>%
    select(-all_of(rain_col))
  
  # -------------------------
  # AOI precip subset (local)
  # -------------------------
  joined <- conus2 %>%
    inner_join(mask, by="grib_id", suffix=c("", "_mask"))
  
  mismatch_n <- joined %>%
    filter(hrap_x != hrap_x_mask | hrap_y != hrap_y_mask) %>%
    nrow()
  if (mismatch_n > 0) stop("HRAP mismatch after join by grib_id: ", mismatch_n, " rows on ", as.character(d))
  
  aoi <- joined %>%
    transmute(
      cycle    = cycle,
      lat      = as.numeric(lat),
      lon      = as.numeric(lon),
      hrap_x   = as.integer(hrap_x),
      hrap_y   = as.integer(hrap_y),
      grib_id  = as.integer(grib_id),
      bin_area = as.numeric(bin_area),
      rain_mm  = as.numeric(rain_mm)
    )
  
  if (nrow(aoi) == 0) {
    log_msg("WARN: AOI subset is 0 rows on ", as.character(d), " (skipping day)")
    next
  }
  
  # write precip parquet (canonical local)
  out_precip <- out_day_path(file.path(OUT_PRECIP_ROOT, "precip_parquet"), d)
  ensure_dir_for_file(out_precip)
  arrow::write_parquet(aoi, out_precip, compression="zstd")
  if (file.info(out_precip)$size < 10*1024) stop("Precip parquet too small: ", out_precip)
  
  # -------------------------
  # Daily stats (local)
  # -------------------------
  rain_df <- conus2 %>%
    transmute(
      grib_id = as.integer(grib_id),
      hrap_x  = as.integer(hrap_x),
      hrap_y  = as.integer(hrap_y),
      rain_mm = as.numeric(rain_mm)
    )
  
  stats_join <- rain_df %>%
    inner_join(vol_masks, by="grib_id", suffix=c("", "_vol"))
  
  mismatch2 <- stats_join %>%
    filter(hrap_x != hrap_x_vol | hrap_y != hrap_y_vol) %>%
    nrow()
  if (mismatch2 > 0) stop("HRAP mismatch after vol-calcs join: ", mismatch2, " rows on ", as.character(d))
  
  daily_stats <- stats_join %>%
    mutate(
      rain_mm0 = ifelse(is.na(rain_mm), 0, rain_mm),
      vol_m3_bin = (rain_mm0 / 1000) * bin_area
    ) %>%
    group_by(area_name) %>%
    summarise(
      area_id   = AREA_ID,
      cycle_eff = cycle_eff,
      date_utc  = as.character(d),
      area_m2   = sum(bin_area, na.rm = TRUE),
      vol_m3    = sum(vol_m3_bin, na.rm = TRUE),
      basin_avg_mm = (vol_m3 / area_m2) * 1000,
      max_bin_mm   = max(rain_mm0, na.rm = TRUE),
      n_bins    = dplyr::n(),
      .groups = "drop"
    ) %>%
    select(area_id, area_name, cycle_eff, date_utc, n_bins, area_m2, vol_m3, basin_avg_mm, max_bin_mm)
  
  out_daily <- out_day_path(file.path(OUT_STATS_ROOT, "daily"), d)
  ensure_dir_for_file(out_daily)
  arrow::write_parquet(daily_stats, out_daily, compression="zstd")
  if (file.info(out_daily)$size < 1*1024) stop("Daily stats parquet too small: ", out_daily)
  
  # -------------------------
  # derived_ytd_precip (local) – incremental per-cell YTD
  # -------------------------
  day_rain <- aoi %>%
    transmute(
      grib_id  = as.integer(grib_id),
      hrap_x   = as.integer(hrap_x),
      hrap_y   = as.integer(hrap_y),
      bin_area = as.numeric(bin_area),
      rain_mm  = as.numeric(rain_mm)
    )
  
  if (is.null(cum_ytd)) {
    cum_ytd <- day_rain %>%
      transmute(grib_id, rain_ytd_mm = ifelse(is.na(rain_mm), 0, rain_mm))
  } else {
    cum_ytd <- cum_ytd %>%
      full_join(day_rain %>% transmute(grib_id, add = ifelse(is.na(rain_mm), 0, rain_mm)),
                by="grib_id") %>%
      mutate(
        rain_ytd_mm = ifelse(is.na(rain_ytd_mm), 0, rain_ytd_mm) + ifelse(is.na(add), 0, add)
      ) %>%
      select(grib_id, rain_ytd_mm)
  }
  
  ytd_cells <- day_rain %>%
    select(grib_id, hrap_x, hrap_y, bin_area) %>%
    left_join(cum_ytd, by="grib_id") %>%
    mutate(rain_ytd_mm = as.numeric(rain_ytd_mm))
  
  out_dytd <- out_day_path(OUT_DYTD_ROOT, d)
  ensure_dir_for_file(out_dytd)
  arrow::write_parquet(ytd_cells, out_dytd, compression="zstd")
  if (file.info(out_dytd)$size < 1*1024) stop("derived_ytd_precip parquet too small: ", out_dytd)
  
  # -------------------------
  # YTD stats (local) – incremental per-basin cumulative volume
  # -------------------------
  daily_vol <- daily_stats %>%
    select(area_name, area_m2, vol_m3)
  
  if (is.null(cum_vol)) {
    cum_vol <- daily_vol %>%
      transmute(area_name, area_m2, ytd_vol_m3 = vol_m3)
  } else {
    cum_vol <- cum_vol %>%
      full_join(daily_vol %>% transmute(area_name, area_m2_new = area_m2, add_vol = vol_m3),
                by="area_name") %>%
      mutate(
        area_m2 = ifelse(is.na(area_m2), area_m2_new, area_m2),
        ytd_vol_m3 = ifelse(is.na(ytd_vol_m3), 0, ytd_vol_m3) + ifelse(is.na(add_vol), 0, add_vol)
      ) %>%
      select(area_name, area_m2, ytd_vol_m3)
  }
  
  ytd_stats <- cum_vol %>%
    mutate(
      area_id = AREA_ID,
      year = format(d, "%Y"),
      days_present = i,
      days_expected = i,
      days_missing = 0L,
      ytd_avg_mm = (ytd_vol_m3 / area_m2) * 1000
    ) %>%
    select(area_id, area_name, year, days_present, days_expected, days_missing, area_m2, ytd_vol_m3, ytd_avg_mm)
  
  out_ytd <- out_day_path(file.path(OUT_STATS_ROOT, "ytd"), d)
  ensure_dir_for_file(out_ytd)
  arrow::write_parquet(ytd_stats, out_ytd, compression="zstd")
  if (file.info(out_ytd)$size < 1*1024) stop("YTD stats parquet too small: ", out_ytd)
  
  log_msg("OK day ", as.character(d),
          " | precip=", out_precip,
          " | daily=", out_daily,
          " | dytd=", out_dytd,
          " | ytd=", out_ytd)
}

log_msg("DONE. Wrote local archive under: ", OUT_ROOT)