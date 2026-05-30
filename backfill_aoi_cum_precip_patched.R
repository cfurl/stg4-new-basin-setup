# backfill_aoi_local_one_areaid.R
# FULL DROP-IN (creates any missing folders automatically)
#
# What it does (LOCAL backfill):
#   - Reads CONUS parquet from a local canonical archive (no S3 CONUS reads)
#   - Downloads AOI boundary mask + area-vol-calc masks ONCE from S3 (cached locally)
#   - Writes LOCAL canonical trees:
#       precip/precip_parquet/year=YYYY/month=MM/day=DD/part-0.parquet
#       stats/daily/year=YYYY/month=MM/day=DD/part-0.parquet
#       derived_ytd_precip/year=YYYY/month=MM/day=DD/part-0.parquet
#       stats/ytd/year=YYYY/month=MM/day=DD/part-0.parquet
#   - YTD resets each Jan 1
#   - Safe for reruns when skip_existing=TRUE because it seeds YTD accumulators on skip

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(paws)
  library(arrow)
})

# ============================================================
# MASTER CONFIG (EDIT ONLY THIS BLOCK)
# ============================================================
AREA_ID <- "labatt"   # <--- change this ONLY

cfg <- list(
  area_id = AREA_ID,
  cycle_hour = "12",  # fixed for 24h product
  
  # Local CONUS parquet repository (input)
  conus_local_root = "F:/conus_archive_build/stg4_24hr_conus_archive/parquet",
  
  # Local output base drive/folder
  out_base = "F:/",   # will write to: F:/<area_id>_archive_build/...
  
  # AWS creds file (only used to download AOI assets)
  renviron_path = ".Renviron",
  aws_region = "us-east-2",
  
  # S3 locations (derived ONLY from area_id)
  pipeline_bucket = "stg4-24hr-aws-pipeline",
  aoi_config_prefix = "CONUS_subset/config/aoi",
  
  # Optional processing window (inclusive). NA = ignore
  date_start = as.Date(NA),
  date_end   = as.Date(NA),
  
  # Safety cap (set Inf to unleash)
  max_days_total = 3,
  
  # Behavior
  skip_existing = TRUE,         # skip day if all 4 outputs exist
  seed_ytd_on_skip = TRUE,      # IMPORTANT: keeps YTD correct during incremental reruns
  strict_join = TRUE,
  strict_volcalc_join = TRUE,
  
  # What to write
  write_precip = TRUE,
  write_daily_stats = TRUE,
  write_derived_ytd_precip = TRUE,
  write_ytd_stats = TRUE
)
# ============================================================

# ============================================================
# Derived paths (do NOT edit)
# ============================================================
cfg$out_root <- file.path(cfg$out_base, paste0(cfg$area_id, "_archive_build"))

cfg$boundary_mask_uri <- sprintf(
  "s3://%s/%s/%s/assets/%s-boundary-mask.parquet",
  cfg$pipeline_bucket, cfg$aoi_config_prefix, cfg$area_id, cfg$area_id
)
cfg$area_vol_calcs_uri <- sprintf(
  "s3://%s/%s/%s/assets/%s-area-vol-calc-masks.parquet",
  cfg$pipeline_bucket, cfg$aoi_config_prefix, cfg$area_id, cfg$area_id
)

cfg$cache_dir <- file.path(cfg$out_root, "_cache")
cfg$qa_dir    <- file.path(cfg$out_root, "qa")

cfg$out_precip_root <- file.path(cfg$out_root, "precip", "precip_parquet")
cfg$out_daily_root  <- file.path(cfg$out_root, "stats", "daily")
cfg$out_ytd_root    <- file.path(cfg$out_root, "stats", "ytd")
cfg$out_dytd_root   <- file.path(cfg$out_root, "derived_ytd_precip")

# ============================================================
# Small utilities (folder-safe)
# ============================================================
safe_mkdir <- function(p) {
  dir.create(p, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(p)) stop("Failed to create directory: ", p)
}

ensure_dir_for_file <- function(p) safe_mkdir(dirname(p))

# Make sure the entire output tree exists BEFORE any downloads/writes
safe_mkdir(cfg$out_root)
safe_mkdir(cfg$cache_dir)
safe_mkdir(cfg$qa_dir)
safe_mkdir(cfg$out_precip_root)
safe_mkdir(cfg$out_daily_root)
safe_mkdir(cfg$out_ytd_root)
safe_mkdir(cfg$out_dytd_root)

run_id <- format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")

log_msg <- function(..., level="INFO") {
  ts <- format(Sys.time(), tz="UTC", usetz=TRUE)
  message(sprintf("[%s][AOI_LOCAL_BACKFILL][%s][%s] %s",
                  ts, level, cfg$area_id, paste0(..., collapse="")))
}

# ============================================================
# Helpers
# ============================================================
parse_s3_uri <- function(uri) {
  if (!startsWith(uri, "s3://")) stop("Not s3:// URI: ", uri)
  x <- sub("^s3://", "", uri)
  parts <- strsplit(x, "/", fixed = TRUE)[[1]]
  list(bucket = parts[1], key = paste(parts[-1], collapse = "/"))
}

download_s3_cached <- function(s3, s3_uri, local_path) {
  # KEY FIX: always create folder for the target file
  ensure_dir_for_file(local_path)
  
  if (file.exists(local_path) && file.info(local_path)$size > 0) {
    log_msg("Cache hit: ", local_path)
    return(local_path)
  }
  
  p <- parse_s3_uri(s3_uri)
  log_msg("Downloading asset: ", s3_uri)
  
  obj <- s3$get_object(Bucket = p$bucket, Key = p$key)
  
  # Write atomically to avoid partial files
  tmp <- tempfile(fileext = ".tmp", tmpdir = dirname(local_path))
  writeBin(obj$Body, tmp)
  file.rename(tmp, local_path)
  
  sz <- file.info(local_path)$size
  if (is.na(sz) || sz < 10 * 1024) stop("Downloaded asset too small: ", local_path, " size=", sz)
  log_msg("Cached: ", local_path, " (", format(sz, big.mark=","), " bytes)")
  local_path
}

detect_rain_col <- function(df, cycle_eff) {
  candidates <- names(df)[grepl("^rain_\\d{10}_mm$", names(df))]
  if (length(candidates) == 0) stop("No rain_YYYYMMDDHH_mm column found in CONUS parquet.")
  if (length(candidates) == 1) return(candidates)
  exact <- paste0("rain_", cycle_eff, "_mm")
  if (exact %in% candidates) return(exact)
  stop("Multiple rain cols found; none match cycle_eff=", cycle_eff,
       " found=", paste(candidates, collapse=", "))
}

parse_date_from_conus_path <- function(p) {
  m <- str_match(p, "year=(\\d{4})[/\\\\]month=(\\d{2})[/\\\\]day=(\\d{2})[/\\\\]part-0\\.parquet$")
  if (any(is.na(m))) return(as.Date(NA))
  as.Date(sprintf("%s-%s-%s", m[2], m[3], m[4]))
}

out_day_path <- function(root, d) {
  d <- as.Date(d, origin="1970-01-01")
  file.path(
    root,
    paste0("year=",  format(d, "%Y")),
    paste0("month=", format(d, "%m")),
    paste0("day=",   format(d, "%d")),
    "part-0.parquet"
  )
}

# ============================================================
# 0) AWS init (only to download 2 assets)
# ============================================================
if (file.exists(cfg$renviron_path)) readRenviron(cfg$renviron_path)
Sys.setenv(AWS_REGION = cfg$aws_region, AWS_DEFAULT_REGION = cfg$aws_region)
s3 <- paws::s3(config = list(region = cfg$aws_region))

log_msg("Resolved asset URIs: ",
        "mask=", cfg$boundary_mask_uri,
        " vol=", cfg$area_vol_calcs_uri)

# ============================================================
# 1) Download + read AOI assets (cached locally)
# ============================================================
mask_cache <- file.path(cfg$cache_dir, paste0(cfg$area_id, "-boundary-mask.parquet"))
vol_cache  <- file.path(cfg$cache_dir, paste0(cfg$area_id, "-area-vol-calc-masks.parquet"))

download_s3_cached(s3, cfg$boundary_mask_uri, mask_cache)
download_s3_cached(s3, cfg$area_vol_calcs_uri, vol_cache)

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
log_msg("Vol-mask rows: ", nrow(vol_masks), " distinct areas=", dplyr::n_distinct(vol_masks$area_name))

# ============================================================
# 2) Discover local CONUS files
# ============================================================
conus_files <- list.files(cfg$conus_local_root, pattern="part-0\\.parquet$",
                          recursive=TRUE, full.names=TRUE)
if (length(conus_files) == 0) stop("No CONUS parquets found under: ", cfg$conus_local_root)

conus_dates <- as.Date(vapply(conus_files, parse_date_from_conus_path, as.Date(NA)))
ok <- !is.na(conus_dates)
conus_files <- conus_files[ok]
conus_dates <- conus_dates[ok]

if (!is.na(cfg$date_start)) {
  keep <- conus_dates >= cfg$date_start
  conus_files <- conus_files[keep]; conus_dates <- conus_dates[keep]
}
if (!is.na(cfg$date_end)) {
  keep <- conus_dates <= cfg$date_end
  conus_files <- conus_files[keep]; conus_dates <- conus_dates[keep]
}

ord <- order(conus_dates)
conus_files <- conus_files[ord]
conus_dates <- conus_dates[ord]

if (length(conus_files) == 0) stop("No CONUS files left after date filtering.")

if (is.finite(cfg$max_days_total) && length(conus_files) > cfg$max_days_total) {
  conus_files <- conus_files[1:cfg$max_days_total]
  conus_dates <- conus_dates[1:cfg$max_days_total]
}

log_msg("Days selected: ", length(conus_files),
        " (", as.character(min(conus_dates)), " .. ", as.character(max(conus_dates)), ")")

# ============================================================
# 3) PROCESS YEAR-BY-YEAR (YTD resets each Jan 1)
# ============================================================
years <- sort(unique(format(conus_dates, "%Y")))

for (yr in years) {
  
  log_msg("---- YEAR ", yr, " ----")
  
  idx <- format(conus_dates, "%Y") == yr
  year_files <- conus_files[idx]
  year_dates <- conus_dates[idx]
  ord2 <- order(year_dates)
  year_files <- year_files[ord2]
  year_dates <- year_dates[ord2]
  
  cum_ytd <- NULL
  cum_vol <- NULL
  
  year_start <- as.Date(paste0(yr, "-01-01"))
  processed_dates <- as.Date(character(0))
  
  for (ii in seq_along(year_files)) {
    
    f <- year_files[ii]
    d <- as.Date(year_dates[ii], origin="1970-01-01")
    cycle_eff <- paste0(format(d, "%Y%m%d"), cfg$cycle_hour)
    
    out_precip <- out_day_path(cfg$out_precip_root, d)
    out_daily  <- out_day_path(cfg$out_daily_root,  d)
    out_dytd   <- out_day_path(cfg$out_dytd_root,   d)
    out_ytd    <- out_day_path(cfg$out_ytd_root,    d)
    
    all_exist <- file.exists(out_precip) && file.exists(out_daily) &&
      file.exists(out_dytd) && file.exists(out_ytd)
    
    if (isTRUE(cfg$skip_existing) && all_exist) {
      
      if (isTRUE(cfg$seed_ytd_on_skip)) {
        dytd_existing <- arrow::read_parquet(out_dytd)
        cum_ytd <- dytd_existing %>%
          transmute(grib_id = as.integer(grib_id),
                    rain_ytd_mm = as.numeric(rain_ytd_mm)) %>%
          distinct(grib_id, .keep_all = TRUE)
        
        ytd_existing <- arrow::read_parquet(out_ytd)
        cum_vol <- ytd_existing %>%
          transmute(area_name = as.character(area_name),
                    area_m2 = as.numeric(area_m2),
                    ytd_vol_m3 = as.numeric(ytd_vol_m3)) %>%
          distinct(area_name, .keep_all = TRUE)
      }
      
      processed_dates <- sort(unique(c(processed_dates, d)))
      log_msg("[", yr, " ", ii, "/", length(year_files), "] SKIP (exists): ", as.character(d))
      next
    }
    
    log_msg("[", yr, " ", ii, "/", length(year_files), "] Reading CONUS: ", f)
    
    conus <- arrow::read_parquet(f)
    rain_col <- detect_rain_col(conus, cycle_eff)
    
    conus2 <- conus %>%
      mutate(
        cycle = cycle_eff,
        rain_mm = .data[[rain_col]]
      ) %>%
      select(-all_of(rain_col))
    
    # A) AOI precip subset
    joined <- conus2 %>%
      inner_join(mask, by="grib_id", suffix=c("", "_mask"))
    
    mismatch_n <- joined %>%
      filter(hrap_x != hrap_x_mask | hrap_y != hrap_y_mask) %>%
      nrow()
    
    if (mismatch_n > 0) {
      msg <- paste0("HRAP mismatch after boundary join: ", mismatch_n, " rows on ", as.character(d))
      if (isTRUE(cfg$strict_join)) stop(msg) else log_msg(msg, level="WARN")
    }
    
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
      log_msg("WARN: AOI subset is 0 rows on ", as.character(d), " (skipping day)", level="WARN")
      next
    }
    
    if (isTRUE(cfg$write_precip)) {
      ensure_dir_for_file(out_precip)
      arrow::write_parquet(aoi, out_precip, compression="zstd")
      if (file.info(out_precip)$size < 10*1024) stop("Precip parquet too small: ", out_precip)
    }
    
    # B) Daily stats
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
    
    if (mismatch2 > 0) {
      msg2 <- paste0("HRAP mismatch after vol-calcs join: ", mismatch2, " rows on ", as.character(d))
      if (isTRUE(cfg$strict_volcalc_join)) stop(msg2) else log_msg(msg2, level="WARN")
    }
    
    daily_stats <- stats_join %>%
      mutate(
        rain_mm0 = ifelse(is.na(rain_mm), 0, rain_mm),
        vol_m3_bin = (rain_mm0 / 1000) * bin_area
      ) %>%
      group_by(area_name) %>%
      summarise(
        area_id   = cfg$area_id,
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
    
    if (isTRUE(cfg$write_daily_stats)) {
      ensure_dir_for_file(out_daily)
      arrow::write_parquet(daily_stats, out_daily, compression="zstd")
      if (file.info(out_daily)$size < 1024) stop("Daily stats parquet too small: ", out_daily)
    }
    
    # coverage stats
    processed_dates <- sort(unique(c(processed_dates, d)))
    days_present  <- length(processed_dates)
    days_expected <- length(seq.Date(year_start, d, by="day"))
    days_missing  <- days_expected - days_present
    
    # C) derived_ytd_precip (reset each year)
    if (isTRUE(cfg$write_derived_ytd_precip)) {
      
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
      
      ensure_dir_for_file(out_dytd)
      arrow::write_parquet(ytd_cells, out_dytd, compression="zstd")
      if (file.info(out_dytd)$size < 1024) stop("derived_ytd_precip parquet too small: ", out_dytd)
    }
    
    # D) stats/ytd (reset each year)
    if (isTRUE(cfg$write_ytd_stats)) {
      
      daily_vol <- daily_stats %>%
        transmute(
          area_name = as.character(area_name),
          area_m2   = as.numeric(area_m2),
          vol_m3    = as.numeric(vol_m3)
        )
      
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
          area_id = cfg$area_id,
          year = yr,
          days_present  = days_present,
          days_expected = days_expected,
          days_missing  = days_missing,
          ytd_avg_mm = (ytd_vol_m3 / area_m2) * 1000
        ) %>%
        select(area_id, area_name, year, days_present, days_expected, days_missing,
               area_m2, ytd_vol_m3, ytd_avg_mm)
      
      ensure_dir_for_file(out_ytd)
      arrow::write_parquet(ytd_stats, out_ytd, compression="zstd")
      if (file.info(out_ytd)$size < 1024) stop("YTD stats parquet too small: ", out_ytd)
    }
    
    log_msg("OK ", as.character(d),
            " | days_present=", days_present,
            " days_expected=", days_expected,
            " days_missing=", days_missing)
  }
  
  log_msg("YEAR DONE: ", yr, " (days=", length(year_files), ")")
}

log_msg("DONE. out_root=", cfg$out_root)