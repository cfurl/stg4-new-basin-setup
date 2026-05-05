# backfill_aoi_local_strict_schema.R
#
# Strict local AOI backfill/rebuild for Stage IV 24-hour precipitation.
#
# Purpose:
#   Rebuilds the local AOI archive from local CONUS parquet while enforcing
#   the current production/S3 column names and schemas for:
#
#   1) precip/precip_parquet
#   2) derived_ytd_precip
#   3) stats/daily
#   4) stats/ytd
#
# Important schema choices:
#   - precip/precip_parquet keeps lat/lon as CHARACTER to preserve the
#     variable decimal-place formatting inherited from CONUS parquet.
#   - derived_ytd_precip writes lat/lon as DOUBLE, and does NOT include bin_area.
#   - YTD products reset every January 1.
#
# Expected local CONUS input:
#   .../year=YYYY/month=MM/day=DD/part-0.parquet
#
# Expected CONUS rain column:
#   rain_YYYYMMDDHH_mm
#
# ----------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(paws)
  library(arrow)
})

# ============================================================
# CONFIG: EDIT ONLY THIS BLOCK
# ============================================================
cfg <- list(
  # Identity
  area_id = "texas_mrb",
  cycle_hour = "12",   # 24h product uses 12Z
  
  # Local CONUS parquet repository input.
  # Expects: .../year=YYYY/month=MM/day=DD/part-0.parquet
  conus_local_root = "D:/conus_archive_build/stg4_24hr_conus_archive/parquet",
  
  # Local output root.
  # Writes:
  #   out_root/precip/precip_parquet
  #   out_root/derived_ytd_precip
  #   out_root/stats/daily
  #   out_root/stats/ytd
  out_root = "D:/texas_mrb_archive_build",
  
  # Prefer these local cached asset files when present.
  local_boundary_mask_file   = "D:/texas_mrb_archive_build/_cache/texas_mrb-boundary-mask.parquet",
  local_area_vol_calcs_file  = "D:/texas_mrb_archive_build/_cache/texas_mrb-area-vol-calc-masks.parquet",
  
  # S3 asset URIs used only if local cached files above do not exist.
  boundary_mask_uri = "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-boundary-mask.parquet",
  area_vol_calcs_uri = "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/texas_mrb/assets/texas_mrb-area-vol-calc-masks.parquet",
  
  # AWS creds only needed if the cache files do not exist and assets must be downloaded.
  renviron_path = ".Renviron",
  aws_region = "us-east-2",
  
  # Processing window.
  # Use full available archive by default.
  date_start = as.Date("2002-01-01"),
  date_end   = as.Date("2002-01-31"), #2026-05-03
  
  # Safety: set Inf for full rebuild.
  max_days_total = Inf,
  
  # Rebuild behavior.
  # WARNING: delete_existing_outputs = TRUE deletes local output folders below before rebuilding.
  delete_existing_outputs = TRUE,
  skip_existing = FALSE,
  
  # Strict join checks.
  strict_join = TRUE,
  strict_volcalc_join = TRUE,
  
  # What to write.
  write_precip = TRUE,
  write_daily_stats = TRUE,
  write_derived_ytd_precip = TRUE,
  write_ytd_stats = TRUE,
  
  # Validate each data frame before writing. This is strict and recommended.
  validate_before_write = TRUE
)
# ============================================================


# ============================================================
# SCHEMA CONTRACTS
# ============================================================

precip_cols <- c(
  "cycle", "lat", "lon", "hrap_x", "hrap_y", "grib_id", "bin_area", "rain_mm"
)

dytd_cols <- c(
  "lat", "lon", "hrap_x", "hrap_y", "grib_id", "rain_ytd_mm",
  "year", "thru_date_utc", "days_present", "days_expected", "days_missing"
)

daily_cols <- c(
  "area_id", "area_name", "cycle_eff", "date_utc", "n_bins",
  "area_m2", "vol_m3", "basin_avg_mm", "max_bin_mm",
  "pct_area_gt_2p54mm", "pct_area_gt_6p35mm", "pct_area_gt_12p7mm",
  "pct_area_gt_19p05mm", "pct_area_gt_25p4mm", "pct_area_gt_31p75mm",
  "pct_area_gt_38p1mm"
)

ytd_cols <- c(
  "area_id", "area_name", "year", "days_present", "days_expected",
  "days_missing", "area_m2", "ytd_vol_m3", "ytd_avg_mm"
)


# ============================================================
# DERIVED PATHS
# ============================================================

cfg$cache_dir <- file.path(cfg$out_root, "_cache")

cfg$out_precip_root <- file.path(cfg$out_root, "precip", "precip_parquet")
cfg$out_daily_root  <- file.path(cfg$out_root, "stats", "daily")
cfg$out_ytd_root    <- file.path(cfg$out_root, "stats", "ytd")
cfg$out_dytd_root   <- file.path(cfg$out_root, "derived_ytd_precip")

dir.create(cfg$cache_dir, recursive = TRUE, showWarnings = FALSE)


# ============================================================
# LOGGING
# ============================================================

log_msg <- function(..., level = "INFO") {
  ts <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  message(sprintf("[%s][AOI_LOCAL_BACKFILL][%s] %s", ts, level, paste0(..., collapse = "")))
}


# ============================================================
# HELPERS
# ============================================================

parse_s3_uri <- function(uri) {
  x <- sub("^s3://", "", uri)
  parts <- strsplit(x, "/", fixed = TRUE)[[1]]
  list(bucket = parts[1], key = paste(parts[-1], collapse = "/"))
}

init_s3_if_needed <- function() {
  if (file.exists(cfg$renviron_path)) readRenviron(cfg$renviron_path)
  Sys.setenv(AWS_REGION = cfg$aws_region, AWS_DEFAULT_REGION = cfg$aws_region)
  paws::s3(config = list(region = cfg$aws_region))
}

download_s3_cached <- function(s3, s3_uri, local_path) {
  if (file.exists(local_path) && file.info(local_path)$size > 0) {
    log_msg("Cache hit: ", local_path)
    return(local_path)
  }
  
  p <- parse_s3_uri(s3_uri)
  log_msg("Downloading asset: ", s3_uri)
  obj <- s3$get_object(Bucket = p$bucket, Key = p$key)
  
  dir.create(dirname(local_path), recursive = TRUE, showWarnings = FALSE)
  writeBin(obj$Body, local_path)
  
  if (is.na(file.info(local_path)$size) || file.info(local_path)$size < 10 * 1024) {
    stop("Downloaded asset too small: ", local_path)
  }
  
  log_msg("Cached: ", local_path, " (", file.info(local_path)$size, " bytes)")
  local_path
}

resolve_asset_file <- function(local_file, s3_uri, cache_file) {
  if (!is.na(local_file) && nzchar(local_file) && file.exists(local_file) && file.info(local_file)$size > 0) {
    log_msg("Using local asset: ", local_file)
    return(local_file)
  }
  
  s3 <- init_s3_if_needed()
  download_s3_cached(s3, s3_uri, cache_file)
}

detect_rain_col <- function(df, cycle_eff) {
  candidates <- names(df)[grepl("^rain_\\d{10}_mm$", names(df))]
  
  if (length(candidates) == 0) {
    stop("No rain_YYYYMMDDHH_mm column found in CONUS parquet.")
  }
  
  exact <- paste0("rain_", cycle_eff, "_mm")
  
  if (exact %in% candidates) {
    return(exact)
  }
  
  if (length(candidates) == 1) {
    return(candidates)
  }
  
  stop(
    "Multiple rain cols found; none match cycle_eff=", cycle_eff,
    " found=", paste(candidates, collapse = ", ")
  )
}

parse_date_from_conus_path <- function(p) {
  m <- str_match(
    p,
    "year=(\\d{4})[/\\\\]month=(\\d{2})[/\\\\]day=(\\d{2})[/\\\\]part-0\\.parquet$"
  )
  
  if (any(is.na(m))) return(as.Date(NA))
  
  as.Date(sprintf("%s-%s-%s", m[2], m[3], m[4]))
}

out_day_path <- function(root, d) {
  d <- as.Date(d, origin = "1970-01-01")
  
  file.path(
    root,
    paste0("year=",  format(d, "%Y")),
    paste0("month=", format(d, "%m")),
    paste0("day=",   format(d, "%d")),
    "part-0.parquet"
  )
}

ensure_dir_for_file <- function(p) {
  dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
}

atomic_write_parquet <- function(df, path, compression = "zstd") {
  ensure_dir_for_file(path)
  
  tmp <- file.path(
    dirname(path),
    paste0(".tmp_", tools::file_path_sans_ext(basename(path)), "_", Sys.getpid(), ".parquet")
  )
  
  if (file.exists(tmp)) unlink(tmp, force = TRUE)
  arrow::write_parquet(df, tmp, compression = compression)
  
  if (!file.exists(tmp) || is.na(file.info(tmp)$size) || file.info(tmp)$size < 256) {
    stop("Temp parquet write failed or too small: ", tmp)
  }
  
  if (file.exists(path)) unlink(path, force = TRUE)
  ok <- file.rename(tmp, path)
  
  if (!ok) {
    # Fallback for filesystems where rename over destination can be cranky.
    file.copy(tmp, path, overwrite = TRUE)
    unlink(tmp, force = TRUE)
  }
  
  if (!file.exists(path) || is.na(file.info(path)$size) || file.info(path)$size < 256) {
    stop("Final parquet missing or too small: ", path)
  }
  
  invisible(path)
}

validate_names <- function(df, expected_cols, label) {
  actual <- names(df)
  
  if (!identical(actual, expected_cols)) {
    stop(
      label, " column mismatch.\n",
      "Expected: ", paste(expected_cols, collapse = ", "), "\n",
      "Actual:   ", paste(actual, collapse = ", ")
    )
  }
  
  invisible(TRUE)
}

validate_classes <- function(df, spec, label) {
  for (nm in names(spec)) {
    expected <- spec[[nm]]
    actual <- class(df[[nm]])[1]
    
    if (!identical(actual, expected)) {
      stop(
        label, " class mismatch for column '", nm, "'. ",
        "Expected ", expected, "; got ", actual
      )
    }
  }
  
  invisible(TRUE)
}

validate_precip_df <- function(df) {
  validate_names(df, precip_cols, "precip/precip_parquet")
  
  validate_classes(
    df,
    list(
      cycle = "character",
      lat = "character",
      lon = "character",
      hrap_x = "integer",
      hrap_y = "integer",
      grib_id = "integer",
      bin_area = "numeric",
      rain_mm = "numeric"
    ),
    "precip/precip_parquet"
  )
}

validate_dytd_df <- function(df) {
  validate_names(df, dytd_cols, "derived_ytd_precip")
  
  validate_classes(
    df,
    list(
      lat = "numeric",
      lon = "numeric",
      hrap_x = "integer",
      hrap_y = "integer",
      grib_id = "integer",
      rain_ytd_mm = "numeric",
      year = "integer",
      thru_date_utc = "character",
      days_present = "integer",
      days_expected = "integer",
      days_missing = "integer"
    ),
    "derived_ytd_precip"
  )
}

validate_daily_df <- function(df) {
  validate_names(df, daily_cols, "stats/daily")
  
  validate_classes(
    df,
    list(
      area_id = "character",
      area_name = "character",
      cycle_eff = "character",
      date_utc = "character",
      n_bins = "integer",
      area_m2 = "numeric",
      vol_m3 = "numeric",
      basin_avg_mm = "numeric",
      max_bin_mm = "numeric",
      pct_area_gt_2p54mm = "numeric",
      pct_area_gt_6p35mm = "numeric",
      pct_area_gt_12p7mm = "numeric",
      pct_area_gt_19p05mm = "numeric",
      pct_area_gt_25p4mm = "numeric",
      pct_area_gt_31p75mm = "numeric",
      pct_area_gt_38p1mm = "numeric"
    ),
    "stats/daily"
  )
}

validate_ytd_df <- function(df) {
  validate_names(df, ytd_cols, "stats/ytd")
  
  validate_classes(
    df,
    list(
      area_id = "character",
      area_name = "character",
      year = "character",
      days_present = "integer",
      days_expected = "integer",
      days_missing = "integer",
      area_m2 = "numeric",
      ytd_vol_m3 = "numeric",
      ytd_avg_mm = "numeric"
    ),
    "stats/ytd"
  )
}


# ============================================================
# CLEAN OUTPUTS
# ============================================================

if (isTRUE(cfg$delete_existing_outputs)) {
  targets <- c(
    cfg$out_precip_root,
    cfg$out_daily_root,
    cfg$out_ytd_root,
    cfg$out_dytd_root
  )
  
  for (target in targets) {
    if (dir.exists(target)) {
      log_msg("Deleting existing output tree: ", target, level = "WARN")
      unlink(target, recursive = TRUE, force = TRUE)
    }
  }
}

dir.create(cfg$out_precip_root, recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_daily_root,  recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_ytd_root,    recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_dytd_root,   recursive = TRUE, showWarnings = FALSE)


# ============================================================
# 1) READ AOI ASSETS
# ============================================================

mask_cache <- file.path(cfg$cache_dir, paste0(cfg$area_id, "-boundary-mask.parquet"))
vol_cache  <- file.path(cfg$cache_dir, paste0(cfg$area_id, "-area-vol-calc-masks.parquet"))

mask_file <- resolve_asset_file(
  local_file = cfg$local_boundary_mask_file,
  s3_uri = cfg$boundary_mask_uri,
  cache_file = mask_cache
)

vol_file <- resolve_asset_file(
  local_file = cfg$local_area_vol_calcs_file,
  s3_uri = cfg$area_vol_calcs_uri,
  cache_file = vol_cache
)

mask <- arrow::read_parquet(mask_file) %>%
  transmute(
    grib_id = as.integer(grib_id),
    hrap_x = as.integer(hrap_x),
    hrap_y = as.integer(hrap_y),
    bin_area = as.numeric(bin_area)
  ) %>%
  distinct(grib_id, .keep_all = TRUE)

vol_masks <- arrow::read_parquet(vol_file) %>%
  transmute(
    grib_id = as.integer(grib_id),
    hrap_x = as.integer(hrap_x),
    hrap_y = as.integer(hrap_y),
    bin_area = as.numeric(bin_area),
    area_name = as.character(area_name)
  ) %>%
  distinct(grib_id, hrap_x, hrap_y, bin_area, area_name, .keep_all = TRUE)

if (nrow(mask) == 0) stop("Boundary mask has zero rows.")
if (nrow(vol_masks) == 0) stop("Volume mask has zero rows.")

log_msg("Boundary mask rows: ", nrow(mask))
log_msg("Vol-mask rows: ", nrow(vol_masks), " distinct areas=", n_distinct(vol_masks$area_name))


# ============================================================
# 2) DISCOVER LOCAL CONUS FILES
# ============================================================

conus_files <- list.files(
  cfg$conus_local_root,
  pattern = "part-0\\.parquet$",
  recursive = TRUE,
  full.names = TRUE
)

if (length(conus_files) == 0) {
  stop("No CONUS parquets found under: ", cfg$conus_local_root)
}

conus_dates <- as.Date(vapply(conus_files, parse_date_from_conus_path, as.Date(NA)))
ok <- !is.na(conus_dates)

conus_files <- conus_files[ok]
conus_dates <- conus_dates[ok]

if (!is.na(cfg$date_start)) {
  keep <- conus_dates >= cfg$date_start
  conus_files <- conus_files[keep]
  conus_dates <- conus_dates[keep]
}

if (!is.na(cfg$date_end)) {
  keep <- conus_dates <= cfg$date_end
  conus_files <- conus_files[keep]
  conus_dates <- conus_dates[keep]
}

ord <- order(conus_dates)
conus_files <- conus_files[ord]
conus_dates <- conus_dates[ord]

if (length(conus_files) == 0) {
  stop("No CONUS files left after date filtering.")
}

if (is.finite(cfg$max_days_total) && length(conus_files) > cfg$max_days_total) {
  conus_files <- conus_files[seq_len(cfg$max_days_total)]
  conus_dates <- conus_dates[seq_len(cfg$max_days_total)]
}

log_msg(
  "Days selected: ", length(conus_files),
  " (", as.character(min(conus_dates)), " .. ", as.character(max(conus_dates)), ")"
)


# ============================================================
# 3) PROCESS YEAR BY YEAR; YTD RESETS EACH JAN 1
# ============================================================

years <- sort(unique(format(conus_dates, "%Y")))

failed_dates <- character(0)
processed_output <- list()

for (yr in years) {
  
  log_msg("---- YEAR ", yr, " ----")
  
  idx <- format(conus_dates, "%Y") == yr
  year_files <- conus_files[idx]
  year_dates <- conus_dates[idx]
  
  ord2 <- order(year_dates)
  year_files <- year_files[ord2]
  year_dates <- year_dates[ord2]
  
  # Reset YTD accumulators for this year.
  cum_ytd <- NULL  # grib_id, rain_ytd_mm
  cum_vol <- NULL  # area_name, area_m2, ytd_vol_m3
  
  year_start <- as.Date(paste0(yr, "-01-01"))
  processed_dates <- as.Date(character(0))
  
  for (ii in seq_along(year_files)) {
    
    f <- year_files[ii]
    d <- as.Date(year_dates[ii], origin = "1970-01-01")
    cycle_eff <- paste0(format(d, "%Y%m%d"), cfg$cycle_hour)
    
    out_precip <- out_day_path(cfg$out_precip_root, d)
    out_daily  <- out_day_path(cfg$out_daily_root,  d)
    out_dytd   <- out_day_path(cfg$out_dytd_root,   d)
    out_ytd    <- out_day_path(cfg$out_ytd_root,    d)
    
    all_exist <- file.exists(out_precip) && file.exists(out_daily) &&
      file.exists(out_dytd) && file.exists(out_ytd)
    
    if (isTRUE(cfg$skip_existing) && all_exist) {
      log_msg("[", yr, " ", ii, "/", length(year_files), "] SKIP exists: ", as.character(d))
      processed_dates <- sort(unique(c(processed_dates, d)))
      next
    }
    
    tryCatch({
      
      log_msg("[", yr, " ", ii, "/", length(year_files), "] Reading CONUS: ", f)
      
      conus <- arrow::read_parquet(f)
      
      required_base <- c("lat", "lon", "hrap_x", "hrap_y", "grib_id")
      missing_base <- setdiff(required_base, names(conus))
      
      if (length(missing_base) > 0) {
        stop("CONUS missing required columns: ", paste(missing_base, collapse = ", "))
      }
      
      rain_col <- detect_rain_col(conus, cycle_eff)
      
      # Preserve CONUS character formatting for lat/lon when possible.
      # If CONUS is already character, this does not force decimal places.
      # If CONUS is numeric, as.character() is used as fallback.
      conus2 <- conus %>%
        transmute(
          cycle = as.character(cycle_eff),
          lat = as.character(lat),
          lon = as.character(lon),
          hrap_x = as.integer(hrap_x),
          hrap_y = as.integer(hrap_y),
          grib_id = as.integer(grib_id),
          rain_mm = as.numeric(.data[[rain_col]])
        )
      
      # ======================================================
      # A) AOI precip subset: boundary mask join
      # ======================================================
      
      joined <- conus2 %>%
        inner_join(mask, by = "grib_id", suffix = c("", "_mask"))
      
      if (nrow(joined) == 0) {
        stop("AOI boundary join returned zero rows for ", as.character(d))
      }
      
      mismatch_n <- joined %>%
        filter(hrap_x != hrap_x_mask | hrap_y != hrap_y_mask) %>%
        nrow()
      
      if (mismatch_n > 0) {
        msg <- paste0("HRAP mismatch after boundary join: ", mismatch_n, " rows on ", as.character(d))
        if (isTRUE(cfg$strict_join)) stop(msg) else log_msg(msg, level = "WARN")
      }
      
      aoi <- joined %>%
        transmute(
          cycle = as.character(cycle),
          lat = as.character(lat),
          lon = as.character(lon),
          hrap_x = as.integer(hrap_x),
          hrap_y = as.integer(hrap_y),
          grib_id = as.integer(grib_id),
          bin_area = as.numeric(bin_area),
          rain_mm = as.numeric(rain_mm)
        ) %>%
        select(all_of(precip_cols))
      
      if (isTRUE(cfg$validate_before_write)) validate_precip_df(aoi)
      
      if (isTRUE(cfg$write_precip) && !(isTRUE(cfg$skip_existing) && file.exists(out_precip))) {
        atomic_write_parquet(aoi, out_precip, compression = "zstd")
        if (file.info(out_precip)$size < 10 * 1024) stop("Precip parquet too small: ", out_precip)
      }
      
      # ======================================================
      # B) Daily stats: vol-calcs join
      # ======================================================
      
      rain_df <- conus2 %>%
        transmute(
          grib_id = as.integer(grib_id),
          hrap_x = as.integer(hrap_x),
          hrap_y = as.integer(hrap_y),
          rain_mm = as.numeric(rain_mm)
        )
      
      stats_join <- rain_df %>%
        inner_join(vol_masks, by = "grib_id", suffix = c("", "_vol"))
      
      if (nrow(stats_join) == 0) {
        stop("Vol-calcs join returned zero rows for ", as.character(d))
      }
      
      mismatch2 <- stats_join %>%
        filter(hrap_x != hrap_x_vol | hrap_y != hrap_y_vol) %>%
        nrow()
      
      if (mismatch2 > 0) {
        msg2 <- paste0("HRAP mismatch after vol-calcs join: ", mismatch2, " rows on ", as.character(d))
        if (isTRUE(cfg$strict_volcalc_join)) stop(msg2) else log_msg(msg2, level = "WARN")
      }
      
      daily_stats <- stats_join %>%
        mutate(
          rain_mm0 = ifelse(is.na(rain_mm), 0, rain_mm),
          vol_m3_bin = (rain_mm0 / 1000) * bin_area
        ) %>%
        group_by(area_name) %>%
        summarise(
          area_id = cfg$area_id,
          cycle_eff = cycle_eff,
          date_utc = as.character(d),
          
          n_bins = as.integer(dplyr::n()),
          area_m2 = sum(bin_area, na.rm = TRUE),
          vol_m3 = sum(vol_m3_bin, na.rm = TRUE),
          basin_avg_mm = (vol_m3 / area_m2) * 1000,
          max_bin_mm = max(rain_mm0, na.rm = TRUE),
          
          pct_area_gt_2p54mm  = 100 * sum(bin_area[rain_mm0 > 2.54],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_6p35mm  = 100 * sum(bin_area[rain_mm0 > 6.35],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_12p7mm  = 100 * sum(bin_area[rain_mm0 > 12.7],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_19p05mm = 100 * sum(bin_area[rain_mm0 > 19.05], na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_25p4mm  = 100 * sum(bin_area[rain_mm0 > 25.4],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_31p75mm = 100 * sum(bin_area[rain_mm0 > 31.75], na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          pct_area_gt_38p1mm  = 100 * sum(bin_area[rain_mm0 > 38.1],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE),
          
          .groups = "drop"
        ) %>%
        mutate(
          area_id = as.character(area_id),
          area_name = as.character(area_name),
          cycle_eff = as.character(cycle_eff),
          date_utc = as.character(date_utc),
          
          n_bins = as.integer(n_bins),
          area_m2 = as.numeric(area_m2),
          vol_m3 = as.numeric(vol_m3),
          basin_avg_mm = as.numeric(basin_avg_mm),
          max_bin_mm = as.numeric(max_bin_mm),
          
          pct_area_gt_2p54mm = as.numeric(pct_area_gt_2p54mm),
          pct_area_gt_6p35mm = as.numeric(pct_area_gt_6p35mm),
          pct_area_gt_12p7mm = as.numeric(pct_area_gt_12p7mm),
          pct_area_gt_19p05mm = as.numeric(pct_area_gt_19p05mm),
          pct_area_gt_25p4mm = as.numeric(pct_area_gt_25p4mm),
          pct_area_gt_31p75mm = as.numeric(pct_area_gt_31p75mm),
          pct_area_gt_38p1mm = as.numeric(pct_area_gt_38p1mm)
        ) %>%
        select(all_of(daily_cols)) %>%
        arrange(area_name)
      
      if (isTRUE(cfg$validate_before_write)) validate_daily_df(daily_stats)
      
      if (isTRUE(cfg$write_daily_stats) && !(isTRUE(cfg$skip_existing) && file.exists(out_daily))) {
        atomic_write_parquet(daily_stats, out_daily, compression = "zstd")
        if (file.info(out_daily)$size < 1024) stop("Daily stats parquet too small: ", out_daily)
      }
      
      # Mark this date processed after daily precip/stats were successfully built.
      processed_dates <- sort(unique(c(processed_dates, d)))
      
      days_present <- as.integer(length(processed_dates))
      days_expected <- as.integer(length(seq.Date(year_start, d, by = "day")))
      days_missing <- as.integer(days_expected - days_present)
      
      # ======================================================
      # C) derived_ytd_precip: year reset, S3-style schema
      # ======================================================
      
      day_cells <- aoi %>%
        transmute(
          grib_id = as.integer(grib_id),
          hrap_x = as.integer(hrap_x),
          hrap_y = as.integer(hrap_y),
          lat = as.numeric(lat),
          lon = as.numeric(lon),
          rain_mm = as.numeric(rain_mm)
        )
      
      if (is.null(cum_ytd)) {
        cum_ytd <- day_cells %>%
          transmute(
            grib_id = as.integer(grib_id),
            rain_ytd_mm = ifelse(is.na(rain_mm), 0, rain_mm)
          )
      } else {
        cum_ytd <- cum_ytd %>%
          full_join(
            day_cells %>%
              transmute(
                grib_id = as.integer(grib_id),
                add = ifelse(is.na(rain_mm), 0, rain_mm)
              ),
            by = "grib_id"
          ) %>%
          mutate(
            rain_ytd_mm = ifelse(is.na(rain_ytd_mm), 0, rain_ytd_mm) + ifelse(is.na(add), 0, add)
          ) %>%
          select(grib_id, rain_ytd_mm)
      }
      
      ytd_cells <- day_cells %>%
        select(lat, lon, hrap_x, hrap_y, grib_id) %>%
        left_join(cum_ytd, by = "grib_id") %>%
        mutate(
          lat = as.numeric(lat),
          lon = as.numeric(lon),
          hrap_x = as.integer(hrap_x),
          hrap_y = as.integer(hrap_y),
          grib_id = as.integer(grib_id),
          rain_ytd_mm = as.numeric(rain_ytd_mm),
          year = as.integer(yr),
          thru_date_utc = as.character(d),
          days_present = as.integer(days_present),
          days_expected = as.integer(days_expected),
          days_missing = as.integer(days_missing)
        ) %>%
        select(all_of(dytd_cols)) %>%
        arrange(grib_id)
      
      if (isTRUE(cfg$validate_before_write)) validate_dytd_df(ytd_cells)
      
      if (isTRUE(cfg$write_derived_ytd_precip) && !(isTRUE(cfg$skip_existing) && file.exists(out_dytd))) {
        atomic_write_parquet(ytd_cells, out_dytd, compression = "zstd")
        if (file.info(out_dytd)$size < 1024) stop("derived_ytd_precip parquet too small: ", out_dytd)
      }
      
      # ======================================================
      # D) stats/ytd: year reset, S3-style schema
      # ======================================================
      
      daily_vol <- daily_stats %>%
        transmute(
          area_name = as.character(area_name),
          area_m2 = as.numeric(area_m2),
          vol_m3 = as.numeric(vol_m3)
        )
      
      if (is.null(cum_vol)) {
        cum_vol <- daily_vol %>%
          transmute(
            area_name = as.character(area_name),
            area_m2 = as.numeric(area_m2),
            ytd_vol_m3 = as.numeric(vol_m3)
          )
      } else {
        cum_vol <- cum_vol %>%
          full_join(
            daily_vol %>%
              transmute(
                area_name = as.character(area_name),
                area_m2_new = as.numeric(area_m2),
                add_vol = as.numeric(vol_m3)
              ),
            by = "area_name"
          ) %>%
          mutate(
            area_m2 = ifelse(is.na(area_m2), area_m2_new, area_m2),
            ytd_vol_m3 = ifelse(is.na(ytd_vol_m3), 0, ytd_vol_m3) + ifelse(is.na(add_vol), 0, add_vol)
          ) %>%
          select(area_name, area_m2, ytd_vol_m3)
      }
      
      ytd_stats <- cum_vol %>%
        mutate(
          area_id = as.character(cfg$area_id),
          year = as.character(yr),
          days_present = as.integer(days_present),
          days_expected = as.integer(days_expected),
          days_missing = as.integer(days_missing),
          area_m2 = as.numeric(area_m2),
          ytd_vol_m3 = as.numeric(ytd_vol_m3),
          ytd_avg_mm = as.numeric((ytd_vol_m3 / area_m2) * 1000)
        ) %>%
        select(all_of(ytd_cols)) %>%
        arrange(area_name)
      
      if (isTRUE(cfg$validate_before_write)) validate_ytd_df(ytd_stats)
      
      if (isTRUE(cfg$write_ytd_stats) && !(isTRUE(cfg$skip_existing) && file.exists(out_ytd))) {
        atomic_write_parquet(ytd_stats, out_ytd, compression = "zstd")
        if (file.info(out_ytd)$size < 1024) stop("YTD stats parquet too small: ", out_ytd)
      }
      
      processed_output[[length(processed_output) + 1]] <- data.frame(
        date = as.character(d),
        cycle_eff = cycle_eff,
        precip = out_precip,
        daily = out_daily,
        dytd = out_dytd,
        ytd = out_ytd,
        stringsAsFactors = FALSE
      )
      
      log_msg(
        "OK ", as.character(d),
        " | days_present=", days_present,
        " days_expected=", days_expected,
        " days_missing=", days_missing
      )
      
    }, error = function(e) {
      msg <- paste0(as.character(d), " | ", conditionMessage(e))
      failed_dates <<- c(failed_dates, msg)
      log_msg("FAILED ", msg, level = "ERROR")
    })
  }
  
  log_msg("YEAR DONE: ", yr, " (input days=", length(year_files), ")")
}


# ============================================================
# FINAL SUMMARY
# ============================================================

log_msg("DONE. Local AOI archive written under: ", cfg$out_root)

if (length(failed_dates) > 0) {
  log_msg("FAILED dates: ", length(failed_dates), level = "ERROR")
  print(failed_dates)
  stop("Backfill finished with failures. Review failed_dates above.")
} else {
  log_msg("No failed dates.")
}

# Optional final spot check.
spot_files <- list(
  precip = out_day_path(cfg$out_precip_root, max(conus_dates)),
  daily  = out_day_path(cfg$out_daily_root,  max(conus_dates)),
  dytd   = out_day_path(cfg$out_dytd_root,   max(conus_dates)),
  ytd    = out_day_path(cfg$out_ytd_root,    max(conus_dates))
)

log_msg("Final spot-check schemas:")

for (nm in names(spot_files)) {
  if (file.exists(spot_files[[nm]])) {
    log_msg("Schema for ", nm, ": ", spot_files[[nm]])
    print(arrow::schema(arrow::read_parquet(spot_files[[nm]])))
  }
}
