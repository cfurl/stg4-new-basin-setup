# backfill_aoi_cum_precip_pipeline_schema_test3.R
# FULL DROP-IN for AOI local backfill testing
# Purpose: write labatt archive outputs using the same column names/order/types as the current pipeline
# Default config is TEST MODE: 2002-01-01 through 2002-01-03, max_days_total = 3, skip_existing = FALSE

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(paws)
  library(arrow)
})

# ============================================================
# MASTER CONFIG (EDIT ONLY THIS BLOCK)
# ============================================================
AREA_ID <- "labatt"   # <--- change ONLY this

cfg <- list(
  area_id = AREA_ID,
  cycle_hour = "12",
  
  # Local CONUS parquet repository (input)
  conus_local_root = "F:/conus_archive_build/stg4_24hr_conus_archive/parquet",
  
  # IMPORTANT: use "F:" (not "F:/") to avoid "F://..."
  out_base = "F:",
  
  renviron_path = ".Renviron",
  aws_region = "us-east-2",
  
  pipeline_bucket = "stg4-24hr-aws-pipeline",
  aoi_config_prefix = "CONUS_subset/config/aoi",
  
  date_start = as.Date(NA),
  date_end   = as.Date(NA),
  
  # TEST MODE: process only three files first.
  # For full archive build, set date_start/date_end as needed and max_days_total = Inf.
  max_days_total = Inf,
  
  # FALSE for schema repair/testing so bad old local outputs are overwritten.
  skip_existing = FALSE,
  seed_ytd_on_skip = TRUE,
  strict_join = TRUE,
  strict_volcalc_join = TRUE,
  
  write_precip = TRUE,
  write_daily_stats = TRUE,
  write_derived_ytd_precip = TRUE,
  write_ytd_stats = TRUE,
  
  # Stop immediately if any written parquet does not match pipeline schema.
  fail_on_schema_mismatch = TRUE
)
# ============================================================

# ============================================================
# Local path normalizer (prevents F://...)
# ============================================================
fix_drive_slash <- function(p) {
  p <- gsub("\\\\", "/", p)
  # collapse "F://something" -> "F:/something"
  p <- gsub("^([A-Za-z]):/+", "\\1:/", p)
  p
}

safe_mkdir <- function(p) {
  p <- fix_drive_slash(p)
  dir.create(p, recursive = TRUE, showWarnings = FALSE)
  
  if (!dir.exists(p)) {
    drive <- paste0(substr(p, 1, 2), "/")
    msg <- paste0(
      "Failed to create directory: ", p, "\n",
      "Preflight:\n",
      "  Sys.info()[sysname] = ", Sys.info()[["sysname"]], "\n",
      "  drive exists? file.exists('", drive, "') = ", file.exists(drive), "\n",
      "  can create test dir? try dir.create('", drive, "__r_test__')\n",
      "\n",
      "If you're running inside WSL/Linux/Docker, you cannot use F:/ paths.\n",
      "Use a mounted path like /mnt/f/... (WSL) or a Docker mount path."
    )
    stop(msg)
  }
  
  invisible(TRUE)
}

ensure_dir_for_file <- function(p) safe_mkdir(dirname(fix_drive_slash(p)))

# ============================================================
# Derived paths (do NOT edit)
# ============================================================
cfg$out_root <- fix_drive_slash(file.path(cfg$out_base, paste0(cfg$area_id, "_archive_build")))

cfg$boundary_mask_uri <- sprintf(
  "s3://%s/%s/%s/assets/%s-boundary-mask.parquet",
  cfg$pipeline_bucket, cfg$aoi_config_prefix, cfg$area_id, cfg$area_id
)
cfg$area_vol_calcs_uri <- sprintf(
  "s3://%s/%s/%s/assets/%s-area-vol-calc-masks.parquet",
  cfg$pipeline_bucket, cfg$aoi_config_prefix, cfg$area_id, cfg$area_id
)

cfg$cache_dir <- fix_drive_slash(file.path(cfg$out_root, "_cache"))
cfg$out_precip_root <- fix_drive_slash(file.path(cfg$out_root, "precip", "precip_parquet"))
cfg$out_daily_root  <- fix_drive_slash(file.path(cfg$out_root, "stats", "daily"))
cfg$out_ytd_root    <- fix_drive_slash(file.path(cfg$out_root, "stats", "ytd"))
cfg$out_dytd_root   <- fix_drive_slash(file.path(cfg$out_root, "derived_ytd_precip"))

# ---------------- ADDED: QA folder + run_id ----------------
cfg$qa_dir <- fix_drive_slash(file.path(cfg$out_root, "qa"))
run_id <- format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")
qa_schema_check_csv <- fix_drive_slash(file.path(cfg$qa_dir, paste0("qa_schema_checks_", cfg$area_id, "_", run_id, ".csv")))
# -----------------------------------------------------------

# Create ALL folders up front
safe_mkdir(cfg$out_root)
safe_mkdir(cfg$cache_dir)
safe_mkdir(cfg$out_precip_root)
safe_mkdir(cfg$out_daily_root)
safe_mkdir(cfg$out_ytd_root)
safe_mkdir(cfg$out_dytd_root)

# ---------------- ADDED: create QA dir up front -------------
safe_mkdir(cfg$qa_dir)
# -----------------------------------------------------------

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
  local_path <- fix_drive_slash(local_path)
  ensure_dir_for_file(local_path)
  
  if (file.exists(local_path) && file.info(local_path)$size > 0) {
    log_msg("Cache hit: ", local_path)
    return(local_path)
  }
  
  p <- parse_s3_uri(s3_uri)
  log_msg("Downloading asset: ", s3_uri)
  
  obj <- s3$get_object(Bucket = p$bucket, Key = p$key)
  
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
  fix_drive_slash(file.path(
    root,
    paste0("year=",  format(d, "%Y")),
    paste0("month=", format(d, "%m")),
    paste0("day=",   format(d, "%d")),
    "part-0.parquet"
  ))
}


# ============================================================
# Pipeline schema enforcement + write-time QA
# ============================================================
expected_schemas <- list(
  precip_parquet = data.frame(
    expected_position = 1:8,
    col_name = c("cycle", "lat", "lon", "hrap_x", "hrap_y", "grib_id", "bin_area", "rain_mm"),
    expected_type = c("character", "character", "character", "integer", "integer", "integer", "numeric", "numeric"),
    stringsAsFactors = FALSE
  ),
  derived_ytd_precip = data.frame(
    expected_position = 1:11,
    col_name = c("lat", "lon", "hrap_x", "hrap_y", "grib_id", "rain_ytd_mm", "year", "thru_date_utc", "days_present", "days_expected", "days_missing"),
    expected_type = c("numeric", "numeric", "integer", "integer", "integer", "numeric", "integer", "character", "integer", "integer", "integer"),
    stringsAsFactors = FALSE
  ),
  stats_daily = data.frame(
    expected_position = 1:16,
    col_name = c(
      "area_id", "area_name", "cycle_eff", "date_utc", "n_bins", "area_m2", "vol_m3", "basin_avg_mm", "max_bin_mm",
      "pct_area_gt_2p54mm", "pct_area_gt_6p35mm", "pct_area_gt_12p7mm", "pct_area_gt_19p05mm", "pct_area_gt_25p4mm", "pct_area_gt_31p75mm", "pct_area_gt_38p1mm"
    ),
    expected_type = c("character", "character", "character", "character", "integer", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
    stringsAsFactors = FALSE
  ),
  stats_ytd = data.frame(
    expected_position = 1:9,
    col_name = c("area_id", "area_name", "year", "days_present", "days_expected", "days_missing", "area_m2", "ytd_vol_m3", "ytd_avg_mm"),
    expected_type = c("character", "character", "integer", "integer", "integer", "integer", "numeric", "numeric", "numeric"),
    stringsAsFactors = FALSE
  )
)

class_to_simple <- function(x) {
  if (inherits(x, "integer")) return("integer")
  if (inherits(x, "numeric")) return("numeric")
  if (inherits(x, "character")) return("character")
  if (inherits(x, "logical")) return("logical")
  if (inherits(x, "Date")) return("Date")
  paste(class(x), collapse = "/")
}

schema_check_rows <- list()

compare_df_to_expected_schema <- function(df, dataset, file_path) {
  if (!dataset %in% names(expected_schemas)) stop("No expected schema registered for dataset: ", dataset)
  expected <- expected_schemas[[dataset]]
  actual <- data.frame(
    actual_position = seq_along(names(df)),
    col_name = names(df),
    actual_type = vapply(df, class_to_simple, character(1)),
    stringsAsFactors = FALSE
  )

  cmp <- dplyr::full_join(expected, actual, by = "col_name") %>%
    dplyr::mutate(
      area_id = cfg$area_id,
      dataset = dataset,
      file_path = file_path,
      checked_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
      status = dplyr::case_when(
        is.na(actual_position) ~ "MISSING_IN_FILE",
        is.na(expected_position) ~ "EXTRA_IN_FILE",
        expected_type != actual_type ~ "TYPE_MISMATCH",
        expected_position != actual_position ~ "ORDER_MISMATCH",
        TRUE ~ "OK"
      )
    ) %>%
    dplyr::arrange(dplyr::coalesce(expected_position, actual_position)) %>%
    dplyr::select(area_id, dataset, file_path, checked_utc, expected_position, actual_position,
                  col_name, expected_type, actual_type, status)

  cmp
}

record_schema_check <- function(dataset, file_path) {
  df <- arrow::read_parquet(file_path)
  cmp <- compare_df_to_expected_schema(df, dataset, file_path)
  schema_check_rows[[length(schema_check_rows) + 1]] <<- cmp

  # Persist every time so you still get QA evidence even if the next file fails.
  all_checks <- dplyr::bind_rows(schema_check_rows)
  write.csv(all_checks, qa_schema_check_csv, row.names = FALSE)

  bad <- cmp %>% dplyr::filter(status != "OK")
  if (nrow(bad) > 0) {
    log_msg("SCHEMA FAIL for ", dataset, " at ", file_path, " - see ", qa_schema_check_csv, level = "ERROR")
    print(as.data.frame(bad), row.names = FALSE)
    if (isTRUE(cfg$fail_on_schema_mismatch)) {
      stop("Schema check failed for ", dataset, ": ", file_path)
    }
  } else {
    log_msg("SCHEMA PASS for ", dataset, ": ", file_path)
  }

  invisible(cmp)
}

coerce_precip_schema <- function(df) {
  # Pipeline distinction: precip_parquet stores lat/lon as character/string,
  # while derived_ytd_precip stores lat/lon as numeric/double.
  df %>%
    dplyr::transmute(
      cycle    = as.character(cycle),
      lat      = as.character(lat),
      lon      = as.character(lon),
      hrap_x   = as.integer(hrap_x),
      hrap_y   = as.integer(hrap_y),
      grib_id  = as.integer(grib_id),
      bin_area = as.numeric(bin_area),
      rain_mm  = as.numeric(rain_mm)
    )
}

coerce_daily_stats_schema <- function(df) {
  df %>%
    dplyr::transmute(
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
    )
}

coerce_derived_ytd_schema <- function(df) {
  df %>%
    dplyr::transmute(
      lat = as.numeric(lat),
      lon = as.numeric(lon),
      hrap_x = as.integer(hrap_x),
      hrap_y = as.integer(hrap_y),
      grib_id = as.integer(grib_id),
      rain_ytd_mm = as.numeric(rain_ytd_mm),
      year = as.integer(year),
      thru_date_utc = as.character(thru_date_utc),
      days_present = as.integer(days_present),
      days_expected = as.integer(days_expected),
      days_missing = as.integer(days_missing)
    )
}

coerce_ytd_stats_schema <- function(df) {
  df %>%
    dplyr::transmute(
      area_id = as.character(area_id),
      area_name = as.character(area_name),
      year = as.integer(year),
      days_present = as.integer(days_present),
      days_expected = as.integer(days_expected),
      days_missing = as.integer(days_missing),
      area_m2 = as.numeric(area_m2),
      ytd_vol_m3 = as.numeric(ytd_vol_m3),
      ytd_avg_mm = as.numeric(ytd_avg_mm)
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
mask_cache <- fix_drive_slash(file.path(cfg$cache_dir, paste0(cfg$area_id, "-boundary-mask.parquet")))
vol_cache  <- fix_drive_slash(file.path(cfg$cache_dir, paste0(cfg$area_id, "-area-vol-calc-masks.parquet")))

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
log_msg("Vol-mask rows: ", nrow(vol_masks), " areas=", dplyr::n_distinct(vol_masks$area_name))

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
                    hrap_x = as.integer(hrap_x),
                    hrap_y = as.integer(hrap_y),
                    lat = as.numeric(lat),
                    lon = as.numeric(lon),
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
      mutate(cycle = cycle_eff,
             rain_mm = .data[[rain_col]]) %>%
      select(-all_of(rain_col))
    
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
        lat      = lat,
        lon      = lon,
        hrap_x   = hrap_x,
        hrap_y   = hrap_y,
        grib_id  = grib_id,
        bin_area = bin_area,
        rain_mm  = rain_mm
      ) %>%
      coerce_precip_schema()
    
    if (nrow(aoi) == 0) next
    
    if (isTRUE(cfg$write_precip)) {
      ensure_dir_for_file(out_precip)
      arrow::write_parquet(aoi, out_precip, compression="zstd")
      record_schema_check("precip_parquet", out_precip)
    }
    
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

        # Match the current pipeline stats/daily schema exactly.
        pct_area_gt_2p54mm  = 100 * (sum(bin_area[rain_mm0 > 2.54],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_6p35mm  = 100 * (sum(bin_area[rain_mm0 > 6.35],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_12p7mm  = 100 * (sum(bin_area[rain_mm0 > 12.7],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_19p05mm = 100 * (sum(bin_area[rain_mm0 > 19.05], na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_25p4mm  = 100 * (sum(bin_area[rain_mm0 > 25.4],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_31p75mm = 100 * (sum(bin_area[rain_mm0 > 31.75], na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),
        pct_area_gt_38p1mm  = 100 * (sum(bin_area[rain_mm0 > 38.1],  na.rm = TRUE) / sum(bin_area, na.rm = TRUE)),

        n_bins    = dplyr::n(),
        .groups = "drop"
      ) %>%
      coerce_daily_stats_schema()
    
    if (isTRUE(cfg$write_daily_stats)) {
      ensure_dir_for_file(out_daily)
      arrow::write_parquet(daily_stats, out_daily, compression="zstd")
      record_schema_check("stats_daily", out_daily)
    }
    
    processed_dates <- sort(unique(c(processed_dates, d)))
    days_present  <- length(processed_dates)
    days_expected <- length(seq.Date(year_start, d, by="day"))
    days_missing  <- days_expected - days_present
    
    if (isTRUE(cfg$write_derived_ytd_precip)) {
      day_rain <- aoi %>%
        transmute(
          grib_id = as.integer(grib_id),
          hrap_x  = as.integer(hrap_x),
          hrap_y  = as.integer(hrap_y),
          lat     = as.numeric(lat),
          lon     = as.numeric(lon),
          rain_mm = as.numeric(rain_mm)
        )

      day_add <- day_rain %>%
        transmute(
          grib_id,
          hrap_x,
          hrap_y,
          lat,
          lon,
          add = ifelse(is.na(rain_mm), 0, rain_mm)
        )
      
      if (is.null(cum_ytd)) {
        cum_ytd <- day_add %>%
          transmute(grib_id, hrap_x, hrap_y, lat, lon, rain_ytd_mm = add)
      } else {
        cum_ytd <- cum_ytd %>%
          full_join(day_add, by="grib_id", suffix=c("", "_new")) %>%
          mutate(
            hrap_x = dplyr::coalesce(hrap_x, hrap_x_new),
            hrap_y = dplyr::coalesce(hrap_y, hrap_y_new),
            lat = dplyr::coalesce(lat, lat_new),
            lon = dplyr::coalesce(lon, lon_new),
            rain_ytd_mm = ifelse(is.na(rain_ytd_mm), 0, rain_ytd_mm) + ifelse(is.na(add), 0, add)
          ) %>%
          select(grib_id, hrap_x, hrap_y, lat, lon, rain_ytd_mm)
      }
      
      ytd_cells <- cum_ytd %>%
        mutate(
          year = as.integer(yr),
          thru_date_utc = as.character(d),
          days_present = as.integer(days_present),
          days_expected = as.integer(days_expected),
          days_missing = as.integer(days_missing)
        ) %>%
        coerce_derived_ytd_schema()
      
      ensure_dir_for_file(out_dytd)
      arrow::write_parquet(ytd_cells, out_dytd, compression="zstd")
      record_schema_check("derived_ytd_precip", out_dytd)
    }
    
    if (isTRUE(cfg$write_ytd_stats)) {
      daily_vol <- daily_stats %>%
        transmute(area_name=as.character(area_name), area_m2=as.numeric(area_m2), vol_m3=as.numeric(vol_m3))
      
      if (is.null(cum_vol)) {
        cum_vol <- daily_vol %>% transmute(area_name, area_m2, ytd_vol_m3 = vol_m3)
      } else {
        cum_vol <- cum_vol %>%
          full_join(daily_vol %>% transmute(area_name, area_m2_new=area_m2, add_vol=vol_m3), by="area_name") %>%
          mutate(area_m2 = ifelse(is.na(area_m2), area_m2_new, area_m2),
                 ytd_vol_m3 = ifelse(is.na(ytd_vol_m3), 0, ytd_vol_m3) + ifelse(is.na(add_vol), 0, add_vol)) %>%
          select(area_name, area_m2, ytd_vol_m3)
      }
      
      ytd_stats <- cum_vol %>%
        mutate(area_id = cfg$area_id, year = as.integer(yr),
               days_present=as.integer(days_present), days_expected=as.integer(days_expected), days_missing=as.integer(days_missing),
               ytd_avg_mm = (ytd_vol_m3 / area_m2) * 1000) %>%
        coerce_ytd_stats_schema()
      
      ensure_dir_for_file(out_ytd)
      arrow::write_parquet(ytd_stats, out_ytd, compression="zstd")
      record_schema_check("stats_ytd", out_ytd)
    }
    
    log_msg("OK ", as.character(d), " | days_present=", days_present, " days_expected=", days_expected, " days_missing=", days_missing)
  }
  
  log_msg("YEAR DONE: ", yr)
}

# ============================================================
# --------------------------- ADDED QA ------------------------
# Writes:
#  - qa_file_counts_<area_id>_<run_id>.csv
#  - qa_parquet_schemas_<area_id>_<run_id>.csv
#  - qa_schema_checks_<area_id>_<run_id>.csv
#  - qa_summary_<area_id>_<run_id>.txt
#  - missing_dates_<dataset>_year=<YYYY>_<run_id>.txt (if missing)
# ============================================================

schema_from_parquet <- function(p) {
  df <- arrow::read_parquet(p)
  data.frame(
    col_name = names(df),
    col_type = vapply(df, class_to_simple, character(1)),
    stringsAsFactors = FALSE
  )
}

pick_first_parquet <- function(root) {
  files <- list.files(root, pattern="part-0\\.parquet$", recursive=TRUE, full.names=TRUE)
  if (length(files) == 0) return(NA_character_)
  fix_drive_slash(files[1])
}

# Ensure qa dir exists (defensive)
safe_mkdir(cfg$qa_dir)

qa_counts_csv  <- fix_drive_slash(file.path(cfg$qa_dir, paste0("qa_file_counts_", cfg$area_id, "_", run_id, ".csv")))
qa_schema_csv  <- fix_drive_slash(file.path(cfg$qa_dir, paste0("qa_parquet_schemas_", cfg$area_id, "_", run_id, ".csv")))
qa_summary_txt <- fix_drive_slash(file.path(cfg$qa_dir, paste0("qa_summary_", cfg$area_id, "_", run_id, ".txt")))

ds_roots <- list(
  precip_parquet     = cfg$out_precip_root,
  derived_ytd_precip = cfg$out_dytd_root,
  stats_daily        = cfg$out_daily_root,
  stats_ytd          = cfg$out_ytd_root
)

years_all <- sort(unique(format(conus_dates, "%Y")))
counts_rows <- list()

for (yr in years_all) {
  yr_dates <- conus_dates[format(conus_dates, "%Y") == yr]
  if (length(yr_dates) == 0) next
  
  # expected = Jan 1 through last CONUS date processed for that year
  start_expected <- as.Date(paste0(yr, "-01-01"))
  end_expected   <- max(yr_dates)
  expected_dates <- seq.Date(start_expected, end_expected, by="day")
  expected_n <- length(expected_dates)
  
  for (ds in names(ds_roots)) {
    root <- ds_roots[[ds]]
    present_flags <- vapply(expected_dates, function(d) file.exists(out_day_path(root, d)), logical(1))
    present_n <- sum(present_flags)
    miss_n <- expected_n - present_n
    
    counts_rows[[length(counts_rows) + 1]] <- data.frame(
      area_id = cfg$area_id,
      dataset = ds,
      year = as.integer(yr),
      start_expected = as.character(start_expected),
      end_expected = as.character(end_expected),
      expected_files = expected_n,
      present_files = present_n,
      missing_files = miss_n,
      pct_present = if (expected_n == 0) NA_real_ else round(100 * present_n / expected_n, 2),
      stringsAsFactors = FALSE
    )
    
    if (miss_n > 0) {
      miss_path <- fix_drive_slash(file.path(cfg$qa_dir, paste0("missing_dates_", ds, "_year=", yr, "_", run_id, ".txt")))
      writeLines(as.character(expected_dates[!present_flags]), miss_path)
    }
  }
}

counts_df <- if (length(counts_rows) == 0) {
  data.frame(area_id=character(), dataset=character(), year=integer(),
             start_expected=character(), end_expected=character(),
             expected_files=integer(), present_files=integer(),
             missing_files=integer(), pct_present=double(),
             stringsAsFactors = FALSE)
} else {
  do.call(rbind, counts_rows)
}
write.csv(counts_df, qa_counts_csv, row.names = FALSE)

schema_rows <- list()

# assets (cached)
if (file.exists(mask_cache)) {
  schema_rows[[length(schema_rows) + 1]] <- data.frame(
    item = "boundary_mask_asset",
    sample_file = mask_cache,
    schema_from_parquet(mask_cache),
    stringsAsFactors = FALSE
  )
}
if (file.exists(vol_cache)) {
  schema_rows[[length(schema_rows) + 1]] <- data.frame(
    item = "area_vol_calcs_asset",
    sample_file = vol_cache,
    schema_from_parquet(vol_cache),
    stringsAsFactors = FALSE
  )
}

# representative parquet per dataset
for (ds in names(ds_roots)) {
  sample <- pick_first_parquet(ds_roots[[ds]])
  if (is.na(sample)) next
  schema_rows[[length(schema_rows) + 1]] <- data.frame(
    item = paste0("dataset_", ds),
    sample_file = sample,
    schema_from_parquet(sample),
    stringsAsFactors = FALSE
  )
}

schema_df <- if (length(schema_rows) == 0) {
  data.frame(item=character(), sample_file=character(), col_name=character(), col_type=character(),
             stringsAsFactors = FALSE)
} else {
  do.call(rbind, schema_rows)
}
write.csv(schema_df, qa_schema_csv, row.names = FALSE)

writeLines(c(
  paste0("area_id: ", cfg$area_id),
  paste0("run_id: ", run_id),
  paste0("out_root: ", cfg$out_root),
  "",
  "QA outputs:",
  paste0("  - ", qa_counts_csv),
  paste0("  - ", qa_schema_csv),
  paste0("  - ", qa_schema_check_csv),
  paste0("  - ", qa_summary_txt),
  "",
  "Notes:",
  "  - expected_files per year = Jan 1 .. last CONUS day processed for that year in this run.",
  "  - missing_dates_*.txt written only when missing_files > 0."
), qa_summary_txt)

log_msg("QA written: ", qa_counts_csv)
log_msg("QA written: ", qa_schema_csv)
log_msg("DONE. out_root=", cfg$out_root)