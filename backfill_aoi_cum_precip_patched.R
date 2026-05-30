# backfill_aoi_cum_precip_patched.R  (DROP-IN, GENERIC CONFIG AT TOP + QA)
#
# Local backfill for ANY AOI:
#   - Reads CONUS parquet from a local canonical archive (no S3 CONUS reads)
#   - Downloads AOI boundary mask + area-vol-calc masks ONCE from S3 (cached locally)
#   - Writes AOI precip + daily stats + derived_ytd_precip + ytd stats to local disk
#   - YTD RESETS each Jan 1 (days_present will not exceed 365/366)
#
# NEW:
#   - Writes QA outputs under: <out_root>/qa/
#     * qa_file_counts_<area_id>_<run_id>.csv  (expected vs present per year)
#     * missing_dates_<dataset>_year=<YYYY>_<run_id>.txt  (only when missing)
#     * qa_parquet_schemas_<area_id>_<run_id>.csv (assets + one file per dataset)
#     * qa_summary_<area_id>_<run_id>.txt

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
  area_id = "labatt",
  cycle_hour = "12",   # 24h product uses 12Z
  
  # Local CONUS parquet repository (input)
  # expects: .../year=YYYY/month=MM/day=DD/part-0.parquet
  conus_local_root = "F:/conus_archive_build/stg4_24hr_conus_archive/parquet",
  
  # Local output root (writes to precip/, stats/, derived_ytd_precip/)
  out_root = "F:/texas_mrb_archive_build",
  
  # S3 asset URIs (downloaded once; cached under out_root/_cache/)
  boundary_mask_uri = "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/labatt/assets/labatt-boundary-mask.parquet",
  area_vol_calcs_uri = "s3://stg4-24hr-aws-pipeline/CONUS_subset/config/aoi/labatt/assets/labatt-area-vol-calc-masks.parquet",
  
  # AWS creds (only used to download the two assets above)
  renviron_path = ".Renviron",
  aws_region = "us-east-2",
  
  # Optional local processing window (inclusive). Use NA to ignore.
  date_start = as.Date(NA),   # e.g., as.Date("2002-01-01")
  date_end   = as.Date(NA),   # e.g., as.Date("2026-05-03")
  
  # Safety: process only first N days after filtering/sorting. Set Inf to unleash.
  max_days_total = 3,
  
  # Behavior toggles
  skip_existing = TRUE,        # if outputs exist for a day, skip that day
  strict_join = TRUE,          # boundary mask HRAP mismatch => stop
  strict_volcalc_join = TRUE,  # vol-calcs HRAP mismatch => stop
  
  # What to write
  write_precip = TRUE,
  write_daily_stats = TRUE,
  write_derived_ytd_precip = TRUE,
  write_ytd_stats = TRUE
)
# ============================================================

# -----------------------------
# Derived paths
# -----------------------------
cfg$cache_dir <- file.path(cfg$out_root, "_cache")

cfg$out_precip_root <- file.path(cfg$out_root, "precip", "precip_parquet")
cfg$out_daily_root  <- file.path(cfg$out_root, "stats", "daily")
cfg$out_ytd_root    <- file.path(cfg$out_root, "stats", "ytd")
cfg$out_dytd_root   <- file.path(cfg$out_root, "derived_ytd_precip")

dir.create(cfg$cache_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_precip_root, recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_daily_root,  recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_ytd_root,    recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$out_dytd_root,   recursive = TRUE, showWarnings = FALSE)

# -----------------------------
# QA outputs (written under out_root/qa)
# -----------------------------
cfg$qa_dir <- file.path(cfg$out_root, "qa")
dir.create(cfg$qa_dir, recursive = TRUE, showWarnings = FALSE)
run_id <- format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")

# -----------------------------
# Logging
# -----------------------------
log_msg <- function(..., level="INFO") {
  ts <- format(Sys.time(), tz="UTC", usetz=TRUE)
  message(sprintf("[%s][AOI_LOCAL_BACKFILL][%s] %s", ts, level, paste0(..., collapse="")))
}

# -----------------------------
# Helpers
# -----------------------------
parse_s3_uri <- function(uri) {
  x <- sub("^s3://", "", uri)
  parts <- strsplit(x, "/", fixed = TRUE)[[1]]
  list(bucket = parts[1], key = paste(parts[-1], collapse = "/"))
}

download_s3_cached <- function(s3, s3_uri, local_path) {
  if (file.exists(local_path) && file.info(local_path)$size > 0) {
    log_msg("Cache hit: ", local_path)
    return(local_path)
  }
  p <- parse_s3_uri(s3_uri)
  log_msg("Downloading asset: ", s3_uri)
  obj <- s3$get_object(Bucket = p$bucket, Key = p$key)
  writeBin(obj$Body, local_path)
  if (is.na(file.info(local_path)$size) || file.info(local_path)$size < 10*1024) {
    stop("Downloaded asset too small: ", local_path)
  }
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

ensure_dir_for_file <- function(p) dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)

# ============================================================
# 0) AWS init (only to download 2 assets)
# ============================================================
if (file.exists(cfg$renviron_path)) readRenviron(cfg$renviron_path)
Sys.setenv(AWS_REGION = cfg$aws_region, AWS_DEFAULT_REGION = cfg$aws_region)
s3 <- paws::s3(config = list(region = cfg$aws_region))

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
log_msg("Vol-mask rows: ", nrow(vol_masks), " distinct areas=", n_distinct(vol_masks$area_name))

# ============================================================
# 2) Discover local CONUS files
# ============================================================
conus_files <- list.files(cfg$conus_local_root, pattern="part-0\\.parquet$", recursive=TRUE, full.names=TRUE)
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
  
  # Reset YTD accumulators for this year
  cum_ytd <- NULL  # grib_id, rain_ytd_mm
  cum_vol <- NULL  # area_name, area_m2, ytd_vol_m3
  
  year_start <- as.Date(paste0(yr, "-01-01"))
  processed_dates <- as.Date(character(0))
  
  for (ii in seq_along(year_files)) {
    
    f <- year_files[ii]
    d <- as.Date(year_dates[ii], origin="1970-01-01")
    cycle_eff <- paste0(format(d, "%Y%m%d"), cfg$cycle_hour)
    
    # output paths
    out_precip <- out_day_path(cfg$out_precip_root, d)
    out_daily  <- out_day_path(cfg$out_daily_root,  d)
    out_dytd   <- out_day_path(cfg$out_dytd_root,   d)
    out_ytd    <- out_day_path(cfg$out_ytd_root,    d)
    
    if (isTRUE(cfg$skip_existing) &&
        file.exists(out_precip) && file.exists(out_daily) && file.exists(out_dytd) && file.exists(out_ytd)) {
      log_msg("[", yr, " ", ii, "/", length(year_files), "] SKIP (exists): ", as.character(d))
      processed_dates <- sort(unique(c(processed_dates, d)))
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
    
    # ========================================================
    # A) AOI precip subset (boundary mask join)
    # ========================================================
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
    
    if (isTRUE(cfg$write_precip) && !(isTRUE(cfg$skip_existing) && file.exists(out_precip))) {
      ensure_dir_for_file(out_precip)
      arrow::write_parquet(aoi, out_precip, compression="zstd")
      if (file.info(out_precip)$size < 10*1024) stop("Precip parquet too small: ", out_precip)
    }
    
    # ========================================================
    # B) Daily stats (vol-calcs join)
    # ========================================================
    if (isTRUE(cfg$write_daily_stats) && !(isTRUE(cfg$skip_existing) && file.exists(out_daily))) {
      
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
        select(area_id, area_name, cycle_eff, date_utc, n_bins, area_m2, vol_m3, basin_avg_mm, max_bin_mm,
               pct_area_gt_2p54mm, pct_area_gt_6p35mm, pct_area_gt_12p7mm, pct_area_gt_19p05mm,
               pct_area_gt_25p4mm, pct_area_gt_31p75mm, pct_area_gt_38p1mm)
      
      ensure_dir_for_file(out_daily)
      arrow::write_parquet(daily_stats, out_daily, compression="zstd")
      if (file.info(out_daily)$size < 1024) stop("Daily stats parquet too small: ", out_daily)
    } else {
      daily_stats <- arrow::read_parquet(out_daily)
    }
    
    # Mark this day processed successfully (used for days_present)
    processed_dates <- sort(unique(c(processed_dates, d)))
    
    # Coverage metrics like the worker (present vs expected)
    days_present  <- length(processed_dates)
    days_expected <- length(seq.Date(year_start, d, by="day"))
    days_missing  <- days_expected - days_present
    
    # ========================================================
    # C) derived_ytd_precip (YEAR RESET)
    # ========================================================
    if (isTRUE(cfg$write_derived_ytd_precip) && !(isTRUE(cfg$skip_existing) && file.exists(out_dytd))) {
      
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
    
    # ========================================================
    # D) YTD stats (YEAR RESET)
    # ========================================================
    if (isTRUE(cfg$write_ytd_stats) && !(isTRUE(cfg$skip_existing) && file.exists(out_ytd))) {
      
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
        select(area_id, area_name, year, days_present, days_expected, days_missing, area_m2, ytd_vol_m3, ytd_avg_mm)
      
      ensure_dir_for_file(out_ytd)
      arrow::write_parquet(ytd_stats, out_ytd, compression="zstd")
      if (file.info(out_ytd)$size < 1024) stop("YTD stats parquet too small: ", out_ytd)
    }
    
    log_msg("OK ", as.character(d),
            " | precip=", out_precip,
            " | daily=", out_daily,
            " | dytd=", out_dytd,
            " | ytd=", out_ytd,
            " | days_present=", days_present, " days_expected=", days_expected, " days_missing=", days_missing)
  }
  
  log_msg("YEAR DONE: ", yr, " (days=", length(year_files), ")")
}

# ============================================================
# 4) QA REPORTS (local) - counts by year + parquet schemas
# ============================================================

qa_counts_path  <- file.path(cfg$qa_dir, paste0("qa_file_counts_", cfg$area_id, "_", run_id, ".csv"))
qa_schema_path  <- file.path(cfg$qa_dir, paste0("qa_parquet_schemas_", cfg$area_id, "_", run_id, ".csv"))
qa_summary_path <- file.path(cfg$qa_dir, paste0("qa_summary_", cfg$area_id, "_", run_id, ".txt"))

# Helper: capture a simple schema (names + typeof/class) from a parquet file
schema_from_parquet <- function(p) {
  df <- arrow::read_parquet(p)
  data.frame(
    col_name = names(df),
    col_type = vapply(df, function(x) paste(class(x), collapse = "/"), character(1)),
    stringsAsFactors = FALSE
  )
}

# A) File counts by year (expected vs present) for each dataset tree
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
  
  end_expected <- max(yr_dates)
  
  start_expected <- as.Date(paste0(yr, "-01-01"))
  if (!is.na(cfg$date_start) && format(cfg$date_start, "%Y") == yr) {
    start_expected <- max(start_expected, cfg$date_start)
  }
  if (!is.na(cfg$date_start) && format(cfg$date_start, "%Y") > yr) next
  
  # end_expected already respects cfg$date_end via conus_dates filtering, but keep safe:
  if (!is.na(cfg$date_end) && format(cfg$date_end, "%Y") == yr) {
    end_expected <- min(end_expected, cfg$date_end)
  }
  
  if (end_expected < start_expected) next
  
  expected_dates <- seq.Date(start_expected, end_expected, by = "day")
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
      missing_dates <- as.character(expected_dates[!present_flags])
      miss_path <- file.path(cfg$qa_dir, paste0("missing_dates_", ds, "_year=", yr, "_", run_id, ".txt"))
      writeLines(missing_dates, miss_path)
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

write.csv(counts_df, qa_counts_path, row.names = FALSE)

# B) Parquet schemas (one representative file per dataset + the two asset parquets)
schema_rows <- list()

# Asset schemas from cached files
schema_rows[[length(schema_rows) + 1]] <- data.frame(
  item = "boundary_mask_asset",
  sample_file = normalizePath(mask_cache, winslash = "/", mustWork = FALSE),
  schema_from_parquet(mask_cache),
  stringsAsFactors = FALSE
)

schema_rows[[length(schema_rows) + 1]] <- data.frame(
  item = "area_vol_calcs_asset",
  sample_file = normalizePath(vol_cache, winslash = "/", mustWork = FALSE),
  schema_from_parquet(vol_cache),
  stringsAsFactors = FALSE
)

# Dataset schemas: pick the first part-0.parquet found (should be stable across years)
pick_first_parquet <- function(root) {
  files <- list.files(root, pattern = "part-0\\.parquet$", recursive = TRUE, full.names = TRUE)
  if (length(files) == 0) return(NA_character_)
  files[1]
}

for (ds in names(ds_roots)) {
  sample <- pick_first_parquet(ds_roots[[ds]])
  if (is.na(sample)) next
  
  schema_rows[[length(schema_rows) + 1]] <- data.frame(
    item = paste0("dataset_", ds),
    sample_file = normalizePath(sample, winslash = "/", mustWork = FALSE),
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

write.csv(schema_df, qa_schema_path, row.names = FALSE)

# C) Human-readable summary
summ_lines <- c(
  paste0("area_id: ", cfg$area_id),
  paste0("run_id: ", run_id),
  paste0("created_utc: ", format(Sys.time(), tz="UTC", usetz=TRUE)),
  "",
  "QA outputs:",
  paste0("  - ", qa_counts_path),
  paste0("  - ", qa_schema_path),
  paste0("  - ", qa_summary_path),
  "",
  "Notes:",
  "  - qa_file_counts: expected vs present counts per year for each dataset tree.",
  "  - missing_dates_*.txt files are written only when missing_files > 0.",
  "  - qa_parquet_schemas: column names + R classes from representative parquets (assets + one file per dataset)."
)

writeLines(summ_lines, qa_summary_path)

log_msg("QA written: ", qa_counts_path)
log_msg("QA written: ", qa_schema_path)
log_msg("QA written: ", qa_summary_path)

log_msg("DONE. Local AOI archive written under: ", cfg$out_root)