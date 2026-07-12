# 6_ytd_climatology_values.R
#
# Build a historical YTD climatology values table from local hive-style
# stats/ytd parquet files, derive daily precip from YTD values, then write
# both parquet and CSV locally only.
#
# Input example:
#   F:/ea-rchg-zn_archive_build/stats/ytd/year=2004/month=03/day=02/part-0.parquet
#
# Output examples:
#   F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_values_ea-rchg-zn_2002_2025.parquet
#   F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_values_ea-rchg-zn_2002_2025.csv
#
# Notes:
#   - area_name is the subbasin/statistics area column.
#   - Daily precip is derived as:
#       daily_basin_avg_mm = today's ytd_basin_avg_mm - yesterday's ytd_basin_avg_mm
#   - January 1 is handled as:
#       Jan 1 daily = Jan 1 YTD - 0
#   - 02-29 is read so leap-year daily deltas are correct, then excluded from final output.
#   - plot_day is a 1-365 calendar-day index after removing 02-29.
#     Jan 1 = 1, Feb 28 = 59, Mar 1 = 60, Dec 31 = 365.
#   - This script writes LOCAL ONLY. No S3 upload is attempted.

# ==============================================================================
# CONFIG
# ==============================================================================

AREA_ID <- "texas_mrb"

BEGIN_YEAR <- 2002L
END_YEAR   <- 2025L

HIST_YTD_ROOT <- "F:/texas_mrb_archive_build/stats/ytd"

# Local output location.
# Keep this separate from the tidy hive archive structure.
LOCAL_OUTPUT_DIR <- "F:/texas_mrb_archive_build/climatology"

OUTPUT_FILE_NAME <- sprintf(
  "ytd_climatology_values_%s_%s_%s.parquet",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

OUTPUT_CSV_FILE_NAME <- sprintf(
  "ytd_climatology_values_%s_%s_%s.csv",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

LOCAL_OUTPUT_FILE <- file.path(LOCAL_OUTPUT_DIR, OUTPUT_FILE_NAME)
LOCAL_OUTPUT_CSV  <- file.path(LOCAL_OUTPUT_DIR, OUTPUT_CSV_FILE_NAME)

# Set TRUE for a safe validation/sample run.
# Dry run does not write the local parquet or CSV.
DRY_RUN <- FALSE
DRY_RUN_N_FILES <- 10L

# For the final production build, this should usually be TRUE.
# While the archive is still being written, set FALSE if you want to inspect
# currently available files without stopping on missing dates.
FAIL_ON_MISSING_FILES <- TRUE

# Column names in the source stats/ytd parquet files.
AREA_ID_COL   <- "area_id"
SUBBASIN_COL  <- "area_name"
YTD_MM_COL    <- "ytd_avg_mm"

# Output climatology value column names.
OUTPUT_YTD_MM_COL   <- "ytd_basin_avg_mm"
OUTPUT_DAILY_MM_COL <- "daily_basin_avg_mm"

# ==============================================================================
# PACKAGE SETUP
# ==============================================================================

required_packages <- c(
  "arrow",
  "dplyr",
  "lubridate",
  "purrr",
  "tibble"
)

missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]

if (length(missing_packages) > 0) {
  stop(
    "Missing required package(s): ", paste(missing_packages, collapse = ", "),
    "\nInstall them, then rerun this script.",
    call. = FALSE
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(purrr)
  library(tibble)
})

# ==============================================================================
# HELPERS
# ==============================================================================

message2 <- function(...) {
  message(sprintf(...))
}

normalize_local_path <- function(x) {
  x <- gsub("\\\\", "/", x)
  x <- gsub("^([A-Za-z]):/+", "\\1:/", x)
  normalizePath(x, winslash = "/", mustWork = FALSE)
}

extract_date_from_hive_path <- function(path) {
  path_norm <- normalize_local_path(path)
  m <- regexec("year=([0-9]{4})/month=([0-9]{2})/day=([0-9]{2})", path_norm)
  parts <- regmatches(path_norm, m)[[1]]

  if (length(parts) != 4L) {
    stop("Could not extract hive date from path: ", path, call. = FALSE)
  }

  as.Date(sprintf("%s-%s-%s", parts[2], parts[3], parts[4]))
}

make_expected_dates <- function(begin_year, end_year) {
  dates <- seq.Date(
    from = as.Date(sprintf("%04d-01-01", begin_year)),
    to   = as.Date(sprintf("%04d-12-31", end_year)),
    by   = "day"
  )

  # Exclude Feb. 29 from final climatology inventory expectation.
  dates[format(dates, "%m-%d") != "02-29"]
}

calc_plot_day <- function(date) {
  date <- as.Date(date)
  doy <- lubridate::yday(date)
  yr <- lubridate::year(date)

  # After removing 02-29, shift leap-year days after Feb. 29 back by one.
  shift <- lubridate::leap_year(yr) & doy > 60L
  as.integer(doy - ifelse(shift, 1L, 0L))
}

read_one_ytd_file <- function(path) {
  file_date <- extract_date_from_hive_path(path)
  file_year <- lubridate::year(file_date)

  x_raw <- arrow::read_parquet(path)

  required_cols <- c(AREA_ID_COL, SUBBASIN_COL, YTD_MM_COL)
  missing_cols <- setdiff(required_cols, names(x_raw))

  if (length(missing_cols) > 0L) {
    stop(
      "Missing required column(s) in ", path, ": ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  found_area_ids <- sort(unique(as.character(x_raw[[AREA_ID_COL]])))

  if (length(found_area_ids) != 1L || !identical(found_area_ids, AREA_ID)) {
    stop(
      "Unexpected area_id value(s) in ", path, ". Expected exactly '",
      AREA_ID, "' but found: ",
      paste(found_area_ids, collapse = ", "),
      call. = FALSE
    )
  }

  x <- x_raw |>
    dplyr::transmute(
      area_id = as.character(.data[[AREA_ID_COL]]),
      area_name = as.character(.data[[SUBBASIN_COL]]),
      year = as.integer(file_year),
      date = as.Date(file_date),
      mmdd = format(as.Date(file_date), "%m-%d"),
      plot_day = calc_plot_day(file_date),
      "{OUTPUT_YTD_MM_COL}" := as.numeric(.data[[YTD_MM_COL]])
    )

  x
}

list_historical_ytd_files <- function(hist_ytd_root, begin_year, end_year) {
  if (!dir.exists(hist_ytd_root)) {
    stop("Historical YTD root does not exist: ", hist_ytd_root, call. = FALSE)
  }

  files <- list.files(
    hist_ytd_root,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )

  if (length(files) == 0L) {
    stop("No parquet files found under: ", hist_ytd_root, call. = FALSE)
  }

  dates <- vapply(files, extract_date_from_hive_path, as.Date("2000-01-01"))
  dates <- as.Date(dates, origin = "1970-01-01")

  out <- tibble(
    file = files,
    date = dates,
    year = lubridate::year(dates),
    mmdd = format(dates, "%m-%d")
  ) |>
    dplyr::filter(
      .data$year >= begin_year,
      .data$year <= end_year
    ) |>
    dplyr::arrange(.data$date)

  out
}

validate_file_inventory <- function(file_index, expected_dates, fail_on_missing = TRUE) {
  file_index_no_leap <- file_index |>
    dplyr::filter(.data$mmdd != "02-29")

  available_dates <- unique(file_index_no_leap$date)
  missing_dates <- setdiff(expected_dates, available_dates)

  duplicate_dates <- available_dates[
    tabulate(match(file_index_no_leap$date, available_dates)) > 1L
  ]

  message2("Expected non-02/29 dates: %s", length(expected_dates))
  message2("Available non-02/29 parquet dates: %s", length(available_dates))
  message2("Available total parquet dates including 02/29: %s", dplyr::n_distinct(file_index$date))

  year_counts <- file_index_no_leap |>
    dplyr::count(.data$year, name = "n_files_non_leap") |>
    dplyr::arrange(.data$year)

  print(year_counts, n = Inf)

  if (length(duplicate_dates) > 0L) {
    warning(
      "Duplicate parquet dates detected. First few: ",
      paste(head(as.character(duplicate_dates), 10), collapse = ", "),
      call. = FALSE
    )
  }

  if (length(missing_dates) > 0L) {
    msg <- paste0(
      "Missing ", length(missing_dates), " expected parquet date(s). First few: ",
      paste(head(as.character(missing_dates), 20), collapse = ", ")
    )

    if (isTRUE(fail_on_missing)) {
      stop(msg, call. = FALSE)
    } else {
      warning(msg, call. = FALSE)
    }
  }

  invisible(list(
    missing_dates = missing_dates,
    duplicate_dates = duplicate_dates,
    year_counts = year_counts
  ))
}

# ==============================================================================
# MAIN
# ==============================================================================

message2("Building YTD climatology values")
message2("AREA_ID: %s", AREA_ID)
message2("Years: %s-%s", BEGIN_YEAR, END_YEAR)
message2("Historical YTD root: %s", HIST_YTD_ROOT)
message2("Local output dir: %s", LOCAL_OUTPUT_DIR)
message2("Local output parquet: %s", LOCAL_OUTPUT_FILE)
message2("Local output CSV: %s", LOCAL_OUTPUT_CSV)
message2("Dry run: %s", DRY_RUN)
message2("Local-only mode: TRUE. No S3 upload will be attempted.")

expected_dates <- make_expected_dates(BEGIN_YEAR, END_YEAR)
file_index <- list_historical_ytd_files(HIST_YTD_ROOT, BEGIN_YEAR, END_YEAR)

inventory <- validate_file_inventory(
  file_index = file_index,
  expected_dates = expected_dates,
  fail_on_missing = FAIL_ON_MISSING_FILES
)

files_to_read <- file_index$file

if (isTRUE(DRY_RUN)) {
  files_to_read <- head(files_to_read, DRY_RUN_N_FILES)
  message2("Dry run enabled. Reading first %s parquet file(s) only.", length(files_to_read))
}

message2("Reading parquet files...")

climatology_values <- purrr::map_dfr(
  files_to_read,
  function(path) {
    message2("Reading: %s", normalize_local_path(path))
    read_one_ytd_file(path)
  }
)

# Derive daily basin-average precip from YTD values.
# Important:
#   1. Do this BEFORE removing 02-29, so leap-year Mar 1 is correct.
#   2. Use lag(..., default = 0), so Jan 1 daily = Jan 1 YTD - 0.
climatology_values <- climatology_values |>
  dplyr::arrange(.data$area_id, .data$area_name, .data$year, .data$date) |>
  dplyr::group_by(.data$area_id, .data$area_name, .data$year) |>
  dplyr::mutate(
    "{OUTPUT_DAILY_MM_COL}" :=
      .data[[OUTPUT_YTD_MM_COL]] - dplyr::lag(.data[[OUTPUT_YTD_MM_COL]], default = 0),
    "{OUTPUT_DAILY_MM_COL}" :=
      ifelse(abs(.data[[OUTPUT_DAILY_MM_COL]]) < 1e-9, 0, .data[[OUTPUT_DAILY_MM_COL]])
  ) |>
  dplyr::ungroup()

negative_daily <- climatology_values |>
  dplyr::filter(.data[[OUTPUT_DAILY_MM_COL]] < -1e-6)

if (nrow(negative_daily) > 0L) {
  warning(
    "Found ", nrow(negative_daily),
    " negative daily precip value(s). First few rows are printed below.",
    call. = FALSE
  )

  print(
    negative_daily |>
      dplyr::select(
        area_id,
        area_name,
        year,
        date,
        mmdd,
        plot_day,
        all_of(OUTPUT_YTD_MM_COL),
        all_of(OUTPUT_DAILY_MM_COL)
      ) |>
      head(40),
    n = 40
  )
}

climatology_values <- climatology_values |>
  dplyr::filter(.data$mmdd != "02-29") |>
  dplyr::arrange(.data$area_id, .data$area_name, .data$date)

message2("Rows built: %s", nrow(climatology_values))
message2("Distinct area_name values: %s", dplyr::n_distinct(climatology_values$area_name))
message2("Distinct dates: %s", dplyr::n_distinct(climatology_values$date))

message2("Rows by area_name:")
print(
  climatology_values |>
    dplyr::count(.data$area_name, name = "n_rows") |>
    dplyr::arrange(.data$area_name),
  n = Inf
)

message2("Preview:")
print(head(climatology_values, 20), n = 20)

message2("Daily precip preview:")
print(
  climatology_values |>
    dplyr::select(
      area_id,
      area_name,
      year,
      date,
      mmdd,
      plot_day,
      all_of(OUTPUT_YTD_MM_COL),
      all_of(OUTPUT_DAILY_MM_COL)
    ) |>
    head(40),
  n = 40
)

message2("January 1 check:")
print(
  climatology_values |>
    dplyr::filter(format(.data$date, "%m-%d") == "01-01") |>
    dplyr::select(
      area_id,
      area_name,
      year,
      date,
      all_of(OUTPUT_YTD_MM_COL),
      all_of(OUTPUT_DAILY_MM_COL)
    ) |>
    head(40),
  n = 40
)

if (isTRUE(DRY_RUN)) {
  message2("Dry run complete. No local parquet or CSV was written.")
  message2("Set DRY_RUN <- FALSE for the full local build.")
} else {
  dir.create(LOCAL_OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

  if (!dir.exists(LOCAL_OUTPUT_DIR)) {
    stop("Failed to create LOCAL_OUTPUT_DIR: ", LOCAL_OUTPUT_DIR, call. = FALSE)
  }

  message2("Writing local parquet: %s", normalize_local_path(LOCAL_OUTPUT_FILE))
  arrow::write_parquet(climatology_values, LOCAL_OUTPUT_FILE)

  message2("Writing local CSV: %s", normalize_local_path(LOCAL_OUTPUT_CSV))
  write.csv(climatology_values, LOCAL_OUTPUT_CSV, row.names = FALSE, quote = TRUE)

  message2("Done.")
  message2("Output local parquet: %s", normalize_local_path(LOCAL_OUTPUT_FILE))
  message2("Output local CSV: %s", normalize_local_path(LOCAL_OUTPUT_CSV))
}
