# 7_ytd_climatology_summary.R
#
# Build a historical YTD climatology summary table from the local climatology
# values file created by 6_ytd_climatology_values.R, then write both parquet
# and CSV locally only.
#
# Input example:
#   F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_values_ea-rchg-zn_2002_2025.parquet
#
# Output examples:
#   F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_summary_ea-rchg-zn_2002_2025.parquet
#   F:/ea-rchg-zn_archive_build/climatology/ytd_climatology_summary_ea-rchg-zn_2002_2025.csv
#
# Notes:
#   - area_name is the subbasin/statistics area column.
#   - 02-29 is excluded from the summary, matching the values file behavior.
#   - plot_day is a 1-365 calendar-day index after removing 02-29.
#     Jan 1 = 1, Feb 28 = 59, Mar 1 = 60, Dec 31 = 365.
#   - This script writes LOCAL ONLY. No S3 upload is attempted.

# ==============================================================================
# CONFIG
# ==============================================================================

AREA_ID <- "texas_mrb"

BEGIN_YEAR <- 2002L
END_YEAR   <- 2025L

# Local climatology folder. This should match LOCAL_OUTPUT_DIR from
# 6_ytd_climatology_values.R.
LOCAL_CLIMATOLOGY_DIR <- "F:/texas_mrb_archive_build/climatology"

INPUT_VALUES_FILE_NAME <- sprintf(
  "ytd_climatology_values_%s_%s_%s.parquet",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

INPUT_VALUES_CSV_FILE_NAME <- sprintf(
  "ytd_climatology_values_%s_%s_%s.csv",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

OUTPUT_FILE_NAME <- sprintf(
  "ytd_climatology_summary_%s_%s_%s.parquet",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

OUTPUT_CSV_FILE_NAME <- sprintf(
  "ytd_climatology_summary_%s_%s_%s.csv",
  AREA_ID,
  BEGIN_YEAR,
  END_YEAR
)

LOCAL_INPUT_VALUES_FILE <- file.path(LOCAL_CLIMATOLOGY_DIR, INPUT_VALUES_FILE_NAME)
LOCAL_INPUT_VALUES_CSV  <- file.path(LOCAL_CLIMATOLOGY_DIR, INPUT_VALUES_CSV_FILE_NAME)

LOCAL_OUTPUT_FILE <- file.path(LOCAL_CLIMATOLOGY_DIR, OUTPUT_FILE_NAME)
LOCAL_OUTPUT_CSV  <- file.path(LOCAL_CLIMATOLOGY_DIR, OUTPUT_CSV_FILE_NAME)

# Set TRUE for a safe validation/sample run.
# Dry run does not write the local parquet or CSV.
DRY_RUN <- FALSE

# If TRUE, the script stops when any area_name/mmdd group has fewer than the
# expected number of baseline years.
FAIL_ON_INCOMPLETE_GROUPS <- TRUE

# Column names expected in the values file from 6_ytd_climatology_values.R.
AREA_ID_COL      <- "area_id"
SUBBASIN_COL     <- "area_name"
YEAR_COL         <- "year"
DATE_COL         <- "date"
MMDD_COL         <- "mmdd"
PLOT_DAY_COL     <- "plot_day"
YTD_MM_COL       <- "ytd_basin_avg_mm"
DAILY_MM_COL     <- "daily_basin_avg_mm"

# ==============================================================================
# PACKAGE SETUP
# ==============================================================================

required_packages <- c(
  "arrow",
  "dplyr",
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

expected_n_years <- function(begin_year, end_year) {
  as.integer(end_year - begin_year + 1L)
}

safe_quantile <- function(x, prob) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  as.numeric(stats::quantile(x, probs = prob, na.rm = TRUE, names = FALSE, type = 7))
}

safe_mean <- function(x) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  mean(x)
}

safe_min <- function(x) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  min(x)
}

safe_max <- function(x) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  max(x)
}

read_climatology_values <- function(parquet_path, csv_path) {
  if (file.exists(parquet_path)) {
    message2("Reading local values parquet: %s", normalize_local_path(parquet_path))
    return(arrow::read_parquet(parquet_path))
  }

  if (file.exists(csv_path)) {
    message2("Values parquet not found. Reading local values CSV: %s", normalize_local_path(csv_path))
    return(utils::read.csv(csv_path, stringsAsFactors = FALSE))
  }

  stop(
    "Could not find input values parquet or CSV.\n",
    "Expected parquet: ", parquet_path, "\n",
    "Expected CSV: ", csv_path,
    call. = FALSE
  )
}

check_required_columns <- function(x, required_cols) {
  missing_cols <- setdiff(required_cols, names(x))

  if (length(missing_cols) > 0L) {
    stop(
      "Input values file is missing required column(s): ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

validate_values_table <- function(x, area_id, begin_year, end_year) {
  expected_years <- seq.int(begin_year, end_year)
  n_expected_years <- expected_n_years(begin_year, end_year)

  # Hard stop if area_id does not match the declared area.
  area_ids_found <- sort(unique(as.character(x[[AREA_ID_COL]])))

  if (length(area_ids_found) != 1L || !identical(area_ids_found, area_id)) {
    stop(
      "Input values file must contain exactly one area_id, and it must equal AREA_ID.\n",
      "Declared AREA_ID: ", area_id, "\n",
      "Found area_id value(s): ", paste(area_ids_found, collapse = ", "),
      call. = FALSE
    )
  }

  years_found <- sort(unique(as.integer(x[[YEAR_COL]])))
  missing_years <- setdiff(expected_years, years_found)
  extra_years <- setdiff(years_found, expected_years)

  if (length(missing_years) > 0L) {
    stop(
      "Input values file is missing expected year(s): ",
      paste(missing_years, collapse = ", "),
      call. = FALSE
    )
  }

  if (length(extra_years) > 0L) {
    stop(
      "Input values file contains year(s) outside configured range: ",
      paste(extra_years, collapse = ", "),
      call. = FALSE
    )
  }

  feb29_rows <- x |>
    dplyr::filter(.data[[MMDD_COL]] == "02-29")

  if (nrow(feb29_rows) > 0L) {
    warning(
      "Input values file contains 02-29 rows. They will be excluded before summarizing.",
      call. = FALSE
    )
  }

  dupes <- x |>
    dplyr::count(
      .data[[AREA_ID_COL]],
      .data[[SUBBASIN_COL]],
      .data[[YEAR_COL]],
      .data[[MMDD_COL]],
      name = "n"
    ) |>
    dplyr::filter(.data$n > 1L)

  if (nrow(dupes) > 0L) {
    message2("Duplicate area/year/mmdd combinations detected. First 40 rows:")
    print(head(dupes, 40), n = 40)
    stop("Duplicate area_name/year/mmdd rows found in values file.", call. = FALSE)
  }

  year_counts <- x |>
    dplyr::filter(.data[[MMDD_COL]] != "02-29") |>
    dplyr::group_by(.data[[AREA_ID_COL]], .data[[SUBBASIN_COL]], .data[[MMDD_COL]], .data[[PLOT_DAY_COL]]) |>
    dplyr::summarise(n_years = dplyr::n_distinct(.data[[YEAR_COL]]), .groups = "drop")

  incomplete_groups <- year_counts |>
    dplyr::filter(.data$n_years != n_expected_years)

  message2("Expected baseline years per area_name/mmdd group: %s", n_expected_years)
  message2("Summary groups checked: %s", nrow(year_counts))

  if (nrow(incomplete_groups) > 0L) {
    message2("Incomplete area_name/mmdd groups detected. First 40 rows:")
    print(head(incomplete_groups, 40), n = 40)

    if (isTRUE(FAIL_ON_INCOMPLETE_GROUPS)) {
      stop("Incomplete climatology groups found.", call. = FALSE)
    } else {
      warning("Incomplete climatology groups found.", call. = FALSE)
    }
  }

  invisible(TRUE)
}

build_climatology_summary <- function(values, begin_year, end_year) {
  values |>
    dplyr::filter(.data[[MMDD_COL]] != "02-29") |>
    dplyr::mutate(
      area_id = as.character(.data[[AREA_ID_COL]]),
      area_name = as.character(.data[[SUBBASIN_COL]]),
      year = as.integer(.data[[YEAR_COL]]),
      mmdd = as.character(.data[[MMDD_COL]]),
      plot_day = as.integer(.data[[PLOT_DAY_COL]]),
      ytd_basin_avg_mm = as.numeric(.data[[YTD_MM_COL]]),
      daily_basin_avg_mm = as.numeric(.data[[DAILY_MM_COL]])
    ) |>
    dplyr::group_by(.data$area_id, .data$area_name, .data$mmdd, .data$plot_day) |>
    dplyr::summarise(
      n_years = dplyr::n_distinct(.data$year),
      begin_year = as.integer(begin_year),
      end_year = as.integer(end_year),
      p10_ytd_basin_avg_mm = safe_quantile(.data$ytd_basin_avg_mm, 0.10),
      p25_ytd_basin_avg_mm = safe_quantile(.data$ytd_basin_avg_mm, 0.25),
      p50_ytd_basin_avg_mm = safe_quantile(.data$ytd_basin_avg_mm, 0.50),
      p75_ytd_basin_avg_mm = safe_quantile(.data$ytd_basin_avg_mm, 0.75),
      p90_ytd_basin_avg_mm = safe_quantile(.data$ytd_basin_avg_mm, 0.90),
      mean_ytd_basin_avg_mm = safe_mean(.data$ytd_basin_avg_mm),
      min_ytd_basin_avg_mm = safe_min(.data$ytd_basin_avg_mm),
      max_ytd_basin_avg_mm = safe_max(.data$ytd_basin_avg_mm),
      p10_daily_basin_avg_mm = safe_quantile(.data$daily_basin_avg_mm, 0.10),
      p25_daily_basin_avg_mm = safe_quantile(.data$daily_basin_avg_mm, 0.25),
      p50_daily_basin_avg_mm = safe_quantile(.data$daily_basin_avg_mm, 0.50),
      p75_daily_basin_avg_mm = safe_quantile(.data$daily_basin_avg_mm, 0.75),
      p90_daily_basin_avg_mm = safe_quantile(.data$daily_basin_avg_mm, 0.90),
      mean_daily_basin_avg_mm = safe_mean(.data$daily_basin_avg_mm),
      min_daily_basin_avg_mm = safe_min(.data$daily_basin_avg_mm),
      max_daily_basin_avg_mm = safe_max(.data$daily_basin_avg_mm),
      .groups = "drop"
    ) |>
    dplyr::select(
      area_id,
      area_name,
      mmdd,
      plot_day,
      n_years,
      begin_year,
      end_year,
      p10_ytd_basin_avg_mm,
      p25_ytd_basin_avg_mm,
      p50_ytd_basin_avg_mm,
      p75_ytd_basin_avg_mm,
      p90_ytd_basin_avg_mm,
      mean_ytd_basin_avg_mm,
      min_ytd_basin_avg_mm,
      max_ytd_basin_avg_mm,
      p10_daily_basin_avg_mm,
      p25_daily_basin_avg_mm,
      p50_daily_basin_avg_mm,
      p75_daily_basin_avg_mm,
      p90_daily_basin_avg_mm,
      mean_daily_basin_avg_mm,
      min_daily_basin_avg_mm,
      max_daily_basin_avg_mm
    ) |>
    dplyr::arrange(.data$area_name, .data$plot_day)
}

# ==============================================================================
# MAIN
# ===============================================================================

message2("Building YTD climatology summary")
message2("AREA_ID: %s", AREA_ID)
message2("Years: %s-%s", BEGIN_YEAR, END_YEAR)
message2("Local climatology dir: %s", LOCAL_CLIMATOLOGY_DIR)
message2("Local input values parquet: %s", LOCAL_INPUT_VALUES_FILE)
message2("Local input values CSV fallback: %s", LOCAL_INPUT_VALUES_CSV)
message2("Local output parquet: %s", LOCAL_OUTPUT_FILE)
message2("Local output CSV: %s", LOCAL_OUTPUT_CSV)
message2("Dry run: %s", DRY_RUN)
message2("Local-only mode: TRUE. No S3 upload will be attempted.")

required_cols <- c(
  AREA_ID_COL,
  SUBBASIN_COL,
  YEAR_COL,
  DATE_COL,
  MMDD_COL,
  PLOT_DAY_COL,
  YTD_MM_COL,
  DAILY_MM_COL
)

climatology_values <- read_climatology_values(
  parquet_path = LOCAL_INPUT_VALUES_FILE,
  csv_path = LOCAL_INPUT_VALUES_CSV
)

check_required_columns(climatology_values, required_cols)

validate_values_table(
  x = climatology_values,
  area_id = AREA_ID,
  begin_year = BEGIN_YEAR,
  end_year = END_YEAR
)

message2("Input rows: %s", nrow(climatology_values))
message2("Input distinct area_name values: %s", dplyr::n_distinct(climatology_values[[SUBBASIN_COL]]))
message2("Input distinct mmdd values excluding 02-29: %s", dplyr::n_distinct(climatology_values[[MMDD_COL]][climatology_values[[MMDD_COL]] != "02-29"]))

message2("Input rows by area_name:")
print(
  climatology_values |>
    dplyr::filter(.data[[MMDD_COL]] != "02-29") |>
    dplyr::count(.data[[SUBBASIN_COL]], name = "n_rows") |>
    dplyr::arrange(.data[[SUBBASIN_COL]]),
  n = Inf
)

message2("Building summary percentiles...")

climatology_summary <- build_climatology_summary(
  values = climatology_values,
  begin_year = BEGIN_YEAR,
  end_year = END_YEAR
)

message2("Summary rows built: %s", nrow(climatology_summary))
message2("Summary distinct area_name values: %s", dplyr::n_distinct(climatology_summary$area_name))
message2("Summary distinct mmdd values: %s", dplyr::n_distinct(climatology_summary$mmdd))

message2("Rows by area_name:")
print(
  climatology_summary |>
    dplyr::count(.data$area_name, name = "n_rows") |>
    dplyr::arrange(.data$area_name),
  n = Inf
)

message2("Preview:")
print(head(climatology_summary, 40), n = 40)

message2("Recharge-Zone selected-date preview if available:")
print(
  climatology_summary |>
    dplyr::filter(
      .data$area_name == "Recharge-Zone",
      .data$mmdd %in% c("01-01", "03-01", "06-05", "12-31")
    ) |>
    dplyr::arrange(.data$plot_day),
  n = 40
)

if (isTRUE(DRY_RUN)) {
  message2("Dry run complete. No local parquet or CSV was written.")
  message2("Set DRY_RUN <- FALSE for the full local build.")
} else {
  dir.create(LOCAL_CLIMATOLOGY_DIR, recursive = TRUE, showWarnings = FALSE)

  if (!dir.exists(LOCAL_CLIMATOLOGY_DIR)) {
    stop("Failed to create LOCAL_CLIMATOLOGY_DIR: ", LOCAL_CLIMATOLOGY_DIR, call. = FALSE)
  }

  message2("Writing local parquet: %s", normalize_local_path(LOCAL_OUTPUT_FILE))
  arrow::write_parquet(climatology_summary, LOCAL_OUTPUT_FILE)

  message2("Writing local CSV: %s", normalize_local_path(LOCAL_OUTPUT_CSV))
  write.csv(climatology_summary, LOCAL_OUTPUT_CSV, row.names = FALSE, quote = TRUE)

  message2("Done.")
  message2("Output local parquet: %s", normalize_local_path(LOCAL_OUTPUT_FILE))
  message2("Output local CSV: %s", normalize_local_path(LOCAL_OUTPUT_CSV))
}
