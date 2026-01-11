# Merge all subbasin CSVs in a folder into one CSV, adding area_name from filename
# Output: ea-rchg-zn-area-vol-calc.csv (written back to the same folder)

folder <- "C:/stg4/stg4-hrap-gis/setup_new_area/nueces"
out_file <- file.path(folder, "nueces-area-vol-calc.csv")

# List all CSVs in the folder (exclude the output file if it already exists)
csv_files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)
csv_files <- csv_files[normalizePath(csv_files, winslash = "/", mustWork = FALSE) !=
                         normalizePath(out_file, winslash = "/", mustWork = FALSE)]

if (length(csv_files) == 0) stop("No input .csv files found in: ", folder)

# Expected columns
expected_cols <- c("grib_id", "hrap_x", "hrap_y", "bin_area")

read_one <- function(path) {
  df <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  
  # Basic schema check
  missing <- setdiff(expected_cols, names(df))
  if (length(missing) > 0) {
    stop("File is missing expected columns: ", paste(missing, collapse = ", "),
         "\nFile: ", path)
  }
  
  # Keep only expected cols (and in the right order)
  df <- df[, expected_cols]
  
  # Coerce types (safe)
  df$grib_id  <- suppressWarnings(as.integer(df$grib_id))
  df$hrap_x   <- suppressWarnings(as.integer(df$hrap_x))
  df$hrap_y   <- suppressWarnings(as.integer(df$hrap_y))
  df$bin_area <- suppressWarnings(as.numeric(df$bin_area))
  
  # area_name from filename stem
  df$area_name <- tools::file_path_sans_ext(basename(path))
  
  # Ensure column order: original 4 + area_name as 5th
  df[, c(expected_cols, "area_name")]
}

combined <- do.call(rbind, lapply(csv_files, read_one))

# Write combined CSV (no row numbers, no quotes)
write.table(
  combined,
  file = out_file,
  sep = ",",
  row.names = FALSE,
  col.names = TRUE,
  quote = FALSE
)

cat("Wrote:", out_file, "\n")
cat("Files merged:", length(csv_files), "\n")
cat("Rows:", nrow(combined), " Cols:", ncol(combined), "\n")
