library(sf)
library(dplyr)
library(data.table)

# ============================================================
# CONFIG: EDIT ONLY THIS BLOCK
# ============================================================

epsg_work  <- 3631          # working CRS (meters, equal-area)
name_field <- "name"        # subbasin attribute used for filenames + retained as subbasin_name

# Inputs
path_hrap_conus_in <- "C:/stg4-hrap-gis/layers/hrap/hrap_conus_integer.shp"
path_mask_in       <- "C:/stg4-hrap-gis/layers/hanover/cape_fear_basin.shp"  # overall AOI mask (may be multi-feature; will be unioned)
path_subbasins_in  <- "C:/stg4-hrap-gis/layers/hanover/catchments/nh_cf_huc8_clip.shp"  # subbasins (multi-feature; used for per-basin outputs)

# Key for validation
path_key_csv <- "C:/stg4-hrap-gis/layers/hrap/grib2_lat_lon_pt_with_grib_decimal_fix.csv"

# Output root for this AOI
root_out <- "C:/stg4-hrap-gis/layers/cape_fear2"

# Naming prefix for outputs
prefix <- "nh_cf"

# ============================================================
# DERIVED OUTPUT PATHS (usually do NOT edit)
# ============================================================

# Step 1 outputs
path_hrap_conus_out <- file.path(root_out, paste0("hrap_conus_int_epsg_", epsg_work, ".shp"))

# Step 2 outputs
path_mask_out       <- file.path(root_out, paste0(prefix, "_mask_epsg_", epsg_work, ".shp"))
path_subbasins_out  <- file.path(root_out, paste0(prefix, "_subbasins_epsg_", epsg_work, ".shp"))

# Step 3 output
path_hrap_subset <- file.path(root_out, paste0("hrap_", prefix, "_subset_epsg_", epsg_work, ".shp"))

# Step 4/5 output dirs
dir_prepped       <- file.path(root_out, "prepped")
dir_subbasins_out <- file.path(dir_prepped, "subbasins")

# Step 4 outputs (overall mask)
path_union_shp <- file.path(dir_prepped, paste0(prefix, "_hrap_final.shp"))
path_union_csv <- file.path(dir_prepped, paste0(prefix, "_hrap_final.csv"))

# Step 6 output (GeoPackage version of overall mask cells)
path_cells_gpkg  <- file.path(dir_prepped, "cells.gpkg")
cells_layer_name <- "cells"

# ============================================================
# HELPERS
# ============================================================

ensure_dir <- function(p) dir.create(p, recursive = TRUE, showWarnings = FALSE)

to_int_drop <- function(x, field_name) {
  x_num <- suppressWarnings(as.numeric(x))
  if (anyNA(x_num)) stop("NAs introduced when coercing '", field_name, "' to numeric.")
  as.integer(trunc(x_num))
}

make_valid_safe <- function(x) tryCatch(st_make_valid(x), error = function(e) x)

area_m2_int <- function(x_sf) as.integer(trunc(as.numeric(st_area(x_sf))))

safe_filename <- function(x) {
  x <- as.character(x)
  x <- gsub("[^A-Za-z0-9_-]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  if (nchar(x) == 0) "subbasin" else x
}

as_union_sf <- function(poly_sf, id_name = "mask_id", id_val = 1L) {
  g <- st_union(st_geometry(poly_sf))
  out <- st_as_sf(data.frame(tmp_id = id_val), geometry = g, crs = st_crs(poly_sf))
  names(out)[names(out) == "tmp_id"] <- id_name
  make_valid_safe(out)
}

# ============================================================
# STEP 1: HRAP -> force integer IDs + reproject to epsg_work
# ============================================================

cat("\nSTEP 1: HRAP -> integer IDs + EPSG ", epsg_work, "\n", sep = "")

hrap <- st_read(path_hrap_conus_in, quiet = TRUE)
if (is.na(st_crs(hrap))) stop("HRAP input has no CRS. Check .prj or set st_crs() before transforming.")

req <- c("grib_id", "hrap_x", "hrap_y")
missing <- setdiff(req, names(hrap))
if (length(missing) > 0) stop("HRAP missing required fields: ", paste(missing, collapse = ", "))

hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")
stopifnot(is.integer(hrap$grib_id), is.integer(hrap$hrap_x), is.integer(hrap$hrap_y))

hrap <- hrap[, req]
hrap <- st_transform(hrap, epsg_work)

ensure_dir(root_out)
st_write(hrap, path_hrap_conus_out, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)

cat("  HRAP written: ", path_hrap_conus_out, "\n", sep = "")
cat("  HRAP rows   : ", nrow(hrap), "\n", sep = "")

# ============================================================
# STEP 2: Reproject mask + subbasins to epsg_work
# ============================================================

cat("\nSTEP 2A: Mask -> EPSG ", epsg_work, "\n", sep = "")
mask <- st_read(path_mask_in, quiet = TRUE)
if (is.na(st_crs(mask))) stop("mask.shp has no CRS. Check .prj or set st_crs() before transforming.")
mask <- make_valid_safe(mask)
mask <- st_transform(mask, epsg_work)
st_write(mask, path_mask_out, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
cat("  Mask written: ", path_mask_out, " (features=", nrow(mask), ")\n", sep = "")

cat("\nSTEP 2B: Subbasins -> EPSG ", epsg_work, "\n", sep = "")
subs <- st_read(path_subbasins_in, quiet = TRUE)
if (is.na(st_crs(subs))) stop("subbasins.shp has no CRS. Check .prj or set st_crs() before transforming.")
subs <- make_valid_safe(subs)
subs <- st_transform(subs, epsg_work)
st_write(subs, path_subbasins_out, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
cat("  Subbasins written: ", path_subbasins_out, " (features=", nrow(subs), ")\n", sep = "")

# ============================================================
# STEP 3: Subset HRAP to MASK (unioned) to avoid giant overlays
# ============================================================

cat("\nSTEP 3: Subset HRAP to union(mask)\n")

hrap <- st_read(path_hrap_conus_out, quiet = TRUE)
mask <- st_read(path_mask_out, quiet = TRUE)

hrap <- make_valid_safe(hrap)
mask <- make_valid_safe(mask)

hrap <- st_zm(hrap, drop = TRUE, what = "ZM")
mask <- st_zm(mask, drop = TRUE, what = "ZM")

mask_u <- as_union_sf(mask, id_name = "mask_id", id_val = 1L)

hits <- lengths(st_intersects(hrap, mask_u)) > 0
hrap_subset <- hrap[hits, ]

if (nrow(hrap_subset) == 0) stop("Subset produced 0 HRAP cells. Likely CRS mismatch or wrong mask.")

st_write(hrap_subset, path_hrap_subset, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)

cat("  HRAP original:", nrow(hrap), "\n")
cat("  HRAP subset  :", nrow(hrap_subset), "\n")
cat("  Subset written:", path_hrap_subset, "\n")

# ============================================================
# STEP 4: Clip HRAP bins to overall MASK (unioned)
# Output columns: grib_id, hrap_x, hrap_y, bin_area
# ============================================================

cat("\nSTEP 4: Clip HRAP to union(mask)\n")

ensure_dir(dir_prepped)

hrap <- st_read(path_hrap_subset, quiet = TRUE)
mask <- st_read(path_mask_out, quiet = TRUE)

hrap <- make_valid_safe(hrap)
mask <- make_valid_safe(mask)

hrap <- st_zm(hrap, drop = TRUE, what = "ZM")
mask <- st_zm(mask, drop = TRUE, what = "ZM")

need_cols <- c("grib_id", "hrap_x", "hrap_y")
missing <- setdiff(need_cols, names(hrap))
if (length(missing) > 0) stop("HRAP subset missing fields: ", paste(missing, collapse = ", "))
hrap <- hrap[, need_cols]

hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")

mask_u <- as_union_sf(mask, id_name = "mask_id", id_val = 1L)

hits <- lengths(st_intersects(hrap, mask_u)) > 0
hrap_hit <- hrap[hits, ]
if (nrow(hrap_hit) == 0) stop("No HRAP cells intersect mask union. Check CRS/inputs.")

clip_sf <- suppressWarnings(st_intersection(hrap_hit, mask_u))

clip_sf$bin_area <- area_m2_int(clip_sf)
clip_sf$bin_area[is.na(clip_sf$bin_area) | clip_sf$bin_area < 0] <- 0L

final_mask <- clip_sf %>%
  group_by(grib_id, hrap_x, hrap_y) %>%
  summarise(
    bin_area = as.integer(sum(bin_area, na.rm = TRUE)),
    geometry = st_union(geometry),
    .groups = "drop"
  ) %>%
  arrange(grib_id) %>%
  mutate(
    grib_id  = as.integer(grib_id),
    hrap_x   = as.integer(hrap_x),
    hrap_y   = as.integer(hrap_y),
    bin_area = as.integer(bin_area)
  )

st_write(final_mask, path_union_shp, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
write.csv(st_drop_geometry(final_mask), path_union_csv, row.names = FALSE, quote = FALSE)

cat("  Mask union outputs:\n")
cat("   -", path_union_shp, "\n")
cat("   -", path_union_csv, "\n")

# ============================================================
# STEP 5: Per-subbasin clips (one SHP + one CSV per subbasin)
# Output columns: grib_id, hrap_x, hrap_y, bin_area, subbasin_name
# ============================================================

cat("\nSTEP 5: Per-subbasin outputs\n")

ensure_dir(dir_subbasins_out)

hrap <- st_read(path_hrap_subset, quiet = TRUE)
subs <- st_read(path_subbasins_out, quiet = TRUE)

hrap <- make_valid_safe(hrap)
subs <- make_valid_safe(subs)

hrap <- st_zm(hrap, drop = TRUE, what = "ZM")
subs <- st_zm(subs, drop = TRUE, what = "ZM")

if (!(name_field %in% names(subs))) {
  stop("Subbasins missing name_field '", name_field,
       "'. Available: ", paste(names(subs), collapse = ", "))
}

missing <- setdiff(need_cols, names(hrap))
if (length(missing) > 0) stop("HRAP subset missing fields: ", paste(missing, collapse = ", "))

hrap <- hrap[, need_cols]
hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")

cat("  Subbasins/features:", nrow(subs), "\n")

seen <- character(0)

for (i in seq_len(nrow(subs))) {
  
  basin_name_raw <- as.character(subs[[name_field]][i])
  basin_slug     <- safe_filename(basin_name_raw)
  
  if (basin_slug %in% seen) {
    k <- sum(seen == basin_slug) + 1
    basin_slug <- sprintf("%s_%02d", basin_slug, k)
  }
  seen <- c(seen, basin_slug)
  
  out_shp <- file.path(dir_subbasins_out, paste0(basin_slug, ".shp"))
  out_csv <- file.path(dir_subbasins_out, paste0(basin_slug, ".csv"))
  
  cat("  [", i, "/", nrow(subs), "] ", basin_name_raw, " -> ", basin_slug, "\n", sep = "")
  
  basin_sf <- subs[i, , drop = FALSE] %>%
    mutate(subbasin_name = basin_name_raw) %>%
    make_valid_safe()
  
  hits <- lengths(st_intersects(hrap, basin_sf)) > 0
  hrap_hit <- hrap[hits, ]
  if (nrow(hrap_hit) == 0) {
    empty_tbl <- data.frame(grib_id=integer(), hrap_x=integer(), hrap_y=integer(),
                            bin_area=integer(), subbasin_name=character())
    write.csv(empty_tbl, out_csv, row.names = FALSE, quote = FALSE)
    next
  }
  
  clip_sf <- suppressWarnings(st_intersection(hrap_hit, basin_sf))
  if (nrow(clip_sf) == 0) {
    empty_tbl <- data.frame(grib_id=integer(), hrap_x=integer(), hrap_y=integer(),
                            bin_area=integer(), subbasin_name=character())
    write.csv(empty_tbl, out_csv, row.names = FALSE, quote = FALSE)
    next
  }
  
  clip_sf$bin_area <- area_m2_int(clip_sf)
  clip_sf$bin_area[is.na(clip_sf$bin_area) | clip_sf$bin_area < 0] <- 0L
  
  basin_final <- clip_sf %>%
    group_by(grib_id, hrap_x, hrap_y) %>%
    summarise(
      bin_area = as.integer(sum(bin_area, na.rm = TRUE)),
      subbasin_name = dplyr::first(subbasin_name),
      geometry = st_union(geometry),
      .groups = "drop"
    ) %>%
    arrange(grib_id) %>%
    mutate(
      grib_id  = as.integer(grib_id),
      hrap_x   = as.integer(hrap_x),
      hrap_y   = as.integer(hrap_y),
      bin_area = as.integer(bin_area)
    )
  
  st_write(basin_final, out_shp, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
  write.csv(st_drop_geometry(basin_final), out_csv, row.names = FALSE, quote = FALSE)
}

cat("  Subbasin outputs written to:", dir_subbasins_out, "\n")

# ============================================================
# STEP 6: Convert overall mask shapefile -> GeoPackage (cells.gpkg)
# ============================================================

cat("\nSTEP 6: Write GeoPackage cells.gpkg\n")

cells <- st_read(path_union_shp, quiet = TRUE)
cells <- make_valid_safe(cells)

ensure_dir(dirname(path_cells_gpkg))

# Overwrite layer if exists
st_write(cells, path_cells_gpkg, layer = cells_layer_name,
         driver = "GPKG", delete_layer = TRUE, quiet = TRUE)

cat("  Wrote GeoPackage:\n")
cat("   - File :", path_cells_gpkg, "\n")
cat("   - Layer:", cells_layer_name, "\n")
cat("   - Rows :", nrow(cells), "\n")
cat("   - EPSG :", st_crs(cells)$epsg, "\n")

# ============================================================
# STEP 7: Key validation for ALL output CSVs under prepped/
# ============================================================

cat("\nSTEP 7: Key validation (recursive over prepped/)\n")

csv_files <- list.files(dir_prepped, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)

key <- fread(
  path_key_csv,
  colClasses = list(
    numeric = c("lat", "lon"),
    integer = c("hrap_x", "hrap_y", "grib_id")
  )
)

key_u <- unique(key[, .(grib_id, hrap_x, hrap_y)])
setnames(key_u, c("hrap_x", "hrap_y"), c("hrap_x_key", "hrap_y_key"))
setkey(key_u, grib_id)

cat("  Key rows:", nrow(key_u), "\n")
cat("  CSV files to check:", length(csv_files), "\n\n")

for (f in csv_files) {
  
  x <- fread(f)
  need <- c("grib_id", "hrap_x", "hrap_y")
  missing <- setdiff(need, names(x))
  if (length(missing) > 0) {
    cat("SKIP:", basename(f), " (missing cols: ", paste(missing, collapse = ", "), ")\n", sep = "")
    next
  }
  
  x <- x[, .(
    grib_id = as.integer(trunc(as.numeric(grib_id))),
    hrap_x  = as.integer(trunc(as.numeric(hrap_x))),
    hrap_y  = as.integer(trunc(as.numeric(hrap_y)))
  )]
  
  dup_ids <- x[, .N, by = grib_id][N > 1]
  dup_n <- nrow(dup_ids)
  
  setnames(x, c("hrap_x", "hrap_y"), c("hrap_x_csv", "hrap_y_csv"))
  
  m <- merge(
    x, key_u,
    by = "grib_id",
    all.x = TRUE,
    all.y = FALSE,
    sort = FALSE
  )
  
  miss_id <- unique(m[is.na(hrap_x_key) | is.na(hrap_y_key), .(grib_id, hrap_x_csv, hrap_y_csv)])
  
  mismatch <- unique(m[
    !is.na(hrap_x_key) & (hrap_x_csv != hrap_x_key | hrap_y_csv != hrap_y_key),
    .(grib_id, hrap_x_csv, hrap_y_csv, hrap_x_key, hrap_y_key)
  ])
  
  cat("File:", gsub("^.*prepped[\\\\/]", "prepped/", f), "\n")
  cat("  Rows:", nrow(x), " Unique grib_id:", uniqueN(x$grib_id), "\n")
  cat("  Duplicate grib_id in CSV:", dup_n, "\n")
  cat("  Missing grib_id in key:", nrow(miss_id), "\n")
  cat("  HRAP mismatches vs key:", nrow(mismatch), "\n")
  
  if (dup_n > 0 || nrow(miss_id) > 0 || nrow(mismatch) > 0) {
    dbg_path <- file.path(dir_prepped, paste0("DEBUG_key_check_", tools::file_path_sans_ext(basename(f)), ".csv"))
    fwrite(m, dbg_path)
    stop("FAIL: Issues found in ", basename(f), ". Wrote debug merge to: ", dbg_path)
  }
  
  cat("  PASS\n\n")
}

cat("ALL PASS: Every CSV's (grib_id, hrap_x, hrap_y) matches the authoritative key.\n")

# ============================================================
# STEP 8: Prepare AWS upload artifacts (Parquet + CSV copies)
#  - Writes to: prepped/aws/
#  - boundary mask:
#      <prefix>-boundary-mask.parquet
#      <prefix>-boundary-mask.csv  (EXACT copy of prepped/<prefix>_hrap_final.csv)
#  - area vol calc masks:
#      <prefix>-area-vol-calc-masks.parquet
#      <prefix>-area-vol-calc-masks.csv
# ============================================================

cat("\nSTEP 8: Prepare AWS upload artifacts (prepped/aws)\n")

# deps (additive only)
if (!requireNamespace("arrow", quietly = TRUE)) {
  stop("Package 'arrow' is required for STEP 8. Install with install.packages('arrow').")
}
library(arrow)

dir_aws <- file.path(dir_prepped, "aws")
ensure_dir(dir_aws)

# ----------------------------
# 8A) Boundary mask artifacts
# ----------------------------
aws_boundary_csv  <- file.path(dir_aws, paste0(prefix, "-boundary-mask.csv"))
aws_boundary_parq <- file.path(dir_aws, paste0(prefix, "-boundary-mask.parquet"))

# Exact copy of prepped/<prefix>_hrap_final.csv
if (!file.exists(path_union_csv)) stop("Boundary CSV not found: ", path_union_csv)
ok_copy <- file.copy(path_union_csv, aws_boundary_csv, overwrite = TRUE)
if (!ok_copy) stop("Failed to copy boundary CSV to: ", aws_boundary_csv)

# Parquet version (same content as CSV, types enforced)
boundary_dt <- fread(aws_boundary_csv)
need_b <- c("grib_id", "hrap_x", "hrap_y", "bin_area")
miss_b <- setdiff(need_b, names(boundary_dt))
if (length(miss_b) > 0) stop("Boundary CSV missing cols: ", paste(miss_b, collapse = ", "))

boundary_dt <- boundary_dt[, .(
  grib_id  = as.integer(trunc(as.numeric(grib_id))),
  hrap_x   = as.integer(trunc(as.numeric(hrap_x))),
  hrap_y   = as.integer(trunc(as.numeric(hrap_y))),
  bin_area = as.integer(trunc(as.numeric(bin_area)))
)]

arrow::write_parquet(as.data.frame(boundary_dt), aws_boundary_parq)

cat("  Boundary artifacts written:\n")
cat("   -", aws_boundary_csv,  "\n")
cat("   -", aws_boundary_parq, "\n")

# ----------------------------
# 8B) Area volume calc masks artifacts
# ----------------------------
aws_area_csv  <- file.path(dir_aws, paste0(prefix, "-area-vol-calc-masks.csv"))
aws_area_parq <- file.path(dir_aws, paste0(prefix, "-area-vol-calc-masks.parquet"))

# Gather all subbasin CSVs
sub_csvs <- list.files(dir_subbasins_out, pattern = "\\.csv$", full.names = TRUE)
if (length(sub_csvs) == 0) stop("No subbasin CSVs found in: ", dir_subbasins_out)

sub_list <- vector("list", length(sub_csvs))

for (i in seq_along(sub_csvs)) {
  f <- sub_csvs[i]
  dt_sub <- fread(f)
  
  # Allow empty CSVs; just enforce schema
  if (nrow(dt_sub) == 0) {
    sub_list[[i]] <- data.table(
      grib_id = integer(), hrap_x = integer(), hrap_y = integer(),
      bin_area = integer(), area_name = character()
    )
    next
  }
  
  need_s <- c("grib_id", "hrap_x", "hrap_y", "bin_area", "subbasin_name")
  miss_s <- setdiff(need_s, names(dt_sub))
  if (length(miss_s) > 0) {
    stop("Subbasin CSV missing cols (", basename(f), "): ", paste(miss_s, collapse = ", "))
  }
  
  dt_sub <- dt_sub[, .(
    grib_id   = as.integer(trunc(as.numeric(grib_id))),
    hrap_x    = as.integer(trunc(as.numeric(hrap_x))),
    hrap_y    = as.integer(trunc(as.numeric(hrap_y))),
    bin_area  = as.integer(trunc(as.numeric(bin_area))),
    area_name = as.character(subbasin_name)
  )]
  
  sub_list[[i]] <- dt_sub
}

subs_all <- rbindlist(sub_list, use.names = TRUE, fill = TRUE)

# Add the overall mask rows as another "area" named by prefix
mask_dt <- fread(path_union_csv)
need_m <- c("grib_id", "hrap_x", "hrap_y", "bin_area")
miss_m <- setdiff(need_m, names(mask_dt))
if (length(miss_m) > 0) stop("Mask CSV missing cols: ", paste(miss_m, collapse = ", "))

mask_dt <- mask_dt[, .(
  grib_id   = as.integer(trunc(as.numeric(grib_id))),
  hrap_x    = as.integer(trunc(as.numeric(hrap_x))),
  hrap_y    = as.integer(trunc(as.numeric(hrap_y))),
  bin_area  = as.integer(trunc(as.numeric(bin_area))),
  area_name = as.character(prefix)
)]

# Combine subbasins + mask
area_all <- rbindlist(list(subs_all, mask_dt), use.names = TRUE, fill = TRUE)

# Final column order + sanity checks
area_all <- area_all[, .(grib_id, hrap_x, hrap_y, bin_area, area_name)]
if (anyNA(area_all$grib_id) || anyNA(area_all$hrap_x) || anyNA(area_all$hrap_y)) {
  stop("NAs detected in (grib_id, hrap_x, hrap_y) after coercion in STEP 8.")
}
if (anyNA(area_all$area_name) || any(area_all$area_name == "")) {
  stop("Blank/NA area_name detected in STEP 8.")
}

# Write CSV + Parquet
fwrite(area_all, aws_area_csv)
arrow::write_parquet(as.data.frame(area_all), aws_area_parq)

cat("  Area-vol-calc artifacts written:\n")
cat("   -", aws_area_csv,  "\n")
cat("   -", aws_area_parq, "\n")

cat("STEP 8 DONE: AWS-ready files are in: ", dir_aws, "\n", sep = "")