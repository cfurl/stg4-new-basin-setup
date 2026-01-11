library(sf)

# STEP 1: Read HRAP CONUS shapefile, force ID fields to INTEGER by DROPPING decimals,
# reproject to EPSG:3310 (Cali), and write a new shapefile.

in_shp  <- "C:/stg4-hrap-gis/layers/hrap/hrap_conus_integer.shp"
out_shp <- "C:/stg4-hrap-gis/layers/hanover/hrap_conus_int_nc_32119.shp"

# --- helper: drop decimals (truncate toward 0) without rounding ---
to_int_drop <- function(x, field_name) {
  x_num <- suppressWarnings(as.numeric(x))
  if (anyNA(x_num)) stop("NAs introduced when coercing '", field_name, "' to numeric.")
  as.integer(trunc(x_num))  # trunc() drops decimals (toward 0)
}

# Read
hrap <- st_read(in_shp, quiet = TRUE)

# CRS must exist to transform
if (is.na(st_crs(hrap))) {
  stop("Input layer has no CRS. Check the .prj file or set st_crs(hrap) before transforming.")
}

# Require the exact fields you expect
req <- c("grib_id", "hrap_x", "hrap_y")
missing <- setdiff(req, names(hrap))
if (length(missing) > 0) {
  stop("Missing required fields: ", paste(missing, collapse = ", "))
}

# Force integer fields by DROPPING decimals (no rounding)
hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")

stopifnot(is.integer(hrap$grib_id), is.integer(hrap$hrap_x), is.integer(hrap$hrap_y))

# Reproject to North Carolina
hrap_32119 <- st_transform(hrap, 32119)

# Write shapefile
dir.create(dirname(out_shp), recursive = TRUE, showWarnings = FALSE)
st_write(hrap_32119, out_shp, driver = "ESRI Shapefile", delete_dsn = TRUE)

# Verify on disk (optional but useful)
chk <- st_read(out_shp, quiet = TRUE)
cat("On-disk classes:\n")
print(sapply(chk[, req], class))
cat("Done: ", out_shp, "\n", sep = "")

# 2. Subset the HRAP grid to the watershed (to avoid giant overlays)

hrap_path <- "C:/stg4-hrap-gis/layers/hanover/hrap_conus_int_nc_32119.shp"
ws_path   <- "C:/stg4-hrap-gis/layers/hanover/nh_cf_union.shp"
out_path  <- "C:/stg4-hrap-gis/layers/hanover/hrap_subset_nc_32119.shp"

# Read
hrap <- st_read(hrap_path, quiet = TRUE)
ws   <- st_read(ws_path, quiet = TRUE)

# Ensure same CRS (should already be 3310, but make it bulletproof)
if (st_crs(hrap) != st_crs(ws)) {
  ws <- st_transform(ws, st_crs(hrap))
}

# Make geometry valid (prevents occasional GEOS errors)
hrap <- st_make_valid(hrap)
ws   <- st_make_valid(ws)

# Subset HRAP cells that intersect the dissolved watershed
# (fast filter first, then exact)
hrap_subset <- hrap[st_intersects(hrap, ws, sparse = FALSE), ]

# Write shapefile
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
st_write(hrap_subset, out_path, driver = "ESRI Shapefile", delete_dsn = TRUE)

cat("Wrote subset:", out_path, "\n")
cat("Original HRAP cells:", nrow(hrap), "\n")
cat("Subset HRAP cells   :", nrow(hrap_subset), "\n")

# 3. Clip HRAP bins to each sub-basin + carry sub-basin IDs
library(sf)
library(dplyr)

# -------------------------
# Paths
# -------------------------
hrap_subset_path <- "C:/stg4-hrap-gis/layers/hanover/hrap_subset_nc_32119.shp"
union_path       <- "C:/stg4-hrap-gis/layers/hanover/nh_cf_union.shp"

out_dir <- "C:/stg4-hrap-gis/layers/hanover/prepped"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_union_shp <- file.path(out_dir, "nh_cf_hrap_final.shp")
out_union_csv <- file.path(out_dir, "nh_cf_hrap_final.csv")

# -------------------------
# Helpers
# -------------------------

# Drop decimals (truncate toward 0) -> integer
to_int_drop <- function(x, field_name) {
  x_num <- suppressWarnings(as.numeric(x))
  if (anyNA(x_num)) stop("NAs introduced when coercing '", field_name, "' to numeric.")
  as.integer(trunc(x_num))
}

# Safe validity wrapper (prevents occasional GEOS errors)
make_valid_safe <- function(x) {
  tryCatch(st_make_valid(x), error = function(e) x)
}

# Area in integer m² (drop decimals)
area_m2_int <- function(x_sf) {
  as.integer(trunc(as.numeric(st_area(x_sf))))
}

# -------------------------
# PART 1: UNION CLIP
# -------------------------
cat("Reading HRAP subset...\n")
hrap <- st_read(hrap_subset_path, quiet = TRUE)

cat("Reading watershed union...\n")
ws <- st_read(union_path, quiet = TRUE)

# Ensure CRS matches
if (st_crs(hrap) != st_crs(ws)) {
  ws <- st_transform(ws, st_crs(hrap))
}

# Geometry validity
hrap <- make_valid_safe(hrap)
ws   <- make_valid_safe(ws)

# Keep only fields you need
need_cols <- c("grib_id", "hrap_x", "hrap_y")
missing <- setdiff(need_cols, names(hrap))
if (length(missing) > 0) stop("HRAP layer missing fields: ", paste(missing, collapse = ", "))
hrap <- hrap[, need_cols]

# Force integer IDs (drop decimals)
hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")

# Make sure union is truly a single polygon geometry for clipping
ws_geom <- st_union(st_geometry(ws))
ws_sf   <- st_as_sf(data.frame(union_id = 1), geometry = ws_geom, crs = st_crs(hrap))
ws_sf   <- make_valid_safe(ws_sf)

# ---- IMPORTANT FIX: select ALL HRAP cells intersecting the union ----
hits <- lengths(st_intersects(hrap, ws_sf)) > 0
hrap_hit <- hrap[hits, ]

cat("HRAP subset rows (input): ", nrow(hrap), "\n", sep = "")
cat("HRAP cells intersecting union: ", nrow(hrap_hit), "\n", sep = "")

if (nrow(hrap_hit) == 0) stop("No HRAP cells intersect the watershed union. CRS mismatch? Wrong files?")

# Exact clip: each HRAP cell becomes the portion inside the watershed
cat("Clipping (st_intersection) ... this may take a moment.\n")
clip_sf <- st_intersection(hrap_hit, ws_sf)

cat("Clipped feature pieces: ", nrow(clip_sf), "\n", sep = "")

# Compute area per clipped piece
clip_sf$bin_m2 <- area_m2_int(clip_sf)

# Aggregate: one row per HRAP cell, sum area (some cells may produce multiple pieces)
final_union <- clip_sf %>%
  group_by(grib_id, hrap_x, hrap_y) %>%
  summarise(
    bin_m2 = as.integer(sum(bin_m2, na.rm = TRUE)),
    geometry = st_union(geometry),
    .groups = "drop"
  )

cat("Final unique HRAP cells in union output: ", nrow(final_union), "\n", sep = "")

# Write outputs
cat("Writing shapefile: ", out_union_shp, "\n", sep = "")
st_write(final_union, out_union_shp, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)

cat("Writing CSV: ", out_union_csv, "\n", sep = "")
write.csv(st_drop_geometry(final_union), out_union_csv, row.names = FALSE, quote = FALSE)

cat("DONE.\n")




# DIDN'T DO, NO SUBBASINS YET
# ============================================================
# PART 2 (DROP-IN)
# For each subbasin polygon:
# - exact clip HRAP subset to basin shape
# - compute bin_m2 (m²) per HRAP cell within that basin
# - aggregate to one row per HRAP cell (sum area if split)
# - export BOTH:
#     (a) shapefile: <NAME>.shp
#     (b) csv:       <NAME>.csv
# Output folder:
#   C:\stg4-hrap-gis\layers\scvwd\prepped\
# ============================================================

library(sf)
library(dplyr)

# -------------------------
# Paths
# -------------------------
hrap_subset_path <- "C:/stg4-hrap-gis/layers/scvwd/hrap_scvwd_subset_cali_3310.shp"
subbasins_path   <- "C:/stg4-hrap-gis/layers/scvwd/SCVWD_Major_Watersheds.shp"

out_dir <- "C:/stg4-hrap-gis/layers/scvwd/prepped"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# -------------------------
# Helpers
# -------------------------

# Drop decimals (truncate toward 0) -> integer
to_int_drop <- function(x, field_name) {
  x_num <- suppressWarnings(as.numeric(x))
  if (anyNA(x_num)) stop("NAs introduced when coercing '", field_name, "' to numeric.")
  as.integer(trunc(x_num))
}

# Safe validity wrapper (prevents occasional GEOS errors)
make_valid_safe <- function(x) {
  tryCatch(st_make_valid(x), error = function(e) x)
}

# Area in integer m² (drop decimals)
area_m2_int <- function(x_sf) {
  as.integer(trunc(as.numeric(st_area(x_sf))))
}

# Make filesystem-safe filenames from basin NAME
safe_filename <- function(x) {
  x <- as.character(x)
  x <- gsub("[^A-Za-z0-9_-]+", "_", x)  # spaces/punct -> _
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  if (nchar(x) == 0) "subbasin" else x
}

# -------------------------
# Read inputs
# -------------------------
cat("PART 2: Reading HRAP subset...\n")
hrap <- st_read(hrap_subset_path, quiet = TRUE)

cat("PART 2: Reading subbasins...\n")
subs <- st_read(subbasins_path, quiet = TRUE)

# Ensure CRS matches
if (st_crs(hrap) != st_crs(subs)) {
  subs <- st_transform(subs, st_crs(hrap))
}

# Valid geometry
hrap <- make_valid_safe(hrap)
subs <- make_valid_safe(subs)

# Require NAME and HRAP id fields
if (!("NAME" %in% names(subs))) stop("Subbasins layer missing required field: NAME")

need_cols <- c("grib_id", "hrap_x", "hrap_y")
missing <- setdiff(need_cols, names(hrap))
if (length(missing) > 0) stop("HRAP layer missing fields: ", paste(missing, collapse = ", "))

# Keep only the needed HRAP fields
hrap <- hrap[, need_cols]

# Force integer IDs (drop decimals)
hrap$grib_id <- to_int_drop(hrap$grib_id, "grib_id")
hrap$hrap_x  <- to_int_drop(hrap$hrap_x,  "hrap_x")
hrap$hrap_y  <- to_int_drop(hrap$hrap_y,  "hrap_y")

cat("PART 2: Subbasins to process: ", nrow(subs), "\n", sep = "")

# -------------------------
# Process each subbasin
# -------------------------
for (i in seq_len(nrow(subs))) {
  
  basin_name_raw <- as.character(subs$NAME[i])
  basin_slug     <- safe_filename(basin_name_raw)
  
  out_shp <- file.path(out_dir, paste0(basin_slug, ".shp"))
  out_csv <- file.path(out_dir, paste0(basin_slug, ".csv"))
  
  cat("\n[", i, "/", nrow(subs), "] ", basin_name_raw, " -> ", basin_slug, "\n", sep = "")
  
  # Make a single-feature sf for this basin (keeps attributes if you want them later)
  basin_sf <- subs[i, , drop = FALSE]
  basin_sf <- basin_sf %>% mutate(basin_name = basin_name_raw)  # stable name field
  basin_sf <- make_valid_safe(basin_sf)
  
  # Fast pre-filter: keep all HRAP cells that intersect this basin
  hits <- lengths(st_intersects(hrap, basin_sf)) > 0
  hrap_hit <- hrap[hits, ]
  
  cat("  HRAP cells intersecting basin (pre-filter): ", nrow(hrap_hit), "\n", sep = "")
  
  if (nrow(hrap_hit) == 0) {
    # Write empty CSV + empty shapefile (optional, but keeps outputs predictable)
    empty_tbl <- data.frame(grib_id=integer(), hrap_x=integer(), hrap_y=integer(), bin_m2=integer())
    write.csv(empty_tbl, out_csv, row.names = FALSE, quote = FALSE)
    
    empty_sf <- st_as_sf(empty_tbl, coords = c("hrap_x","hrap_y"), crs = st_crs(hrap))
    empty_sf <- empty_sf[0, ]  # ensure 0 rows
    st_write(empty_sf, out_shp, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
    
    cat("  Wrote EMPTY basin outputs (no intersecting HRAP cells).\n")
    next
  }
  
  # Exact clip: HRAP cell pieces inside this basin boundary
  cat("  Clipping (st_intersection) ...\n")
  clip_sf <- st_intersection(hrap_hit, basin_sf)
  
  cat("  Clipped feature pieces: ", nrow(clip_sf), "\n", sep = "")
  if (nrow(clip_sf) == 0) {
    empty_tbl <- data.frame(grib_id=integer(), hrap_x=integer(), hrap_y=integer(), bin_m2=integer())
    write.csv(empty_tbl, out_csv, row.names = FALSE, quote = FALSE)
    cat("  Wrote EMPTY CSV (intersection produced 0 pieces).\n")
    next
  }
  
  # Compute area per clipped piece (m²)
  clip_sf$bin_m2 <- area_m2_int(clip_sf)
  
  # Aggregate to ONE row per HRAP cell
  basin_final <- clip_sf %>%
    group_by(grib_id, hrap_x, hrap_y) %>%
    summarise(
      bin_m2 = as.integer(sum(bin_m2, na.rm = TRUE)),
      geometry = st_union(geometry),
      .groups = "drop"
    ) %>%
    arrange(grib_id)
  
  cat("  Final unique HRAP cells in basin: ", nrow(basin_final), "\n", sep = "")
  
  # Write shapefile + CSV
  cat("  Writing shapefile: ", out_shp, "\n", sep = "")
  st_write(basin_final, out_shp, driver = "ESRI Shapefile", delete_dsn = TRUE, quiet = TRUE)
  
  cat("  Writing CSV: ", out_csv, "\n", sep = "")
  write.csv(st_drop_geometry(basin_final), out_csv, row.names = FALSE, quote = FALSE)
}

cat("\nPART 2 DONE.\n")

## Check against authorative key

library(data.table)

# -------------------------
# Paths
# -------------------------
key_path    <- "C:/stg4-hrap-gis/layers/hrap/grib2_lat_lon_pt_with_grib_decimal_fix.csv"
prepped_dir <- "C:/stg4-hrap-gis/layers/hanover/prepped"

csv_files <- list.files(prepped_dir, pattern = "\\.csv$", full.names = TRUE)

# -------------------------
# Read key (authoritative)
# -------------------------
key <- fread(
  key_path,
  colClasses = list(
    numeric = c("lat", "lon"),
    integer = c("hrap_x", "hrap_y", "grib_id")
  )
)

key_u <- unique(key[, .(grib_id, hrap_x, hrap_y)])
setnames(key_u, c("hrap_x", "hrap_y"), c("hrap_x_key", "hrap_y_key"))
setkey(key_u, grib_id)

cat("Key rows:", nrow(key_u), "\n")
cat("CSV files to check:", length(csv_files), "\n\n")

# -------------------------
# Check each CSV
# -------------------------
for (f in csv_files) {
  x <- fread(f)
  
  need <- c("grib_id", "hrap_x", "hrap_y")
  missing <- setdiff(need, names(x))
  if (length(missing) > 0) {
    cat("SKIP:", basename(f), " (missing cols: ", paste(missing, collapse = ", "), ")\n", sep = "")
    next
  }
  
  # Keep only what we need, and force integer (drop decimals)
  x <- x[, .(
    grib_id = as.integer(trunc(as.numeric(grib_id))),
    hrap_x  = as.integer(trunc(as.numeric(hrap_x))),
    hrap_y  = as.integer(trunc(as.numeric(hrap_y)))
  )]
  
  # Duplicate grib_id check inside the CSV itself
  dup_ids <- x[, .N, by = grib_id][N > 1]
  dup_n <- nrow(dup_ids)
  
  # Rename CSV columns so we can compare cleanly
  setnames(x, c("hrap_x", "hrap_y"), c("hrap_x_csv", "hrap_y_csv"))
  
  # Merge: keep all rows from CSV, attach authoritative HRAP coords from key
  m <- merge(
    x, key_u,
    by = "grib_id",
    all.x = TRUE,
    all.y = FALSE,
    sort = FALSE
  )
  
  # 1) grib_id present in CSV but not found in key
  miss_id <- m[is.na(hrap_x_key) | is.na(hrap_y_key),
               .(grib_id, hrap_x_csv, hrap_y_csv)]
  miss_id <- unique(miss_id)
  
  # 2) grib_id exists in both, but HRAP coords disagree
  mismatch <- m[!is.na(hrap_x_key) & (hrap_x_csv != hrap_x_key | hrap_y_csv != hrap_y_key),
                .(grib_id,
                  hrap_x_csv, hrap_y_csv,
                  hrap_x_key, hrap_y_key)]
  mismatch <- unique(mismatch)
  
  cat("File:", basename(f), "\n")
  cat("  Rows:", nrow(x), " Unique grib_id:", uniqueN(x$grib_id), "\n")
  cat("  Duplicate grib_id in CSV:", dup_n, "\n")
  cat("  Missing grib_id in key:", nrow(miss_id), "\n")
  cat("  HRAP mismatches vs key:", nrow(mismatch), "\n")
  
  if (dup_n > 0) {
    cat("  Example duplicate grib_id counts:\n")
    print(head(dup_ids[order(-N)], 20))
  }
  
  if (nrow(miss_id) > 0) {
    cat("  Example missing IDs:\n")
    print(head(miss_id, 20))
  }
  
  if (nrow(mismatch) > 0) {
    cat("  Example mismatches:\n")
    print(head(mismatch, 20))
  }
  
  # If anything is wrong, write a debug file and stop
  if (dup_n > 0 || nrow(miss_id) > 0 || nrow(mismatch) > 0) {
    dbg_path <- file.path(prepped_dir, paste0("DEBUG_key_check_", tools::file_path_sans_ext(basename(f)), ".csv"))
    fwrite(m, dbg_path)
    stop("FAIL: Issues found in ", basename(f), ". Wrote debug merge to: ", dbg_path)
  }
  
  cat("  PASS\n\n")
}

cat("ALL PASS: Every CSV's (grib_id, hrap_x, hrap_y) matches the authoritative key.\n")
