
library(sf)
library(httr)
library(jsonlite)

# ============================================================
# CONFIGURATION
# ============================================================

basin_file <- paste0(
  "C:/stg4-hrap-gis/layers/hanover/",
  "cape_fear_basin.shp"
)

output_folder <- paste0(
  "C:/stg4-hrap-gis/layers/hanover/",
  "waterbodies"
)

# 0 = exact watershed boundary
# 20 = watershed plus 20 km surrounding area
buffer_km <- 0

# FALSE = lakes, ponds, reservoirs only
# TRUE = also include polygonal rivers
include_river_polygons <- FALSE

chunk_size <- 100

query_url <- paste0(
  "https://3dhp.nationalmap.gov/arcgis/rest/services/",
  "usgs_3dhp_all/FeatureServer/60/query"
)

# ============================================================
# PREPARE WATERSHED BOUNDARY
# ============================================================

stopifnot(file.exists(basin_file))

dir.create(
  output_folder,
  recursive = TRUE,
  showWarnings = FALSE
)

basin <- st_read(basin_file, quiet = TRUE)

if (is.na(st_crs(basin))) {
  stop("Basin has no coordinate reference system.")
}

basin <- st_make_valid(basin)

# Dissolve all components, then transform to equal-area CRS
footprint <- st_sf(
  geometry = st_union(
    st_geometry(st_transform(basin, 5070))
  )
)

if (buffer_km > 0) {
  footprint <- st_buffer(
    footprint,
    dist = buffer_km * 1000
  )
}

footprint <- st_make_valid(footprint)

# USGS API spatial query uses EPSG:3857
bb <- st_bbox(st_transform(footprint, 3857))
bbox_text <- paste(as.numeric(bb), collapse = ",")

where_clause <- if (include_river_polygons) {
  "featuretype IN (1,3)"
} else {
  "featuretype = 3"
}

cat("\nUSGS filter:", where_clause, "\n")
cat("Watershed buffer:", buffer_km, "km\n")

# ============================================================
# QUERY USGS FEATURE IDS
# ============================================================

response <- GET(
  query_url,
  query = list(
    where = where_clause,
    geometry = bbox_text,
    geometryType = "esriGeometryEnvelope",
    inSR = "3857",
    spatialRel = "esriSpatialRelIntersects",
    returnIdsOnly = "true",
    f = "json"
  ),
  timeout(120)
)

stop_for_status(response)

result <- fromJSON(
  content(response, as = "text", encoding = "UTF-8")
)

if (!is.null(result$error)) {
  stop("USGS query error: ",
       toJSON(result$error, auto_unbox = TRUE))
}

object_ids <- result$objectIds

if (is.null(object_ids) || length(object_ids) == 0) {
  stop("No USGS waterbodies were found.")
}

cat("Waterbodies found:", length(object_ids), "\n")

# ============================================================
# DOWNLOAD WATERBODY POLYGONS
# ============================================================

id_chunks <- split(
  object_ids,
  ceiling(seq_along(object_ids) / chunk_size)
)

parts <- vector("list", length(id_chunks))

for (i in seq_along(id_chunks)) {
  
  cat(
    "Downloading chunk", i,
    "of", length(id_chunks), "\n"
  )
  
  response <- RETRY(
    "POST",
    query_url,
    body = list(
      objectIds = paste(id_chunks[[i]], collapse = ","),
      outFields = paste(
        "id3dhp",
        "gnisidlabel",
        "featuretype",
        "featuretypelabel",
        "areasqkm",
        sep = ","
      ),
      returnGeometry = "true",
      outSR = "4326",
      f = "geojson"
    ),
    encode = "form",
    times = 3,
    pause_base = 2,
    timeout(120)
  )
  
  stop_for_status(response)
  
  geojson_text <- content(
    response,
    as = "text",
    encoding = "UTF-8"
  )
  
  if (grepl(
    '"error"[[:space:]]*:',
    substr(geojson_text, 1, 1000)
  )) {
    stop("USGS service error on chunk ", i)
  }
  
  temp_file <- tempfile(fileext = ".geojson")
  
  writeLines(
    geojson_text,
    temp_file,
    useBytes = TRUE
  )
  
  parts[[i]] <- tryCatch(
    st_read(temp_file, quiet = TRUE),
    finally = unlink(temp_file)
  )
}

parts <- Filter(
  function(x) !is.null(x) && nrow(x) > 0,
  parts
)

if (length(parts) == 0) {
  stop("No waterbody polygons downloaded.")
}

waterbodies <- do.call(rbind, parts)

cat(
  "\nDownloaded polygons:",
  nrow(waterbodies), "\n"
)

# ============================================================
# CLIP TO CAPE FEAR + BUFFER
# ============================================================

waterbodies <- st_transform(waterbodies, 5070)
waterbodies <- st_make_valid(waterbodies)

cat("Clipping waterbodies...\n")

waterbodies <- suppressWarnings(
  st_intersection(waterbodies, footprint)
)

waterbodies <- st_collection_extract(
  waterbodies,
  "POLYGON",
  warn = FALSE
)

waterbodies <- waterbodies[
  !st_is_empty(waterbodies),
]

if (nrow(waterbodies) == 0) {
  stop("No waterbodies remain after clipping.")
}

# Calculate actual polygon area after clipping
waterbodies$area_km2 <- as.numeric(
  st_area(waterbodies)
) / 1e6

# Retain attributes needed for MapLibre
waterbodies <- waterbodies[
  c(
    "id3dhp",
    "gnisidlabel",
    "featuretype",
    "featuretypelabel",
    "area_km2"
  )
]

# ============================================================
# WRITE MASTER GEOPACKAGE
# ============================================================

master_file <- file.path(
  output_folder,
  "cape_fear_waterbodies_all.gpkg"
)

if (file.exists(master_file)) {
  file.remove(master_file)
}

st_write(
  waterbodies,
  master_file,
  layer = "cape_fear_waterbodies",
  quiet = TRUE
)

# ============================================================
# DIAGNOSTICS — WATERBODY SIZE DISTRIBUTION
# ============================================================

size_breaks <- c(
  0, 0.001, 0.01, 0.05, 0.5, 2, Inf
)

size_labels <- c(
  "<0.001",
  "0.001-0.01",
  "0.01-0.05",
  "0.05-0.5",
  "0.5-2",
  ">=2"
)

size_bin <- cut(
  waterbodies$area_km2,
  breaks = size_breaks,
  labels = size_labels,
  right = FALSE
)

size_summary <- as.data.frame(
  table(size_bin)
)

names(size_summary) <- c(
  "area_km2_range",
  "features"
)

write.csv(
  size_summary,
  file.path(
    output_folder,
    "cape_fear_waterbody_size_summary.csv"
  ),
  row.names = FALSE
)

# ============================================================
# SUMMARY
# ============================================================

cat("\nCOMPLETE\n")
cat("Waterbodies:", nrow(waterbodies), "\n")

print(size_summary, row.names = FALSE)

largest <- st_drop_geometry(waterbodies)[
  , c("gnisidlabel", "area_km2")
]

largest <- largest[
  order(-largest$area_km2),
]

cat("\nLargest waterbodies:\n")
print(head(largest, 15), row.names = FALSE)

cat("\nSaved:\n", master_file, "\n")
