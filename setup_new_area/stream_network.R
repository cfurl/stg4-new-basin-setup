# =============================================================================
# USGS 3DHP STREAM NETWORK BUILDER
#
# Purpose:
#   Create a locally stored hierarchy of USGS 3DHP stream networks for a
#   watershed polygon.
#
# Inputs:
#   - Basin polygon from .gpkg or .shp
#
# Outputs:
#   - Complete clipped ordered Flowline network
#   - One GeoPackage for every stream-order threshold:
#
#       *_streamorder_gt0.gpkg   = streamorder > 0 = orders 1+
#       *_streamorder_gt1.gpkg   = streamorder > 1 = orders 2+
#       *_streamorder_gt2.gpkg   = streamorder > 2 = orders 3+
#       *_streamorder_gt3.gpkg   = streamorder > 3 = orders 4+
#       ...
#
#   - Diagnostic CSV files describing stream order and feature types
#
# Hydrography source:
#   USGS 3D Hydrography Program (3DHP)
#   Flowline FeatureServer
#
# Important:
#   This script intentionally DOES NOT restrict featuretype to Channel Line.
#   Connector Flowline types are retained when they have valid streamorder
#   values so the network remains connected.
# =============================================================================


library(sf)
library(dplyr)
library(httr)
library(jsonlite)


# =============================================================================
# CONFIG
# =============================================================================

# -----------------------------------------------------------------------------
# Project / output naming
# -----------------------------------------------------------------------------

project_id <- "devils"

# Used in filenames such as:
#
#   devils_streamorder_gt0.gpkg
#   devils_streamorder_gt1.gpkg
#
output_prefix <- "devils"


# -----------------------------------------------------------------------------
# Basin polygon input
#
# May be:
#   .gpkg
#   .shp
# -----------------------------------------------------------------------------

basin_file <- paste0(
  "C:/stg4-hrap-gis/layers/devils/basins/",
  "devils_watersheds.gpkg"
)


# -----------------------------------------------------------------------------
# GeoPackage layer
#
# For a .gpkg:
#   Specify the layer name.
#
# For a .shp:
#   This setting is ignored.
#
# Set to NULL to automatically use the layer if the GeoPackage contains
# exactly one layer.
# -----------------------------------------------------------------------------

basin_layer <- "devils_watersheds"


# -----------------------------------------------------------------------------
# Select watershed polygon
#
# The input GIS layer may contain multiple basin polygons.
#
# For Devils:
#
#   BEG_name == "HUCDevils"
#
# Change these two values for another watershed.
# -----------------------------------------------------------------------------

basin_select_field <- "BEG_name"

basin_select_value <- "HUCDevils"


# -----------------------------------------------------------------------------
# Output folder
#
# ALL generated stream files will be written here.
# Existing source basin GIS files are never modified.
# -----------------------------------------------------------------------------

output_folder <- paste0(
  "C:/stg4-hrap-gis/layers/devils/streams/",
  "all_flowline_types"
)


# -----------------------------------------------------------------------------
# Stream-order download minimum
#
# 0 means:
#
#   streamorder > 0
#
# which downloads every ordered stream beginning with first-order streams.
#
# Recommended:
#
#   0
# -----------------------------------------------------------------------------

download_threshold <- 0


# -----------------------------------------------------------------------------
# Output thresholds
#
# NULL:
#   Automatically generate every useful threshold from 0 through N.
#
# Example:
#
#   NULL
#
# could create:
#
#   gt0
#   gt1
#   gt2
#   gt3
#   gt4
#   gt5
#   gt6
#
# depending on the maximum stream order present.
#
# Or manually specify:
#
#   output_thresholds <- c(0, 1, 2, 3, 4)
# -----------------------------------------------------------------------------

output_thresholds <- NULL


# -----------------------------------------------------------------------------
# Save complete clipped network
#
# This is useful because it gives you one master local dataset containing
# all ordered Flowline features before stream-order filtering.
# -----------------------------------------------------------------------------

write_master_network <- TRUE


# -----------------------------------------------------------------------------
# Write diagnostic CSV files
# -----------------------------------------------------------------------------

write_diagnostics <- TRUE


# -----------------------------------------------------------------------------
# USGS download chunk size
#
# POST is used instead of GET for feature downloads to avoid HTTP 414 errors.
#
# 500 has worked well.
# -----------------------------------------------------------------------------

chunk_size <- 500


# -----------------------------------------------------------------------------
# USGS 3DHP Flowline service
#
# Normally do not change this.
# -----------------------------------------------------------------------------

query_url <- paste0(
  "https://3dhp.nationalmap.gov/arcgis/rest/services/",
  "usgs_3dhp_all/FeatureServer/50/query"
)


# =============================================================================
# END CONFIG
# =============================================================================



# =============================================================================
# SETUP
# =============================================================================

options(
  na.print = "NA"
)

dir.create(
  output_folder,
  recursive = TRUE,
  showWarnings = FALSE
)


# -----------------------------------------------------------------------------
# Console table helper
#
# Avoids print.data.frame() issues.
# -----------------------------------------------------------------------------

show_table <- function(x) {
  
  x <- as.data.frame(x)
  
  if (nrow(x) == 0) {
    
    cat("<no rows>\n")
    
    return(
      invisible(x)
    )
  }
  
  write.table(
    x,
    row.names = FALSE,
    quote = FALSE,
    sep = "\t",
    na = "NA"
  )
  
  invisible(x)
}


# =============================================================================
# READ BASIN GIS FILE
# =============================================================================

file_ext <- tolower(
  tools::file_ext(
    basin_file
  )
)


if (!file.exists(basin_file)) {
  
  stop(
    "Basin GIS file does not exist:\n",
    basin_file
  )
}


# -----------------------------------------------------------------------------
# GeoPackage
# -----------------------------------------------------------------------------

if (file_ext == "gpkg") {
  
  available_layers <- st_layers(
    basin_file
  )$name
  
  if (is.null(basin_layer)) {
    
    if (length(available_layers) != 1) {
      
      stop(
        "GeoPackage contains more than one layer.\n",
        "Set basin_layer in CONFIG.\n\n",
        "Available layers:\n",
        paste(
          available_layers,
          collapse = "\n"
        )
      )
    }
    
    basin_layer_use <- available_layers[1]
    
  } else {
    
    basin_layer_use <- basin_layer
    
    if (!basin_layer_use %in% available_layers) {
      
      stop(
        "Configured basin_layer was not found.\n\n",
        "Configured:\n",
        basin_layer_use,
        "\n\nAvailable layers:\n",
        paste(
          available_layers,
          collapse = "\n"
        )
      )
    }
  }
  
  basins <- st_read(
    basin_file,
    layer = basin_layer_use,
    quiet = TRUE
  )
  
  
  # -----------------------------------------------------------------------------
  # Shapefile
  # -----------------------------------------------------------------------------
  
} else if (file_ext == "shp") {
  
  basins <- st_read(
    basin_file,
    quiet = TRUE
  )
  
  
  # -----------------------------------------------------------------------------
  # Unsupported input
  # -----------------------------------------------------------------------------
  
} else {
  
  stop(
    "Unsupported basin GIS format: .",
    file_ext,
    "\n\nUse .gpkg or .shp."
  )
}


cat("\n")
cat("============================================================\n")
cat("BASIN INPUT\n")
cat("============================================================\n\n")

cat(
  "Project:      ",
  project_id,
  "\n",
  sep = ""
)

cat(
  "Input file:   ",
  basin_file,
  "\n",
  sep = ""
)

cat(
  "Features:     ",
  nrow(basins),
  "\n",
  sep = ""
)

cat(
  "Selection:    ",
  basin_select_field,
  " = ",
  basin_select_value,
  "\n",
  sep = ""
)

cat(
  "Output:       ",
  output_folder,
  "\n",
  sep = ""
)


# =============================================================================
# SELECT WATERSHED
# =============================================================================

if (!basin_select_field %in% names(basins)) {
  
  stop(
    "Basin selection field does not exist:\n",
    basin_select_field,
    "\n\nAvailable fields:\n",
    paste(
      names(basins),
      collapse = "\n"
    )
  )
}


boundary <- basins[
  as.character(
    basins[[basin_select_field]]
  ) == as.character(
    basin_select_value
  ),
]


if (nrow(boundary) == 0) {
  
  stop(
    "No basin feature matched:\n\n",
    basin_select_field,
    " = ",
    basin_select_value
  )
}


if (nrow(boundary) > 1) {
  
  cat(
    "\nWARNING: ",
    nrow(boundary),
    " features matched the basin selection.\n",
    "They will be dissolved into one watershed boundary.\n",
    sep = ""
  )
}


# -----------------------------------------------------------------------------
# Validate and dissolve selected polygon(s)
# -----------------------------------------------------------------------------

boundary <- boundary %>%
  st_make_valid()


boundary_geom <- st_union(
  st_geometry(boundary)
)


boundary_sf <- st_sf(
  watershed = project_id,
  geometry = boundary_geom,
  crs = st_crs(boundary)
)


cat(
  "\nSelected watershed feature(s): ",
  nrow(boundary),
  "\n",
  sep = ""
)

cat(
  "Watershed CRS: ",
  st_crs(boundary_sf)$input,
  "\n",
  sep = ""
)


# =============================================================================
# BUILD USGS QUERY EXTENT
# =============================================================================

# USGS service uses EPSG:3857

boundary_3857 <- st_transform(
  boundary_sf,
  3857
)


bb <- st_bbox(
  boundary_3857
)


bbox_text <- paste(
  bb["xmin"],
  bb["ymin"],
  bb["xmax"],
  bb["ymax"],
  sep = ","
)


cat("\nUSGS query bounding box:\n")

cat(
  "  xmin: ",
  bb["xmin"],
  "\n",
  sep = ""
)

cat(
  "  ymin: ",
  bb["ymin"],
  "\n",
  sep = ""
)

cat(
  "  xmax: ",
  bb["xmax"],
  "\n",
  sep = ""
)

cat(
  "  ymax: ",
  bb["ymax"],
  "\n",
  sep = ""
)


# =============================================================================
# QUERY USGS OBJECT IDS
# =============================================================================

where_clause <- paste0(
  "streamorder > ",
  download_threshold
)


cat("\n")
cat("============================================================\n")
cat("USGS 3DHP DOWNLOAD\n")
cat("============================================================\n\n")

cat(
  "Server filter: ",
  where_clause,
  "\n",
  sep = ""
)

cat(
  "Feature type restriction: NONE\n"
)

cat(
  "Requesting ObjectIDs...\n"
)


id_response <- GET(
  query_url,
  query = list(
    where = where_clause,
    geometry = bbox_text,
    geometryType = "esriGeometryEnvelope",
    inSR = "3857",
    spatialRel = "esriSpatialRelIntersects",
    returnIdsOnly = "true",
    f = "json"
  )
)


stop_for_status(
  id_response
)


id_text <- content(
  id_response,
  as = "text",
  encoding = "UTF-8"
)


id_result <- fromJSON(
  id_text
)


# -----------------------------------------------------------------------------
# Service error
# -----------------------------------------------------------------------------

if (!is.null(id_result$error)) {
  
  cat(
    "\nUSGS returned an error:\n"
  )
  
  str(
    id_result$error
  )
  
  stop(
    "USGS ObjectID query failed."
  )
}


object_ids <- id_result$objectIds


if (
  is.null(object_ids) ||
  length(object_ids) == 0
) {
  
  stop(
    "No matching USGS Flowline features were returned."
  )
}


cat(
  "Matching features in watershed bounding box: ",
  length(object_ids),
  "\n",
  sep = ""
)


# =============================================================================
# SPLIT DOWNLOAD INTO CHUNKS
# =============================================================================

id_chunks <- split(
  object_ids,
  ceiling(
    seq_along(object_ids) /
      chunk_size
  )
)


cat(
  "Download chunk size: ",
  chunk_size,
  "\n",
  sep = ""
)

cat(
  "Number of chunks: ",
  length(id_chunks),
  "\n\n",
  sep = ""
)


# =============================================================================
# DOWNLOAD FLOWLINE FEATURES
# =============================================================================

stream_list <- vector(
  "list",
  length(id_chunks)
)


for (i in seq_along(id_chunks)) {
  
  cat(
    "Downloading chunk ",
    i,
    " of ",
    length(id_chunks),
    "...\n",
    sep = ""
  )
  
  
  ids <- paste(
    id_chunks[[i]],
    collapse = ","
  )
  
  
  # ---------------------------------------------------------------------------
  # POST is intentional.
  #
  # Sending hundreds of ObjectIDs through GET can produce:
  #
  #   HTTP 414 Request-URI Too Long
  # ---------------------------------------------------------------------------
  
  response <- POST(
    query_url,
    body = list(
      objectIds = ids,
      outFields = "*",
      returnGeometry = "true",
      outSR = "4326",
      f = "geojson"
    ),
    encode = "form"
  )
  
  
  stop_for_status(
    response
  )
  
  
  geojson_text <- content(
    response,
    as = "text",
    encoding = "UTF-8"
  )
  
  
  # ---------------------------------------------------------------------------
  # ArcGIS can sometimes return an error object with HTTP status 200.
  # ---------------------------------------------------------------------------
  
  possible_error <- tryCatch(
    fromJSON(
      geojson_text
    ),
    error = function(e) NULL
  )
  
  
  if (
    !is.null(possible_error) &&
    !is.null(possible_error$error)
  ) {
    
    cat(
      "\nUSGS service error on chunk ",
      i,
      ":\n",
      sep = ""
    )
    
    str(
      possible_error$error
    )
    
    stop(
      "USGS Flowline download failed."
    )
  }
  
  
  # ---------------------------------------------------------------------------
  # Read GeoJSON
  # ---------------------------------------------------------------------------
  
  temp_geojson <- tempfile(
    fileext = ".geojson"
  )
  
  
  writeLines(
    geojson_text,
    temp_geojson,
    useBytes = TRUE
  )
  
  
  stream_list[[i]] <- st_read(
    temp_geojson,
    quiet = TRUE
  )
  
  
  unlink(
    temp_geojson
  )
}


# =============================================================================
# COMBINE DOWNLOADED FEATURES
# =============================================================================

streams <- do.call(
  rbind,
  stream_list
)


cat(
  "\nDownloaded ",
  nrow(streams),
  " Flowline features.\n",
  sep = ""
)


if (!"streamorder" %in% names(streams)) {
  
  stop(
    "USGS data does not contain a streamorder field."
  )
}


# =============================================================================
# TRANSFORM AND CLIP TO ACTUAL WATERSHED
# =============================================================================

streams <- st_transform(
  streams,
  st_crs(boundary_sf)
)


cat(
  "\nClipping Flowlines to actual watershed polygon...\n"
)


streams_huc <- suppressWarnings(
  st_intersection(
    streams,
    boundary_sf
  )
)


streams_huc <- streams_huc %>%
  filter(
    !st_is_empty(geometry),
    !is.na(streamorder),
    streamorder > download_threshold
  )


cat(
  "Features after watershed clipping: ",
  nrow(streams_huc),
  "\n",
  sep = ""
)


if (nrow(streams_huc) == 0) {
  
  stop(
    "No Flowline features remain after clipping."
  )
}


# =============================================================================
# DIAGNOSTICS
# =============================================================================

# -----------------------------------------------------------------------------
# Stream order
# -----------------------------------------------------------------------------

order_summary <- streams_huc %>%
  st_drop_geometry() %>%
  count(
    streamorder,
    name = "features"
  ) %>%
  arrange(
    streamorder
  )


cat("\n")
cat("============================================================\n")
cat("STREAM ORDER COUNTS\n")
cat("============================================================\n\n")


show_table(
  order_summary
)


# -----------------------------------------------------------------------------
# Feature types
# -----------------------------------------------------------------------------

feature_type_summary <- NULL


if (
  all(
    c(
      "featuretype",
      "featuretypelabel"
    ) %in% names(streams_huc)
  )
) {
  
  feature_type_summary <- streams_huc %>%
    st_drop_geometry() %>%
    count(
      featuretype,
      featuretypelabel,
      name = "features"
    ) %>%
    arrange(
      featuretype
    )
  
} else if (
  "featuretype" %in% names(streams_huc)
) {
  
  feature_type_summary <- streams_huc %>%
    st_drop_geometry() %>%
    count(
      featuretype,
      name = "features"
    ) %>%
    arrange(
      featuretype
    )
}


cat("\n")
cat("============================================================\n")
cat("FLOWLINE FEATURE TYPES\n")
cat("============================================================\n\n")


if (!is.null(feature_type_summary)) {
  
  show_table(
    feature_type_summary
  )
  
} else {
  
  cat(
    "No featuretype field available.\n"
  )
}


# -----------------------------------------------------------------------------
# Feature type by stream order
# -----------------------------------------------------------------------------

type_order_summary <- NULL


if (
  all(
    c(
      "featuretype",
      "featuretypelabel"
    ) %in% names(streams_huc)
  )
) {
  
  type_order_summary <- streams_huc %>%
    st_drop_geometry() %>%
    count(
      streamorder,
      featuretype,
      featuretypelabel,
      name = "features"
    ) %>%
    arrange(
      streamorder,
      featuretype
    )
  
} else if (
  "featuretype" %in% names(streams_huc)
) {
  
  type_order_summary <- streams_huc %>%
    st_drop_geometry() %>%
    count(
      streamorder,
      featuretype,
      name = "features"
    ) %>%
    arrange(
      streamorder,
      featuretype
    )
}


# =============================================================================
# SAVE DIAGNOSTIC CSV FILES
# =============================================================================

if (write_diagnostics) {
  
  write.csv(
    order_summary,
    file.path(
      output_folder,
      paste0(
        output_prefix,
        "_streamorder_summary.csv"
      )
    ),
    row.names = FALSE,
    na = ""
  )
  
  
  if (!is.null(feature_type_summary)) {
    
    write.csv(
      feature_type_summary,
      file.path(
        output_folder,
        paste0(
          output_prefix,
          "_featuretype_summary.csv"
        )
      ),
      row.names = FALSE,
      na = ""
    )
  }
  
  
  if (!is.null(type_order_summary)) {
    
    write.csv(
      type_order_summary,
      file.path(
        output_folder,
        paste0(
          output_prefix,
          "_featuretype_by_streamorder.csv"
        )
      ),
      row.names = FALSE,
      na = ""
    )
  }
}


# =============================================================================
# WRITE MASTER CLIPPED NETWORK
# =============================================================================

if (write_master_network) {
  
  master_file <- file.path(
    output_folder,
    paste0(
      output_prefix,
      "_streams_all_ordered.gpkg"
    )
  )
  
  
  master_layer <- paste0(
    output_prefix,
    "_streams_all_ordered"
  )
  
  
  if (file.exists(master_file)) {
    
    file.remove(
      master_file
    )
  }
  
  
  st_write(
    streams_huc,
    master_file,
    layer = master_layer,
    quiet = TRUE
  )
  
  
  cat(
    "\nWrote master network:\n",
    master_file,
    "\n",
    sep = ""
  )
}


# =============================================================================
# DETERMINE STREAM ORDER RANGE
# =============================================================================

max_streamorder <- max(
  streams_huc$streamorder,
  na.rm = TRUE
)


cat(
  "\nMaximum stream order in watershed: ",
  max_streamorder,
  "\n",
  sep = ""
)


# -----------------------------------------------------------------------------
# Automatically create all thresholds unless manually supplied
# -----------------------------------------------------------------------------

if (is.null(output_thresholds)) {
  
  output_thresholds <- seq(
    from = download_threshold,
    to = max_streamorder - 1
  )
}


# Remove thresholds which cannot produce any features

output_thresholds <- output_thresholds[
  output_thresholds < max_streamorder
]


output_thresholds <- sort(
  unique(
    output_thresholds
  )
)


# =============================================================================
# WRITE STREAM-ORDER GEOPACKAGES
# =============================================================================

cat("\n")
cat("============================================================\n")
cat("WRITING STREAM-ORDER NETWORKS\n")
cat("============================================================\n\n")


output_summary <- data.frame(
  threshold = integer(),
  minimum_order = integer(),
  maximum_order = integer(),
  features = integer(),
  file = character(),
  stringsAsFactors = FALSE
)


for (threshold in output_thresholds) {
  
  
  streams_out <- streams_huc %>%
    filter(
      streamorder > threshold
    )
  
  
  layer_name <- paste0(
    output_prefix,
    "_streamorder_gt",
    threshold
  )
  
  
  output_file <- file.path(
    output_folder,
    paste0(
      layer_name,
      ".gpkg"
    )
  )
  
  
  if (file.exists(output_file)) {
    
    file.remove(
      output_file
    )
  }
  
  
  st_write(
    streams_out,
    output_file,
    layer = layer_name,
    quiet = TRUE
  )
  
  
  cat(
    "Wrote ",
    basename(output_file),
    " | streamorder > ",
    threshold,
    " | orders ",
    threshold + 1,
    "-",
    max_streamorder,
    " | ",
    nrow(streams_out),
    " features\n",
    sep = ""
  )
  
  
  output_summary <- bind_rows(
    output_summary,
    data.frame(
      threshold = threshold,
      minimum_order = threshold + 1,
      maximum_order = max_streamorder,
      features = nrow(streams_out),
      file = output_file,
      stringsAsFactors = FALSE
    )
  )
}


# =============================================================================
# WRITE OUTPUT MANIFEST
# =============================================================================

manifest_file <- file.path(
  output_folder,
  paste0(
    output_prefix,
    "_stream_network_manifest.csv"
  )
)


write.csv(
  output_summary,
  manifest_file,
  row.names = FALSE,
  na = ""
)


# =============================================================================
# FINAL SUMMARY
# =============================================================================

cat("\n")
cat("============================================================\n")
cat("COMPLETE\n")
cat("============================================================\n\n")


cat(
  "Project: ",
  project_id,
  "\n",
  sep = ""
)


cat(
  "Watershed: ",
  basin_select_field,
  " = ",
  basin_select_value,
  "\n",
  sep = ""
)


cat(
  "Maximum stream order: ",
  max_streamorder,
  "\n\n",
  sep = ""
)


cat(
  "Generated stream networks:\n\n"
)


show_table(
  output_summary %>%
    select(
      threshold,
      minimum_order,
      maximum_order,
      features
    )
)


cat(
  "\nOutput folder:\n",
  output_folder,
  "\n",
  sep = ""
)


cat(
  "\nManifest:\n",
  manifest_file,
  "\n",
  sep = ""
)


cat("\n============================================================\n")