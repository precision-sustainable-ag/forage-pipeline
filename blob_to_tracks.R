library(dplyr)
library(sf)
library(stringr)


unlink("tracks_without_labels", recursive = T)
dir.create("tracks_without_labels")


hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12)  

filter_uuids <- function(x, targets) {
  x_uuid <- stringr::str_extract(x, uuid_rx)
  x[!(x_uuid %in% targets)]
}


replace_ext <- function(fn, ext) {
  paste0(tools::file_path_sans_ext(fn), ".", ext)
}

query_errors <- function(l) {
  if (is.null(names(l))) {
    names(l) <- seq_along(l)
  }
  return(purrr::compact(l))
}

# Authenticate: ----
source("secret.R")



# Fetch: ----
blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["landing-zone"]] 

existing_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )

tracked_blobs <- 
  AzureStor::list_blobs(
    AzureStor::list_blob_containers(
      sas_endpoint, 
      sas = sas_token
    )[["00-tracks-without-labels"]], 
    info = "name"
  ) %>% 
  stringr::str_extract(uuid_rx) %>% 
  unique()


blob_csvs <- 
  stringr::str_subset(
    existing_blobs,
    glue::glue("{uuid_rx}\\.(csv|txt)")
  ) %>% 
  stringr::str_subset(
    "_ce1_|_ce2_|_onfarm_|_strip_|_WCC_|_FFAR_"
  ) %>% 
  stringr::str_subset(
    "^box211", negate = T
  ) %>% 
  stringr::str_subset("_S_[0-9]{8}_") %>% 
  filter_uuids(tracked_blobs)


AzureStor::multidownload_blob(
  blob_ctr,
  blob_csvs,
  file.path("blobs_without_plot_labels", blob_csvs),
  overwrite = T
)

# Extract plot locations: ----
parse_box_from_dict <- function(fn) {
  lns <- readr::read_lines(fn, progress = F)
  lns <- lns[str_detect(lns, ",")]
  
  if (!length(lns)) { stop("Empty scanfile: ", fn) }
  
  if (!str_detect(lns[1], "LAT")) { stop("Missing GPS: ", fn) }

  sensor_type <- str_extract(head(lns), "ASC-210|PHENOM_ACS435|ASC-430") %>% na.omit()
  has_voltage <- any(str_detect(head(lns), "INT_VOLT|EXT_VOLT"))

  if (sensor_type %in% c("PHENOM_ACS435", "ASC-430") & has_voltage) {
    hdr <-
      list(
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX", 
          "UTC_DATE", "UTC_TIME", "INT_VOLT", "EXT_VOLT", "SENSOR_ADDR", 
          "NDVI", "3DNDVI", "NIR", "R"), # "SENSOR_TYPE:PHENOM_ACS435"
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX", 
          "UTC_DATE",  "UTC_TIME", "INT_VOLT", "EXT_VOLT", "SENSOR_ADDR", 
          "SONAR", "LIDAR", "ISP1", "ISP2") # "SENSOR_TYPE:PHENOM_DAS44X"
      )
    
    addr_pos <- 12
  } else if (sensor_type == "ASC-210" & has_voltage) {
    hdr <- 
      list(
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "INT_VOLT", "EXT_VOLT", "SENSOR_ADDR", 
          "NDVI", "3DNDVI", "NIR", "R"), # SENSOR_TYPE:ASC-210
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "INT_VOLT", "EXT_VOLT", "SENSOR_ADDR",
          "SONAR", "LIDAR", "ISP1", "ISP2") # SENSOR_TYPE:ASC-210
      )
    
    addr_pos <- 12
  } else if (sensor_type == "ASC-210" & !has_voltage) {
    hdr <- 
      list(
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "SENSOR_ADDR", 
          "NDVI", "3DNDVI", "NIR", "R"), # SENSOR_TYPE:ASC-210
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "SENSOR_ADDR",
          "SONAR", "LIDAR", "ISP1", "ISP2") # SENSOR_TYPE:ASC-210
      )
    
    addr_pos <- 10
    
  } else if (sensor_type %in% c("PHENOM_ACS435", "ASC-430") & !has_voltage) {
    hdr <- 
      list(
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "SENSOR_ADDR", 
          "NDVI", "3DNDVI", "NIR", "R"), # SENSOR_TYPE:PHENOM_ACS435
        c("LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
          "UTC_DATE", "UTC_TIME", "SENSOR_ADDR",
          "SONAR", "LIDAR", "ISP1", "ISP2") # SENSOR_TYPE:PHENOM_DAS44X
      )
    
    addr_pos <- 10
    
  } else {
    stop("Unknown sensor type: ", fn)
  }

  if (str_detect(fn, "box214v1_")) {
    hdr[[2]] <- str_subset(hdr[[2]], "^ISP[12]$", negate = T)
  }
  
  lns <- lns[str_detect(lns, "^[-+0-9]")]

  trailing_flag <- 
    all(str_detect(lns, ",$")) & 
    length(hdr[[1]]) == str_count(tail(lns, 1), ",")

  if (trailing_flag) {
    lns <- str_remove(lns, ",$")
  }
    
  data_lns <- 
    lns %>% 
    str_split(",")
  
  data_lns_idx <- purrr::map_dbl(
    data_lns,
    ~.x[addr_pos] %>% as.numeric()
  )
  
  data_dfs <- purrr::map2(
    data_lns, data_lns_idx,
    ~purrr::set_names(.x, hdr[[.y]]) %>% 
      bind_rows()
  )
  
  dfs_list <- split(data_dfs, data_lns_idx)
  
  ret <- full_join(
    bind_rows(dfs_list[[1]]), 
    bind_rows(dfs_list[[2]]),
    by = c(
      "LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
      "UTC_DATE", "UTC_TIME"
    ),
    suffix = c("__01", "__02")
  ) %>% 
    mutate(fn = fn) %>% 
    mutate_all(readr::parse_guess) %>% 
    select(-SENSOR_ADDR__01, -SENSOR_ADDR__02)
  
  
  ret
}

scans <- file.path(
  "blobs_without_plot_labels",
  blob_csvs[1:10]
) %>% 
  purrr::map(
    ~purrr::safely(parse_box_from_dict)(.x),
    .progress = T
  )

purrr::map(scans, "error") %>% query_errors()

# TODO: manually edit, has mixed trailing commas and not
#   "box214v2_WCC_BARC_232A_Triticale_S_20230322_3213691f-0883-4e40-a0cc-4d7e831d75a2.csv"

tracks_to_save <- 
  purrr::map(scans, "result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~purrr::safely(st_as_sf)(
        .x, 
        coords = c("LNG","LAT"), 
        crs = 4326,
        remove = F
      )
  )

tracks_to_save %>% 
  purrr::map("error") %>% 
  query_errors()

tracks_to_save %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~sf::write_sf(
      .x,
      file.path(
        "tracks_without_labels",
        basename(.x$fn[1]) %>% 
          replace_ext("geojson")
      )
    )
  )


# Upload to blob storage ----
trk_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["00-tracks-without-labels"]] 

labeled_tracks_pushed <- 
  dir(
    "tracks_without_labels",
    full.names = T,
    pattern = "geojson"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, trk_ctr),
  )

labeled_tracks_pushed %>% 
  purrr::map("error") %>% 
  query_errors()
