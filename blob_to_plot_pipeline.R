library(dplyr)
library(sf)
library(ggplot2)

hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12)  


# Authenticate: ----
source("secret.R")
# sas_token <- 
#   readr::read_lines(
#     "secret.R"
#   ) %>% 
#   purrr::set_names(stringr::word(.)) %>% 
#   purrr::keep(~stringr::str_detect(.x, "^sas|^endpoint")) %>% 
#   bind_rows() %>% 
#   mutate_all(
#     ~stringr::str_extract(., '".+"$') %>% 
#       stringr::str_remove_all('"')
#   )


# Fetch: ----
blob_ctr <- 
  AzureStor::blob_container(
    sas_endpoint, 
    sas = sas_token
  ) 

existing_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )

blob_csvs <- stringr::str_subset(
  existing_blobs,
  glue::glue("{uuid_rx}\\.csv")
)

AzureStor::multidownload_blob(
  blob_ctr,
  blob_csvs,
  file.path("forage_blobs", blob_csvs)
)


# Extract plot locations: ----
parse_box_from_dict <- function(fn) {
  lns <- readr::read_lines(fn)
  lns <- lns[str_detect(lns, ",")]
  
  if (!length(lns)) { stop("Empty scanfile: ", fn) }
  
  sensor_type <- str_extract(head(lns), "ASC-210|PHENOM_ACS435") %>% na.omit()
  has_voltage <- any(str_detect(head(lns), "INT_VOLT|EXT_VOLT"))
  
  if (sensor_type == "PHENOM_ACS435" & has_voltage) {
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
    
  } else {
    stop("Unknown sensor type: ", fn)
  }
  
  data_lns <- 
    lns[str_detect(lns, "^[-+0-9]")] %>% 
    str_split(",")
  
  data_lns_idx <- purrr::map_dbl(
    data_lns,
    ~.x[addr_pos] %>% as.numeric()
  )
  
  data_dfs <- purrr::map2(
    data_lns, data_lns_idx,
    ~set_names(.x, hdr[[.y]]) %>% 
      bind_rows()
  )
  
  dfs_list <- split(data_dfs, data_lns_idx)
  
  full_join(
    bind_rows(dfs_list[[1]]), 
    bind_rows(dfs_list[[2]]),
    by = c(
      "LAT", "LNG", "COURSE", "SPEED", "ELEV", "HDOP", "FIX",
      "UTC_DATE", "UTC_TIME"
    ),
    suffix = c("__01", "__02")
  ) %>% 
    mutate(fn = fn) %>% 
    mutate_all(readr::parse_guess)
}

scans <- dir(
  "forage_blobs",
  full.names = T,
  pattern = "csv"
) %>% 
  purrr::map(
    ~purrr::safely(parse_box_from_dict)(.x),
  )

purrr::map(scans, "error") %>% purrr::compact()

# _Onfarm: ----
extract_loop <- function(trk) {
  head_tail_idx <- seq.int(nrow(trk))
  head_tail_idx <- between(
    head_tail_idx, 
    quantile(head_tail_idx, 0.01),
    quantile(head_tail_idx, 0.99)
  )
  
  trk_sf <- 
    st_as_sf(
      trk[head_tail_idx, ], 
      coords = c("LNG","LAT"), 
      crs = 4326
      ) %>% 
    st_combine() %>% 
    st_cast("LINESTRING")
  
  ret <- st_intersection(
    trk_sf, trk_sf
  ) %>% 
    st_polygonize() %>% 
    st_cast() %>% 
    st_as_sf(crs = 4326) %>% 
    rename(geometry = x) %>% 
    mutate(
      sz = st_area(geometry)
    )
  
  
  list(
    scan = trk,
    track = st_as_sf(
      trk, 
      coords = c("LNG","LAT"), 
      crs = 4326,
      remove = F
    ),
    loops = ret
  )
}

label_looped_scan <- function(lps, buffer_size) {
  ctr <- st_centroid(
    lps[["loops"]] %>% 
      arrange(desc(sz)) %>%
      slice(1)
    )
  
  bf <- st_buffer(ctr, buffer_size)
  
  lps[["track"]] %>%
    mutate(
      flag = st_intersects(geometry, bf, sparse = F),
      flag = as.numeric(flag[,1])
      )
}


onfarm_loop_attempts <- 
  purrr::map(scans, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_onfarm_")) %>% 
  purrr::map(purrr::safely(extract_loop))

onfarm_loops <- 
  onfarm_loop_attempts %>% 
  purrr::map("result") %>% 
  purrr::compact() %>%
  purrr::map(~label_looped_scan(.x, 5)) 
    # 5 meter radius from center of largest loop

onfarm_loop_attempts %>% 
  purrr::map("error") %>% 
  purrr::compact()

plot_onfarm <- function(elt) {
  bb <- elt %>% 
    filter(flag == 1) %>% 
    st_bbox() %>% 
    st_as_sfc()
  
  centr_elt <- st_centroid(st_combine(elt))
  centr_bbox <- st_centroid(st_as_sfc(st_bbox(elt)))
  
  vec <- 
    (centr_elt - centr_bbox) %>% 
    st_coordinates()
  
  loc <- 
    paste0(
      ifelse(vec[2] <= 0, "t", "b"),
      ifelse(vec[1] <= 0, "r", "l"),
      collapse = ""
    )

  ggplot(elt, aes(color = as.factor(flag))) + 
    geom_sf(show.legend = F) +
    labs(
      title = paste0("Number of points: ", nrow(filter(elt, flag == 1))),
      subtitle = paste0("Area of bbox: ", format(st_area(bb)))
      ) +
    ggspatial::annotation_scale(
      location = loc,
      width_hint = 0.5
      )
}

purrr::map(
  onfarm_loops,
  plot_onfarm
)

# _CE1: ----
runs_to_plots <- function(flag, n) {
  flag[is.na(flag)] <- F
  x <- rle(flag)
  
  r <- rank(- x$lengths * x$values)
  x$values[which(r > n)] <- 0
  
  x$values <- cumsum(x$values)*x$values
  inverse.rle(x)
}

ce1_plots <- 
  purrr::map(scans, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_ce1_")) %>%
  purrr::map(
    ~.x %>% 
      mutate(
        flag = RcppRoll::roll_max(LIDAR, 10, fill = NA),
        flag = runs_to_plots(flag < 998, 4) # 4 plots in CE1 scans
      )
  )


# Export: ----

# drop geometry and resave as CSV?
# save as shapefiles or geoJSON?




# ce1_plots[6] |> 
#   purrr::map(
#     ~.x %>% 
#       mutate(
#         ts = paste(UTC_DATE, UTC_TIME, sep = "-"),
#         ts = lubridate::dmy_hms(ts)
#       ) %>% 
#       ggplot(aes(LNG, LAT, color = as.factor(flag))) + 
#       geom_point(size = 4, shape = 1, alpha = .7) +
#       scale_color_viridis_d(direction = 1, option = "B") +
#       labs(
#         title = basename(.x$fn[1]) |> 
#           str_remove(glue::glue("_{uuid_rx}.csv")),
#         subtitle = str_extract(.x$fn[1], uuid_rx)
#       )
#   )
# 
# ggsave(
#   filename = "weird_vermont.pdf",
#   width = 8, height = 8,
#   device = "pdf"
# )
