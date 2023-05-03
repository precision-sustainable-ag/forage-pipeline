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

make_square <- function(obj) {
  b <- st_bbox(obj)
  m <- max(b["xmax"] - b["xmin"], b["ymax"] - b["ymin"])
  
  cx <- (b["xmax"] + b["xmin"])/2
  cy <- (b["ymax"] + b["ymin"])/2
  
  b["xmin"] <- cx - m/2
  b["xmax"] <- cx + m/2
  b["ymin"] <- cy - m/2
  b["ymax"] <- cy + m/2
  b
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

labeled_blobs <- 
  AzureStor::list_blobs(
    AzureStor::list_blob_containers(
      sas_endpoint, 
      sas = sas_token
    )[["01-plots-with-labels"]], 
    info = "name"
  ) %>% 
  stringr::str_extract(uuid_rx) %>% 
  unique()


blob_csvs <- 
  stringr::str_subset(
    existing_blobs,
    glue::glue("{uuid_rx}\\.csv")
  ) %>% 
  stringr::str_subset(
    "_ce1_|_ce2_|_onfarm_"
  ) %>% 
  str_subset(
    paste(labeled_blobs, collapse = "|"),
    negate = T
  )

AzureStor::multidownload_blob(
  blob_ctr,
  blob_csvs,
  file.path("blobs_without_plot_labels", blob_csvs),
  overwrite = T
)


# Extract plot locations: ----
parse_box_from_dict <- function(fn) {
  lns <- readr::read_lines(fn)
  lns <- lns[str_detect(lns, ",")]
  
  if (!length(lns)) { stop("Empty scanfile: ", fn) }
  
  if (!str_detect(lns[1], "LAT")) { stop("Missing GPS: ", fn) }

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
    
  } else if (sensor_type == "PHENOM_ACS435" & !has_voltage) {
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
    mutate_all(readr::parse_guess) %>% 
    select(-SENSOR_ADDR__01, -SENSOR_ADDR__02)
}

scans <- file.path(
  "blobs_without_plot_labels",
  blob_csvs
) %>% 
  purrr::map(
    ~purrr::safely(parse_box_from_dict)(.x)
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
  readline("Hit Enter to continue:")
  
  message(
      cli::col_green("File: "),
      cli::col_blue(basename(elt$fn[1]))
    )
  
  sq <- make_square(elt) %>% st_as_sfc()
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

  print(
    ggplot(elt, aes(color = as.factor(flag))) + 
      geom_sf(data = sq, color = NA, fill = NA) +
      geom_sf(show.legend = F) +
      labs(
        title = paste0("Number of points: ", nrow(filter(elt, flag == 1))),
        subtitle = paste0("Area of bbox: ", format(st_area(bb)))
      ) +
      ggspatial::annotation_scale(
        location = loc,
        width_hint = 0.5
      )
  )
}

purrr::walk(
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

plot_ce <- function(elt) {
  readline("Hit Enter to continue:")
  
  message(
    cli::col_green("File: "),
    cli::col_blue(basename(elt$fn[1]))
  )
  
  sq <- make_square(elt) %>% st_as_sfc()
  
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
  
  print(
    ggplot(elt, aes(color = as.factor(paste(flag, plot_id, sep = "_")))) + 
      geom_sf(data = sq, color = NA, fill = NA) +
      geom_sf_label(
        data = function(d) 
          rbind("Start" = head(d, 1), "End" = tail(d, 1)) %>% 
          mutate(label = c("Start", "End")),
        aes(label = label), color = "black",
        hjust = "inward", vjust = "inward",
        fun.geometry = sf::st_centroid
      ) +
      geom_sf() +
      labs(
        color = "Plot Order"
      ) +
      scale_color_manual(
        values = c("grey80", scales::hue_pal()(5)),
        guide = guide_legend(override.aes = list(size = 8))
      ) +
      ggspatial::annotation_scale(
        location = loc,
        width_hint = 0.5
      )
  )
}

plot_ce_error <- function(elt, walk_order) {
  
  sq <- make_square(elt) %>% st_as_sfc()
  
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
  
  labeled <- 
    ggplot(elt, aes(color = as.factor(paste(flag, plot_id, sep = "_")))) + 
    geom_sf(data = sq, color = NA, fill = NA) +
    geom_sf_label(
      data = function(d) 
        rbind("Start" = head(d, 1), "End" = tail(d, 1)) %>% 
        mutate(label = c("Start", "End")),
      aes(label = label), color = "black",
      hjust = "inward", vjust = "inward",
      fun.geometry = sf::st_centroid
    ) +
    geom_sf() +
    labs(
      color = "Plot Order",
      title = glue::glue("Should be:\n{paste(walk_order[-1], collapse = '\n')}")
    ) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5)),
      guide = guide_legend(override.aes = list(size = 8))
    ) +
    ggspatial::annotation_scale(
      location = loc,
      width_hint = 0.5
    )
  
  in_order <- 
    elt %>% 
    mutate(obs_num = row_number()) %>% 
    ggplot(
      aes(
        obs_num, LIDAR, 
        color = as.factor(paste(flag, plot_id, sep = "_"))
      )
    ) +
    geom_point(show.legend = F) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5))
    )
  
  path <- basename(elt$fn[1]) %>% 
    str_remove(".csv$") %>% 
    paste0(getwd(), "/PLOT_ERROR_", ., ".pdf")
  
  library(patchwork)
  ggsave(
    path, labeled + in_order,
    width = 10, height = 8
  )
  
  path
}

extract_ce_plots <- function(dat, n_plots) {
  meta <- basename(dat$fn[1]) %>% 
    str_remove("\\.csv$") %>% 
    str_split("_", simplify = T) %>% 
    set_names(
      c("boxtype", "proj", "location", "timing", "species",
        "walk_order", "scan", "scan_date", "uuid")
    )

  if (str_detect(meta[["walk_order"]], "walkB|walkD")) {
    reverse_flag = T
  } else { reverse_flag = F }
  
  walk_order <- 
    tibble(
      plot_id = c(
        "alley",
        meta[["walk_order"]] %>% 
        str_split("~", simplify = T) %>% 
        as.vector()
      )
    ) %>% 
    slice(-2) %>%             # remove "rep" or "walk"
    mutate(
      flag = row_number() - 1 # index "alley" at 0,
    ) 
  
  if (meta[["proj"]] == "ce2") {
    walk_order <- 
      walk_order %>% 
      mutate(
        rep = replace(
          flag,
          reverse_flag, 
          c(0, rev(row_number())[-1]) 
          # generates 0,1:5, then 0,5:1, then 0,4:1
          )
      )
  } # TODO: fixed, but ugly. Will give 1-5 for A, C and 5-1 for B, D
  
  # TODO: is 0 the same as 999 in the data?
  ret <- dat %>% 
    mutate(
      flag = RcppRoll::roll_max(LIDAR, 10, fill = NA),
      flag = runs_to_plots(flag < 998, n_plots) 
    )

  plot_check <- identical(
    sort(unique(ret$flag)), 
    walk_order$flag
    )
  
  labeled_scan <- 
    left_join(
    ret, walk_order
  ) %>% 
    st_as_sf(
      coords = c("LNG","LAT"), 
      crs = 4326,
      remove = F
    )
  

  
  if (!plot_check) {
    pathname <- 
      plot_ce_error(labeled_scan, walk_order$plot_id)
    stop(
      "Missing one or more plots in scan that were entered on the form.\n",
      "See file:\n",
      pathname, "\n",
      jsonlite::toJSON(
        anti_join(walk_order, ret, by = "flag") %>%
          mutate(file = basename(ret$fn[1]))
        )
      )
  }
  
  labeled_scan
  
}

ce1_plots <- 
  purrr::map(scans, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_ce1_")) %>% 
  purrr::map(purrr::safely(extract_ce_plots), n_plots = 4)

ce2_plots <- 
  purrr::map(scans, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_ce2_")) %>% 
  purrr::map(purrr::safely(extract_ce_plots), n_plots = 5)





purrr::walk(
  purrr::map(ce1_plots, "result") %>% 
    purrr::compact(),
  plot_ce
)
purrr::walk(
  purrr::map(ce2_plots, "result") %>% 
    purrr::compact(),
  plot_ce
)

purrr::map(ce1_plots, "error") %>% 
  purrr::compact()

purrr::map(ce2_plots, "error") %>% 
  purrr::compact()

ce2_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  # purrr::keep(
  #   ~str_detect(
  #     .x$fn[1], 
  #     "box214v2_ce2_NCCE2_brown_rye_walkA~planting-brown~bare~planting-green~planting-greenbrown~bare_S_20230331_549bb4ec-9114-47e6-889a-2b9d66ad8fb2.csv"
  #     )
  #   ) %>% 
  .[[1]] %>% 
  mutate(r = row_number()) %>% 
  ggplot(aes(r, LIDAR, color = plot_id)) +
  geom_point()


ce2_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  .[[5]] %>%
  ggplot(aes(LNG, LAT, shape = as.factor(rep))) +
    geom_point() +
  geom_point(data = function(d) slice(d, 1), size = 10)


# Export: ----


onfarm_loops %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        str_replace(".csv$", ".geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )

ce1_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        str_replace(".csv$", ".geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )

ce2_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        str_replace(".csv$", ".geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )

lbl_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["01-plots-with-labels"]] 

labeled_blobs_pushed <- 
  dir(
    "plots_with_labels",
    full.names = T,
    pattern = "geojson"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, lbl_ctr),
  )

labeled_blobs_pushed %>% 
  purrr::map("error") %>% 
  purrr::compact()

