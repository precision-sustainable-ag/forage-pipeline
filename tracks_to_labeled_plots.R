library(dplyr)
library(sf)
library(ggplot2)
library(patchwork)
library(purrr)
library(stringr)

# unlink("plots_with_labels", recursive = T)
# dir.create("plots_with_labels")
# unlink("blobs_without_plot_labels", recursive = T)
# dir.create("blobs_without_plot_labels")
dir.create("diagnostic_plots")
dir.create("tracks_with_drift")

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

filter_uuids <- function(x, targets) {
  x_uuid <- stringr::str_extract(x, uuid_rx)
  x[!(x_uuid %in% targets)]
}

replace_ext <- function(fn, ext) {
  paste0(tools::file_path_sans_ext(fn), ".", ext)
}

mark_drift <- function(fn) {
  dest <- file.path("tracks_with_drift", fn)
  
  if (file.exists(dest)) {
    return(NULL)
  }
  
  file.copy(
    file.path("tracks_without_labels", fn),
    file.path("tracks_with_drift", fn)
  )
  
  return(fn)
  # check if it's already in the drift folder
  # move it into the drift folder
  # return something to check?
  # 
  # problem: the parsed blobs before labeling need to be stored
  # otherwise this doesn't work right
}

track_files <- 
  dir(
    "tracks_without_labels",
    full.names = T,
    pattern = "geojson"
    ) %>% 
  purrr::map(purrr::safely(sf::read_sf))

# todo: replace every "scans" with "track_files"
# todo: don't make SF obj within this script

# _Onfarm: ----
plot_onfarm_error <- function(elt, jmps) {
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
    ggplot(elt, aes(color = as.factor(flag))) + 
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
    geom_sf(
      data = jmps, 
      shape = 0, size = 4, 
      show.legend = F, inherit.aes = F
    ) +
    labs(
      title = paste0("Number of points: ", nrow(filter(elt, flag == 1)))
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
        obs_num, SONAR, 
        color = as.factor(flag)
      )
    ) +
    geom_point(show.legend = F) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5))
    )
  
  path <- basename(elt$fn[1]) %>% 
    str_remove(".csv$") %>% 
    paste0("/PLOT_ERROR_", ., ".pdf")
  
  ggsave(
    file.path(getwd(), "diagnostic_plots", path), 
    labeled + in_order,
    width = 10, height = 8
  )
  
  path
}


extract_loop <- function(trk) {
  head_tail_idx <- seq.int(nrow(trk))
  head_tail_idx <- between(
    head_tail_idx, 
    quantile(head_tail_idx, 0.01),
    quantile(head_tail_idx, 0.99)
  )
  
  trk_sf <- 
    st_combine(trk) %>% 
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
    track = trk,
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
  
  labeled_loops <- 
    lps[["track"]] %>%
    mutate(
      flag = st_intersects(geometry, bf, sparse = F),
      flag = as.numeric(flag[,1])
    )
  
  jumps <- lps[["track"]] %>% 
    pull(geometry) %>% 
    st_distance(lead(.), by_element = T)
  
  jump_flag <- jumps[jumps > units::set_units(2.5, "m")]
  
  if (length(na.omit(jump_flag))) {
    pathname <- plot_onfarm_error(
      labeled_loops, 
      lps[["track"]][jumps > units::set_units(2.5, "m") & !is.na(jumps), ]
    )
    browser()
    # move the file
    mark_drift(
      basename(labeled_loops$fn[1]) %>% 
        replace_ext("geojson")
      )
    stop(
      "GPS drifting?\n",
      "See file:\n",
      pathname, "\n"
    )
  }
  
  if (nrow(filter(labeled_loops, flag == 1)) <= 5) {
    pathname <- plot_onfarm_error(
      labeled_loops, 
      lps[["track"]][jumps > units::set_units(2.5, "m") & !is.na(jumps), ]
    )
    stop(
      "Malformed loop!\n",
      "See file:\n",
      pathname, "\n"
    )
  }
  
  labeled_loops
}


onfarm_loop_attempts <- 
  purrr::map(track_files, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_onfarm_")) %>%  
  purrr::map(purrr::safely(extract_loop))

onfarm_loops <- 
  onfarm_loop_attempts %>% 
  purrr::map("result") %>% 
  purrr::compact() %>%
  purrr::map(
    ~purrr::safely(label_looped_scan)(.x, 5)
  ) 
# 5 meter radius from center of largest loop

onfarm_loop_attempts %>% 
  purrr::map("error") %>% 
  purrr::compact()

onfarm_loops %>% 
  purrr::map("error") 

plot_onfarm <- function(elt, save = F) {
  if (!save) {
    readline("Hit Enter to continue:")
    
    message(
      cli::col_green("File: "),
      cli::col_blue(basename(elt$fn[1]))
    )
  }
  
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
  
  labeled <- 
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
  
  in_order <- 
    elt %>% 
    mutate(obs_num = row_number()) %>% 
    ggplot(
      aes(
        obs_num, SONAR, 
        color = as.factor(flag)
      )
    ) +
    geom_point(show.legend = F) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5))
    )
  
  if (save) {
    path <- basename(elt$fn[1]) %>% 
      stringr::str_remove(".csv$") %>% 
      paste0(".pdf")
    
    ggsave(
      file.path(getwd(), "preview_maps", path), 
      labeled + in_order,
      width = 10, height = 8
    )  
  } else {
    print(labeled)
  }
}

purrr::walk(
  onfarm_loops %>% 
    purrr::map("result") %>% 
    purrr::compact(),
  plot_onfarm
)

purrr::walk(
  onfarm_loops %>% 
    purrr::map("result") %>% 
    purrr::compact(),
  ~plot_onfarm(.x, save = T)
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
  
  in_order <- 
    elt %>% 
    mutate(obs_num = row_number()) %>% 
    ggplot(
      aes(
        obs_num, SONAR, 
        color = as.factor(paste(flag, plot_id, sep = "_"))
      )
    ) +
    geom_point(show.legend = F) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5))
    )
  
  print(
    labeled + in_order
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
  
  jumps <- elt %>%
    pull(geometry) %>%
    st_distance(lead(.), by_element = T)
  
  jump_set <- elt[jumps > units::set_units(2.5, "m"), ]
  
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
    geom_sf(data = jump_set, shape = 0, size = 4, show.legend = F) +
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
        obs_num, SONAR, 
        color = as.factor(paste(flag, plot_id, sep = "_"))
      )
    ) +
    geom_point(show.legend = F) +
    scale_color_manual(
      values = c("black", scales::hue_pal()(5))
    )
  
  path <- basename(elt$fn[1]) %>% 
    str_remove(".csv$") %>% 
    paste0("/PLOT_ERROR_", ., ".pdf")
  
  ggsave(
    file.path(getwd(), "diagnostic_plots", path), 
    labeled + in_order,
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
  # Decided SONAR is more reliably 999 than LIDAR, 
  #   so we don't have to worry about 0s
  ret <- dat %>% 
    mutate(
      flag = RcppRoll::roll_max(SONAR, 10, fill = NA),
      flag = runs_to_plots(flag < 998, n_plots) 
    )
  
  plot_check <- identical(
    sort(unique(ret$flag)), 
    walk_order$flag
  )
  
  labeled_scan <- 
    left_join(
      ret, walk_order
    ) 
  
  jumps <- labeled_scan %>%
    pull(geometry) %>%
    st_distance(lead(.), by_element = T)
  
  jump_flag <- na.omit(jumps[jumps > units::set_units(2.5, "m")])
  
  if (length(jump_flag)) {
    pathname <- 
      plot_ce_error(labeled_scan, walk_order$plot_id)
    # browser()
    # move the file
    mark_drift(
      basename(dat$fn[1]) %>% 
        replace_ext("geojson")
      )
    stop(
      "GPS drifting?\n",
      "See file:\n",
      pathname, "\n"
    )
  }
  
  
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
  purrr::map(track_files, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_ce1_")) %>% 
  purrr::map(purrr::safely(extract_ce_plots), n_plots = 4)

ce2_plots <- 
  purrr::map(track_files, "result") %>% 
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
  purrr::map("result") %>% 
  purrr::compact() %>% 
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
        replace_ext("geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )

ce2_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        replace_ext("geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )

## Export ----

drift_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["00a-tracks-with-drift"]] 

drifting_blobs_pushed <- 
  dir(
    "tracks_with_drift",
    full.names = T,
    pattern = "geojson"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, drift_ctr),
  )

drifting_blobs_pushed %>% 
  purrr::map("error") %>% 
  purrr::compact()


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

# TODO!! don't upload any files in steps if something from
#   that UUID is failing!!!