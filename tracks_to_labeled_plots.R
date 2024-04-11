library(dplyr)
library(sf)
library(ggplot2)
library(patchwork)
library(purrr)
library(stringr)

source("secret.R")

unlink("plots_with_labels", recursive = T)
dir.create("plots_with_labels")
unlink("tracks_without_labels", recursive = T)
dir.create("tracks_without_labels")
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

hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12)  


filter_uuids <- function(x, targets, exclude = T) {
  x_uuid <- stringr::str_extract(x, uuid_rx)
  if (exclude) {
    ret <- x[!(x_uuid %in% targets)]
  } else {
    ret <- x[(x_uuid %in% targets)]
  }
  ret
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
}



# Fetch: ----
blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["00-tracks-without-labels"]] 

existing_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )

plotted_blobs <- 
  AzureStor::list_blobs(
    AzureStor::list_blob_containers(
      sas_endpoint, 
      sas = sas_token
    )[["01-plots-with-labels"]], 
    info = "name"
  ) %>% 
  stringr::str_extract(uuid_rx) %>% 
  unique()


blob_geojsons <- 
  stringr::str_subset(
    existing_blobs,
    glue::glue("{uuid_rx}\\.geojson")
  ) %>% 
  stringr::str_subset(
    "_ce1_|_ce2_|_onfarm_|_strip_|_WCC_"
  ) %>% 
  stringr::str_subset("_S_[0-9]{8}_") %>% 
  filter_uuids(plotted_blobs)


AzureStor::multidownload_blob(
  blob_ctr,
  blob_geojsons,
  file.path("tracks_without_labels", blob_geojsons),
  overwrite = T
)


track_files <- 
  dir(
    "tracks_without_labels",
    full.names = T,
    pattern = "geojson"
  ) %>% 
  purrr::map(purrr::safely(sf::read_sf))


# TODO: if the file is _SE_ then override the jumps flag

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
      slice(1) %>% 
      st_geometry()
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
  purrr::map("error") %>% 
  purrr::compact()

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


# Strip: ----


strip_tracks <- 
  purrr::map(track_files, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_strip_"))


strip_uuids <- 
  purrr::map_chr(
    strip_tracks,
    ~str_extract(.x$fn[1], uuid_rx)
  )

blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["landing-zone"]] 

existing_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )

strip_poly_names <- 
  existing_blobs %>% 
  filter_uuids(strip_uuids, exclude = F) %>% 
  str_subset("_F_[0-9]{8}_")

strip_point_names <- 
  existing_blobs %>% 
  filter_uuids(strip_uuids, exclude = F) %>% 
  str_subset("_O_[0-9]{8}_")

AzureStor::multidownload_blob(
  blob_ctr,
  c(strip_poly_names, strip_point_names),
  file.path(
    "tracks_without_labels", 
    "strip_locations",
    c(strip_poly_names, strip_point_names)
  ),
  overwrite = T
)

recode_species <- function(s) {
  recode(
    s, "RWP" = "CerealRye-WinterPea", "RV" = "CerealRye-HairyVetch",
    "RCC" = "CerealRye-CrimsonClover", "Rye" = "CerealRye",
    "Trit" = "Triticale", "Brass Rapa" = "BRapa", "Brass R" = "BRapa", 
    "Brass Nap" = "BNapus", "WP" = "WinterPea", "CC" = "CrimsonClover",
    "Vetch" = "HairyVetch", "Oat" = "Oats", "Brass N" = "BNapus",
    "BN" = "BNapus", "BR" = "BRapa", "Rap" = "BRapa"
  )
}


st_extend <- function(g, n) {
  g <- st_zm(g)
  cr <- st_crs(g)
  ctr = st_centroid(g)
  
  st_set_crs((g - ctr)*n + ctr, cr)
}


st_furthest <- function(d = "E") {
  function(g) {
    crd <- st_zm(g) %>% 
      st_coordinates() %>% 
      as.data.frame() %>% 
      group_by(across(c(-X, -Y))) %>% 
      mutate(
        flagE = X == max(X),
        flagW = X == min(X),
        flagN = Y == max(Y),
        flagS = Y == min(Y)
      ) %>% 
      filter(if_any(matches(paste0("flag", d)))) %>% 
      summarize(X = mean(X), Y = mean(Y), .groups = "drop")
    
    crd %>% 
      ungroup() %>% 
      select(X, Y) %>% 
      as.matrix() %>% 
      st_multipoint() %>% 
      st_sfc() %>% 
      st_cast("POINT")
  }
}


plot_strip <- function(obj, ply, pts, s, nm, err) {
  ply <- ply %>% filter(Species %in% obj$Species)
  pts <- pts %>% filter(Species %in% obj$Species)
  
  obj <- obj %>% 
    mutate(
      flag = replace(flag, Species == "mismatch", 2),
      flag = replace(flag, flag == -1, NA)
    )
  
  g <- 
    ggplot(obj) + 
    geom_sf(aes(color = (flag)), show.legend = F) + 
    scale_color_viridis_c(
      na.value = "grey70", option = "C", limits = c(0,2)
    ) + 
    geom_sf(data = pts, fill = NA) + 
    geom_sf_text(data = pts, aes(label = plotID), hjust = 1.5) +
    geom_sf(data = ply, fill = NA) +
    geom_sf_text(
      data = ply, 
      aes(label = Species), 
      fun.geometry = st_furthest("E"), 
      hjust = 0,
      nudge_x = 0.00025
    ) +
    geom_sf(
      data = st_extend(ply$geometry, 3), 
      fill = NA, color = NA
    ) +
    labs(
      subtitle = paste(sort(s), collapse = " ") %>% str_wrap(50),
      x = NULL, y = NULL
    )
  
  if (err) {
    nm <- file.path(getwd(), "diagnostic_plots", paste0("PLOT_ERROR_", nm))
  } else {
    nm <- file.path(getwd(), "preview_maps", nm)
  }
  
  nm <- nm %>% replace_ext("pdf")
  
  ggsave(nm, plot = g, width = 10, height = 8)
}

strip_NC_F_corrected_2022_2023 <- 
  read_sf("strip_NC_F_corrected_2022-2023.geojson") %>% 
  st_zm()

strip_extract_boundaries <- function(trk) {
  meta <- basename(trk$fn[1]) %>% 
    replace_ext("") %>% 
    str_split("_", simplify = T) %>% 
    set_names(
      c("boxtype", "proj", "location", "rep", "species",
        "scan", "scan_date", "uuid.")
    )
  
  species <- meta[["species"]] %>% 
    str_split("~", simplify = T)
  
  trk <- st_transform(trk, 4326)
  
  correct_poly_flag <- 
    meta[["location"]] == "NC" & 
    meta[["scan_date"]] >= "20221118" & 
    meta[["scan_date"]] <= "20230503" 
  
  if (correct_poly_flag) {
    poly <- strip_NC_F_corrected_2022_2023 %>% 
      select(-Species, -Rep) %>% 
      select(Species = Species_correct, Rep = Rep_correct, geometry)
  } else {
    poly <- dir(
      "tracks_without_labels/strip_locations",
      pattern = paste0("_F_[0-9]{8}_", meta[["uuid."]]),
      full.names = T
    ) %>% 
      read_sf() %>% 
      st_zm() %>% 
      select(Species, geometry) %>% 
      st_transform(4326)
  }
  
  
  
  pts_buffer <- dir(
    "tracks_without_labels/strip_locations",
    pattern = paste0("_O_[0-9]{8}_", meta[["uuid."]]),
    full.names = T
  ) %>%
    read_sf() %>%
    st_transform(4326) %>%
    st_zm() %>% 
    select(matches("Species"), matches("plot", ignore.case = T), geometry) %>%
    st_buffer(dist = units::set_units(5, "meters"))
  
  if (!("Species" %in% names(pts_buffer))) {
    pts_buffer <- 
      pts_buffer %>% 
      tidyr::separate(Plot, c("Species", "plotID"), sep = -2)
  }
  
  pts_buffer <- 
    pts_buffer %>% 
    mutate(
      Species = stringr::str_trim(Species),
      Species = recode_species(Species)
    )
  
  ret <- 
    st_join(trk, poly) %>% 
    mutate(flag_poly = if_else(is.na(Species), -1, 0)) %>% 
    rename(Species_poly = Species) %>% 
    st_join(pts_buffer %>% rename(Species_pt = Species)) %>% 
    mutate(
      flag_pt = if_else(is.na(Species_pt), 0, 1),
      flag = flag_poly + flag_pt,
      mismatch = Species_pt != Species_poly,
      Species_mismatch = if_else(mismatch, "mismatch", NA_character_),
      Species = coalesce(Species_mismatch, Species_pt, Species_poly)
    ) %>% 
    select(
      -flag_poly, -flag_pt, -mismatch,
      -Species_poly, -Species_pt, -Species_mismatch)
  
  purrr::quietly(plot_strip)(
    ret, poly, pts_buffer, species, 
    basename(trk$fn[1]), F
  )
  
  ret
}

strip_plots <- 
  strip_tracks %>%  
  set_names(map(., ~basename(.x$fn[1]))) %>% 
  map(safely(strip_extract_boundaries), .progress = "Strip: ") 




strip_plots %>% 
  purrr::map("result") %>% 
  purrr::compact()

strip_plots %>% 
  purrr::map("error") %>% 
  purrr::compact()



# WCC: ----
wcc_tracks <- 
  purrr::map(track_files, "result") %>% 
  purrr::compact() %>% 
  purrr::keep(~stringr::str_detect(.x$fn[1], "_WCC_"))

wcc_flag_ids <- 
  list(
    "2019" = "1rpXS7K8Z1iSGPwl3W21IDNUz4ut-5xLr599LCRbSXhQ",
    "2020" = "1MKNXikH0ftfggsfNZQ-zJKS8rbbvj9w3IuOc4eBozw0",
    "2021" = "1JQ9dx5VBbl0JhKvQGGj05T7k1U6pjxozVMeuezVcTYk",
    "2022" = "10rLeDjzDivrAmB9t1JYahrVzP7VaOTvpRXgiv1TWb5U",
    "2023" = "12YEG8YrvegO5emxtIKRLo9Iaz5RI4Xp6eXXBk7BM0_0",
    "2024" = "1ddRKAAr6GgGup9s3iV89jhpsUHjuZShiUpBUk6VL_hY"
  ) %>% 
  purrr::map(
    ~googlesheets4::read_sheet(
      .x, sheet = "BARC", col_types = "c",
      .name_repair = ~make.unique(.x)
    ) %>% 
      select(
        Field, Plot, matches("Species"), Latitude, Longitude, 
        FS_box_date, Field_sampling_date
      )
  )

wcc_flag_ids_combined <- 
  wcc_flag_ids %>% 
  bind_rows(.id = "harvest_year") %>% 
  mutate(
    date = coalesce(
      lubridate::mdy(FS_box_date, quiet = T), 
      lubridate::ymd(FS_box_date, quiet = T), 
      lubridate::mdy(Field_sampling_date, quiet = T),
      lubridate::ymd(Field_sampling_date, quiet = T)
    ),
    Latitude = as.numeric(Latitude),
    Longitude = as.numeric(Longitude)
  ) %>% 
  tidyr::unite(
    col = "Species", 
    matches("Species"), 
    sep = "-",
    na.rm = T
    ) %>% 
  filter(!is.na(Plot)) %>% 
  filter(!is.na(Latitude), !is.na(Longitude)) %>% 
  select(harvest_year, Field, Plot, Species, Latitude, Longitude, date) %>% 
  st_as_sf(
    coords = c("Longitude", "Latitude"), 
    crs = 4326
  )

# are there any misentries with the date?
wcc_flag_ids_combined %>% 
  filter(is.na(date))

get_harvest_year <- function(d) {
  if (lubridate::month(d) > 6) {
    lubridate::year(d) + 1
  } else { lubridate::year(d) }
}

plot_wcc <- function(lbl_trk, locs, nm) {
  b <- make_square(lbl_trk) %>% st_as_sfc()
  
  locs_ <- 
    locs %>% 
    filter(Plot %in% lbl_trk$Plot) %>% 
    st_buffer(10)
  
  p1 <- ggplot() +
    geom_sf(data = b, fill = "#00000000", color = "#00000000") +
    geom_sf(data = locs_) +
    geom_sf(
      data = lbl_trk %>% filter(is.na(Plot)), 
      aes(color = NDVI)
    ) +
    geom_sf(
      data = lbl_trk %>% filter(!is.na(Plot)), 
      aes(fill = as.factor(Plot)),
      shape = 21, color = "#00000000"
    ) +
    scale_color_viridis_c() +
    labs(fill = NULL)
  
  p2 <- ggplot(
    lbl_trk %>% mutate(obs = row_number()), 
    aes(obs, SONAR)
  ) +
    geom_point(
      aes(color = as.factor(Plot)), 
      show.legend = F,
      alpha = 0.75
    )
  
  path = if (stringr::str_detect(nm, "Error")) {
    file.path("diagnostic_plots", nm) %>% 
      replace_ext("pdf")
  } else {
    file.path("preview_maps", replace_ext(nm, "pdf"))
  }
  
  ggsave(
    path,
    plot = p1 + p2 + plot_annotation(title = nm), 
    width = 12, height = 5
  )
}

wcc_mark_flags <- function(trk) {
  meta <- basename(trk$fn[1]) %>% 
    replace_ext("") %>% 
    str_split("_", simplify = T) %>% 
    set_names(
      c("boxtype", "proj", "location", "field", 
        "species", "scan", "scan_date", "uuid.")
    )
  scan_date = lubridate::ymd(meta[["scan_date"]])
  hy = get_harvest_year(scan_date)
  middle = round(nrow(trk)/2)
  box_dm = str_sub(trk$UTC_DATE[middle], 1, 4)
  box_y = str_sub(trk$UTC_DATE[middle], 5, 6)
  box_date_fixed = 
    paste0(box_dm, "20", box_y) %>% 
    lubridate::dmy()
  
  if (scan_date != box_date_fixed) {
    stop("Filename date does not match box date: ", trk$fn[1])
  }
  
  flag_locs <- 
    wcc_flag_ids_combined %>% 
    filter(harvest_year == hy) %>% 
    filter(date >= scan_date - 7, date <= scan_date + 7) %>% 
    filter(date == date[which.min(abs(date - scan_date))])
  
  if (length(unique(flag_locs$date)) != 1) {
    stop(
      "No field sampling dates found within 1 week of scan date: ", 
      basename(trk$fn[1])
    )
  }
  
  ret <- st_join(
    trk,
    st_buffer(flag_locs, 10)
  ) %>% 
    select(-harvest_year)
  
  ret_inner <- st_filter(
    trk,
    st_buffer(flag_locs, 20)
  )
  
  meta_count <- 
    ret %>% 
    distinct(Species, Field) %>% 
    filter(!is.na(Species)) %>% 
    nrow()
  
  if (meta_count != 1) {
    stop(
      "Multiple/no fields matched within scan track: ", 
      basename(trk$fn[1])
    )
  }
  
  ret$Field <- unique(na.omit(ret$Field))
  ret$Species <- unique(na.omit(ret$Species))
  ret$date <- unique(na.omit(ret$date))
  
  sonar_stuck_num <- 
    ret_inner %>% 
    filter(SONAR != 999, NDVI != 999) %>% 
    mutate(rng = RcppRoll::roll_max(SONAR, n = 100, fill = NA) - RcppRoll::roll_min(SONAR, n = 100, fill = NA)) %>% 
    filter(!is.na(rng), rng < 2) %>% 
    nrow()

  sonar_non999_frac <-
    with(
      ret_inner,
      table(SONAR == 999)[["FALSE"]]/length(SONAR)
    )
  
  err_msg <- ""
  if(sonar_stuck_num > 0) { 
    err_msg = "SONAR Range Error "
  } else if (sonar_non999_frac < 0.7) {
    err_msg = "SONAR 999 Error "
  }
  
  plot_wcc(
    ret, flag_locs, 
    paste0(err_msg, basename(trk$fn[1]))
  )
  
  if (err_msg != "") {
    stop("SONAR either stuck or 999: ", basename(trk$fn[1]))
  }
  
  ret %>% 
    mutate(
      flag = coalesce(Plot, "0")
    ) %>% 
    rename(plotID = Plot)
}


wcc_labeled_tracks <- 
  purrr::map(
    wcc_tracks,
    purrr::safely(wcc_mark_flags),
    .progress = T
  )

wcc_labeled_tracks %>% 
  purrr::map("error") %>% 
  query_errors()


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


strip_plots %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        replace_ext("geojson")
      
      sf::st_write(.x, file.path("plots_with_labels", fn))
    }
  )


wcc_labeled_tracks %>% 
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
