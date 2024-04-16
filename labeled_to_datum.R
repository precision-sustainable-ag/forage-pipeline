library(dplyr)

source("secret.R")

hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12)  

most_common <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

filter_uuids <- function(x, targets) {
  x_uuid <- stringr::str_extract(x, uuid_rx)
  x[!(x_uuid %in% targets)]
}

replace_ext <- function(fn, ext) {
  paste0(tools::file_path_sans_ext(fn), ".", ext)
}

unlink("plots_with_datum", recursive = T)
dir.create("plots_with_datum")
unlink("blobs_without_datum", recursive = T)
dir.create("blobs_without_datum")

# Fetch: ----
blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["01-plots-with-labels"]] 

existing_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )




datumed_blobs <- 
  AzureStor::list_blobs(
    AzureStor::list_blob_containers(
      sas_endpoint, 
      sas = sas_token
    )[["02-plots-with-datum"]], 
    info = "name"
  ) %>% 
  stringr::str_extract(uuid_rx) %>% 
  unique()


blob_geojsons <- 
  stringr::str_subset(
    existing_blobs,
    glue::glue("{uuid_rx}\\.geojson")
  ) %>% 
  filter_uuids(datumed_blobs)



AzureStor::multidownload_blob(
  blob_ctr,
  blob_geojsons,
  file.path("blobs_without_datum", blob_geojsons),
  overwrite = T
)







scans <- dir(
  "blobs_without_datum",
  full.names = T,
  pattern = "geojson"
) %>% 
  purrr::map(
    ~sf::st_read(.x)
  )

# labeled blob to datum ----


track_rm_outliers <- function(trk) {

  trk_clean <- 
    trk %>%  
    filter(!(flag == 0 & str_detect(fn, "_ce1_|_ce2_"))) %>% 
    # TODO we need to filter the tip-up slope from CE1/2 before removing
    # outliers, but the logic isn't yet clear
    filter(!is.na(SONAR), !is.na(LIDAR)) %>%  
    filter(SONAR < 999, LIDAR < 999) %>% 
    filter(SONAR <= 250, LIDAR <= 250)
  
  lidar_dist = MASS::fitdistr(trk_clean$LIDAR, 'normal')
  lidar_mean = as.numeric(lidar_dist$estimate[1])
  lidar_sd = as.numeric(lidar_dist$estimate[2])
  
  lidar_3sd_low = lidar_mean - (lidar_sd * 3)
  lidar_3sd_high = lidar_mean + (lidar_sd * 3)
  
  sonar_dist = MASS::fitdistr(trk_clean$SONAR, 'normal')
  sonar_mean = as.numeric(sonar_dist$estimate[1])
  sonar_sd = as.numeric(sonar_dist$estimate[2])
  
  sonar_3sd_low = sonar_mean - (sonar_sd * 3)
  sonar_3sd_high = sonar_mean + (sonar_sd * 3)
  
  trk_outliers_removed <-
    trk %>%  
    filter(LIDAR < lidar_3sd_high & LIDAR > lidar_3sd_low) %>%
    filter(SONAR < sonar_3sd_high & SONAR > sonar_3sd_low)
  
  if (!nrow(trk_outliers_removed)) {
    stop("All rows removed in outlier step for: ", trk$fn[1])
  }
  
  trk_outliers_removed
}



track_add_datum <- function(trk) {
  
  trk_lidar_top_decile <-
    trk %>%
    filter(!(flag == 0 & str_detect(fn, "_ce1_|_ce2_"))) %>% 
    filter(cume_dist(LIDAR) > 0.89) %>%
    pull(LIDAR) |> 
    most_common()
  
  trk_sonar_top_decile <-
    trk %>%
    filter(!(flag == 0 & str_detect(fn, "_ce1_|_ce2_"))) %>% 
    filter(cume_dist(SONAR) > 0.89) %>%
    pull(SONAR) |> 
    most_common()
  
  trk_lidar_top_decile_brown <-
    trk %>%
    filter(!(flag == 0 & str_detect(fn, "_ce1_|_ce2_"))) %>% 
    filter(cume_dist(LIDAR) > 0.89) %>%
    filter(NDVI <= 0.3) %>% 
    pull(LIDAR) |> 
    most_common()
  
  trk_sonar_top_decile_brown <-
    trk %>%
    filter(!(flag == 0 & str_detect(fn, "_ce1_|_ce2_"))) %>% 
    filter(cume_dist(SONAR) > 0.89) %>%
    filter(NDVI <= 0.3) %>% 
    pull(SONAR) |> 
    most_common()

  # TODO why are we using different numbers for the datum for each sensor?
  #   They're the same height above the ground, so if LIDAR finds the 
  #   ground more often, use that number to subtract from both sensors?

  trk %>%
    mutate(lidar_dat = trk_lidar_top_decile) %>%
    mutate(sonar_dat = trk_sonar_top_decile) %>%
    mutate(lidar_ht = lidar_dat - LIDAR) %>%
    mutate(sonar_ht = sonar_dat - SONAR) %>% 
    mutate(lidar_dat_brown = trk_lidar_top_decile_brown) %>%
    mutate(sonar_dat_brown = trk_sonar_top_decile_brown) %>%
    mutate(lidar_ht_brown = lidar_dat_brown - LIDAR) %>%
    mutate(sonar_ht_brown = sonar_dat_brown - SONAR) %>% 
    mutate(l2s_ratio = LIDAR/SONAR)
}


labeled_plots_without_outliers <- 
  scans %>% 
  purrr::map(
    ~purrr::safely(track_rm_outliers)(.x)
    )

labeled_plots_without_outliers %>% 
  purrr::map("error") %>% 
  purrr::compact()

labeled_plots_without_outliers %>% 
  purrr::map("error") %>% 
  purrr::imap(~{if (is.null(.x)) return(NULL) else return(.y)}) %>% 
  purrr::compact()




# TODO check error, is it grouping correctly? ----
labeled_plots_with_datum_to_push <- 
  labeled_plots_without_outliers %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(track_add_datum)


labeled_plots_with_datum_to_push %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1]) %>% 
        replace_ext("geojson")
      if (is.na(fn)) { browser() }
      
      sf::st_write(
        .x, file.path("plots_with_datum", fn),
        delete_dsn = T
        )
    }
  )

lbl_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["02-plots-with-datum"]] 

# TODO: do/don't overwrite existing blobs?

labeled_plots_with_datum_pushed <- 
  dir(
    "plots_with_datum",
    full.names = T,
    pattern = "geojson"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, lbl_ctr),
  )

labeled_plots_with_datum_pushed %>% 
  purrr::map("error") %>% 
  purrr::compact()


# TODO!! don't upload any files in steps if something from
#   that UUID is failing!!!