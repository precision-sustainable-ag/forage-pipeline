library(dplyr)
library(moments)

source("secret.R")

replace_ext <- function(fn, ext) {
  paste0(tools::file_path_sans_ext(fn), ".", ext)
}

unlink("blobs_without_summary", recursive = T)
dir.create("blobs_without_summary")
unlink("plots_with_summary", recursive = T)
dir.create("plots_with_summary")

blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["02-plots-with-datum"]] 

existing_datum_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  )

# TODO:
# there are duplicated packages for WCC, 
#   looks like 20211220, 20211109 maybe?
# need to hash all the scan files and make sure they're dupes and pick one



summarized_blobs <- 
  AzureStor::list_blobs(
    AzureStor::list_blob_containers(
      sas_endpoint, 
      sas = sas_token
    )[["03-plots-with-summary"]], 
    info = "name"
  ) %>% 
  stringr::str_extract(uuid_rx) %>% 
  unique()


datum_geojsons <- 
  stringr::str_subset(
    existing_datum_blobs,
    glue::glue("{uuid_rx}\\.geojson")
  ) %>% 
  filter_uuids(summarized_blobs)



AzureStor::multidownload_blob(
  blob_ctr,
  datum_geojsons,
  file.path("blobs_without_summary", datum_geojsons),
  overwrite = T
)


# datum to summaries ----

col_prop <- function(x, brks, nm) {
  lbl <- paste(nm, head(brks, -1), tail(brks, -1), sep = "_")
  
  if (all(is.na(x))) {
    ret <- rep(NA, length(lbl)) %>% 
      purrr::set_names(lbl) 
  } else {
      x_lbl <- cut(x, brks, lbl, right = F)
      
      ret <- (table(x_lbl)/length(x)) 
  }
  
  ret %>% 
    bind_rows() %>% 
    mutate_all(as.numeric)
}


summarise_plot <- function(trk) {
  trk <- trk %>% 
    group_by(
      flag, 
      across(matches("Field")),
      across(matches("Species")), 
      across(matches("Rep")),
      across(matches("plotID"))
      )

  flags_with_props <- 
    trk %>% 
    summarise(
      ndvi = col_prop(NDVI, c(0.1, 0.29, 0.4, 0.53, 0.69, 1), "ndvi"),
      sonar = col_prop(sonar_ht, c(0, 5, 10, 15, 20, 40, Inf), "sonar"),
      lidar = col_prop(lidar_ht, c(0, 5, 10, 15, 20, 30, 40, Inf), "lidar"),
      sonar_brown = col_prop(sonar_ht_brown, c(0, 5, 10, 15, 20, 40, Inf), "sonar_brown"),
      lidar_brown = col_prop(lidar_ht_brown, c(0, 5, 10, 15, 20, 30, 40, Inf), "lidar_brown"),
      .groups = "keep"
    ) %>%  
    tidyr::unnest(cols = c(ndvi, sonar, lidar, sonar_brown, lidar_brown))
  
  flags_with_moments <- 
    trk %>% 
    mutate_at(
      vars(NDVI, sonar_ht, lidar_ht, sonar_ht_brown, lidar_ht_brown),
      as.numeric
    ) %>% 
    summarise_at(
      vars(NDVI, sonar_ht, lidar_ht, sonar_ht_brown, lidar_ht_brown),
      list(
        mean = ~mean(.),
        cv = ~mean(.)/sd(.),
        skew = ~skewness(.),
        kurt = ~kurtosis(.)
      )
    )

  full_join(
    flags_with_moments, 
    flags_with_props
  ) %>% 
    mutate(fn = trk$fn[1]) |> 
    select(fn, everything())
}

datum_sf <- dir(
  "blobs_without_summary",
  pattern = "geojson$",
  full.names = T
) %>% 
  purrr::map(read_sf, .progress = T)



scan_summaries <- 
  datum_sf %>% 
  purrr::map(
    ~st_drop_geometry(.x) %>% 
      purrr::safely(summarise_plot)(),
    .progress = T
  )

purrr::map(scan_summaries, "error") %>% 
  purrr::compact()

purrr::map(scan_summaries, "result") %>% 
  purrr::compact()

# upload ----
scan_summaries %>% 
  purrr::map("result") %>% 
  purrr::compact() %>% 
  purrr::map(
    ~{
      fn <- basename(.x$fn[1])
      
      if (is.na(fn)) { browser() }
      
      readr::write_csv(
        .x, file.path("plots_with_summary", replace_ext(fn, "csv")),
        append = F
      )
    }
  )

sum_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["03-plots-with-summary"]] 

# TODO: do/don't overwrite existing blobs?

summarized_plots_pushed <- 
  dir(
    "plots_with_summary",
    full.names = T,
    pattern = "csv"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, sum_ctr),
  )

summarized_plots_pushed %>% 
  purrr::map("error") %>% 
  purrr::compact()


