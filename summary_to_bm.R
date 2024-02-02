library(dplyr)
library(stringr)

source("secret.R")

replace_ext <- function(fn, ext) {
  paste0(tools::file_path_sans_ext(fn), ".", ext)
}

hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12) 



unlink("blobs_without_biomass", recursive = T)
dir.create("blobs_without_biomass")
dir.create("plots_with_biomass")


blob_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["03-plots-with-summary"]] 

existing_summary_blobs <- 
  AzureStor::list_blobs(
    blob_ctr, info = "name"
  ) %>% 
  stringr::str_subset(
    glue::glue("{uuid_rx}\\.csv")
  ) %>% 
  stringr::str_subset("_strip_") # TODO update as needed

AzureStor::multidownload_blob(
  blob_ctr,
  existing_summary_blobs,
  file.path("blobs_without_biomass", existing_summary_blobs),
  overwrite = T
)


## Get physical data from blob storage:
phys_ctr <- 
  AzureStor::list_blob_containers(
    sas_endpoint, 
    sas = sas_token
  )[["landing-zone"]] 

phys_blobs <- 
  AzureStor::list_blobs(
    phys_ctr, info = "name"
  ) %>% 
  stringr::str_subset(
    glue::glue("{uuid_rx}\\.csv")
  ) %>% 
  stringr::str_subset("_strip_") %>% 
  stringr::str_subset("_P_[0-9]{8}")

AzureStor::multidownload_blob(
  phys_ctr,
  phys_blobs,
  file.path("blobs_without_biomass", phys_blobs),
  overwrite = T
)

# Are there any missing physical files?
existing_summary_blobs[
  !(str_extract(existing_summary_blobs, uuid_rx) %in% 
    str_extract(phys_blobs, uuid_rx))
  ]

strip_plot_summaries <- 
  list.files(
  "blobs_without_biomass",
  full.names = T,
  pattern = "_S_"
) %>% 
  stringr::str_subset("_strip_") %>% 
  purrr::set_names(str_extract(., uuid_rx)) %>% 
  purrr::map(readr::read_csv) %>% 
  bind_rows(.id = "uuid")

strip_plot_physical <- 
  list.files(
    "blobs_without_biomass",
    full.names = T,
    pattern = "_P_"
  ) %>% 
  stringr::str_subset("_strip_") %>% 
  purrr::set_names(str_extract(., uuid_rx)) %>% 
  purrr::map(
    ~readr::read_csv(.x, col_types = readr::cols(.default = "c"))
    ) %>% 
  bind_rows(.id = "uuid")


sort(unique(strip_plot_summaries$Species))
sort(unique(strip_plot_physical$Species))


strip_all <- 
  strip_plot_physical %>% 
  mutate(`plotWt (g)` = as.numeric(`plotWt (g)`)) %>% 
  filter(!is.na(`plotWt (g)`)) %>% 
  full_join(
    strip_plot_summaries %>% 
      filter(flag == "1"),
    by = c("uuid", "Species", "plotID")
    ) 

# There are many physical rows with no match, since the physical
#   files were complete across a day, but scans are only subsets of
#   species within a day.
strip_all %>% 
  filter(is.na(NDVI_mean)) %>% 
  View()

# There are missing physical data where there is a species mismatch
#   plus some others to investigate
strip_all %>% 
  filter(is.na(`plotWt (g)`)) %>% 
  View()


strip_complete <- 
  strip_all %>% 
  filter(
    !is.na(NDVI_mean), 
    !is.na(`plotWt (g)`)
  )

# what is the default quadrat size?
# Let's recode all the quadrats:
#   In MD it's always 0.6m^2 (3 1m rows) = 3*(7.5*2.54/100)*(100/100)
#   In NC it's either 3*(7.5*2.54/100)*(50/100) or double that
strip_to_push <- 
  strip_complete %>% 
  mutate(
    quadSize = str_remove_all(quadSize, "m\\^2$"),
    quadSize = as.numeric(quadSize),
    quadSize_m2 = as.numeric(quadSize_m2),
    quad_m2 = coalesce(quadSize_m2, quadSize)
    ) %>%
  mutate(
    exact_quad_m2 = case_when(
      str_detect(fn, "_strip_MD_") ~ 3*(7.5*2.54/100)*(100/100),
      str_detect(fn, "_strip_NC_") & quad_m2 == 0.25 ~ 3*(7.5*2.54/100)*(50/100),
      str_detect(fn, "_strip_NC_") & quad_m2 == 0.5 ~ 3*(7.5*2.54/100)*(100/100)
    )
  ) %>% 
  mutate(
    biomass_kg_ha = `plotWt (g)`/exact_quad_m2 * (10000 / 1000),
    fn = basename(fn)
  ) %>% 
  select(
    uuid, Date, location, Species, plotID,
    biomass_kg_ha, canopy_ht_cm = `canopy_ht_ruler (cm)`,
    biomass_g_quadrat_recorded = `plotWt (g)`,
    quadrat_m2_recorded = quad_m2,
    quadrat_m2_exact = exact_quad_m2,
    Notes,
    fn,
    matches("ndvi"), matches("sonar"), matches("lidar")
    ) 

strip_to_push

readr::write_csv(
  strip_to_push,
  "plots_with_biomass/forage_box_strip_trial.csv"
  )


bio_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["04-plots-with-biomass"]] 


biomass_plots_pushed <- 
  dir(
    "plots_with_biomass",
    full.names = T,
    pattern = "csv"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, bio_ctr),
  )
