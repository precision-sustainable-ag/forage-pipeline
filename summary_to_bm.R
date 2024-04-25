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
  stringr::str_subset("_strip_|_WCC_") # TODO update as needed

AzureStor::multidownload_blob(
  blob_ctr,
  existing_summary_blobs,
  file.path("blobs_without_biomass", existing_summary_blobs),
  overwrite = T
)

## Strip ----
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


## WCC ----
wcc_plot_summaries <- 
  list.files(
    "blobs_without_biomass",
    full.names = T,
    pattern = "_S_"
  ) %>% 
  stringr::str_subset("_WCC_") %>% 
  purrr::set_names(str_extract(., uuid_rx)) %>% 
  purrr::map(readr::read_csv) %>% 
  bind_rows(.id = "uuid") %>% 
  mutate(
    scan_date = str_extract(fn, "_[0-9]{8}_"),
    scan_date = str_remove_all(scan_date, "_"),
    .after = plotID
  )

wcc_biomass <- 
  list(
    "2019" = "1rpXS7K8Z1iSGPwl3W21IDNUz4ut-5xLr599LCRbSXhQ",
    "2020" = "1MKNXikH0ftfggsfNZQ-zJKS8rbbvj9w3IuOc4eBozw0",
    "2021" = "1JQ9dx5VBbl0JhKvQGGj05T7k1U6pjxozVMeuezVcTYk",
    "2022" = "10rLeDjzDivrAmB9t1JYahrVzP7VaOTvpRXgiv1TWb5U",
    "2023" = "12YEG8YrvegO5emxtIKRLo9Iaz5RI4Xp6eXXBk7BM0_0",
    "2024" = "1ddRKAAr6GgGup9s3iV89jhpsUHjuZShiUpBUk6VL_hY"
  ) %>% 
  purrr::map(
    ~{
      Sys.sleep(1)
      googlesheets4::read_sheet(
      .x, sheet = "BARC", col_types = "c",
      .name_repair = ~make.unique(.x)
    ) %>% 
      select(
        Field, plotID = Plot, matches("Species"), Latitude, Longitude, 
        FS_box_date, Field_sampling_date, 
        matches("Height"), matches("Dry_BM_kG_ha"), 
        matches("Dry_Total_BM_kG_ha")
      )
    }
  )

wcc_biomass_combined <- 
  wcc_biomass %>% 
  bind_rows(.id = "harvest_year") %>% 
  mutate(
    Dry_BM_kG_ha = coalesce(Dry_BM_kG_ha, Dry_Total_BM_kG_ha),
    scan_date = coalesce(
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
  filter(!is.na(plotID)) %>% 
  select(
    harvest_year, Field, plotID, Species, scan_date, 
    Latitude, Longitude, 
    Dry_BM_kG_ha, matches("Height")
  ) 


wcc_to_push <- 
  left_join(
    wcc_plot_summaries %>% 
      mutate(
        plotID = as.character(plotID),
        scan_date = lubridate::as_date(scan_date)
      ),
    wcc_biomass_combined
  ) %>% 
  filter(!is.na(plotID))

wcc_to_push %>% 
  filter(is.na(Dry_BM_kG_ha)) %>% 
  View()


readr::write_csv(
  wcc_to_push %>% 
    filter(!is.na(Dry_BM_kG_ha)),
  "plots_with_biomass/forage_box_wcc.csv"
)

## Export ----
bio_ctr <- AzureStor::list_blob_containers(
  sas_endpoint, 
  sas = sas_token
)[["04-plots-with-biomass"]] 


# TODO: fix not uploading when blob exists
biomass_plots_pushed <- 
  dir(
    "plots_with_biomass",
    full.names = T,
    pattern = "csv"
  ) %>%
  purrr::map(
    ~purrr::safely(forage_upload)(.x, bio_ctr),
  )
