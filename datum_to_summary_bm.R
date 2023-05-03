library(dplyr)
library(moments)

# datum to summaries ----

col_prop <- function(x, brks, nm) {
  lbl <- paste(nm, head(brks, -1), tail(brks, -1), sep = "_")
  x_lbl <- cut(x, brks, lbl, right = F)
  
  (table(x_lbl)/length(x)) |> 
    bind_rows() |> 
    mutate_all(as.numeric)
}


summarise_plot <- function(trk) {
  trk <- trk |> filter(flag != 0) |> group_by(flag)
  
  flags_with_props <- 
    trk |> 
    summarise(
      ndvi = col_prop(NDVI, c(0.1, 0.29, 0.4, 0.53, 0.69, 1), "ndvi"),
      sonar = col_prop(sonar_ht, c(0, 5, 10, 15, 20, 40, Inf), "sonar"),
      lidar = col_prop(lidar_ht, c(0, 5, 10, 15, 20, 30, 40, Inf), "lidar")
    ) |> 
    tidyr::unnest(cols = c(ndvi, sonar, lidar))
  
  flags_with_moments <- 
    trk |> 
    summarise_at(
      vars(NDVI, sonar_ht, lidar_ht),
      list(
        mean = ~mean(.),
        cv = ~mean(.)/sd(.),
        skew = ~skewness(.),
        kurt = ~kurtosis(.)
      )
    )

  full_join(
    flags_with_moments, 
    flags_with_props,
    by = "flag"
  ) |> 
    mutate(fn = trk$fn[1]) |> 
    select(fn, everything())
}

onfarm_summaries <- 
  onfarm_loops_with_heights |>
  purrr::map(
    ~st_drop_geometry(.x) |> 
      summarise_plot()
  )





library(httr)
# datum plots to biomass ----
onfarm_biomass_rq <- GET(
  "https://api.precisionsustainableag.org/onfarm/biomass",
  query = list(
    output = "json",
    subplot = "separate"
  ),
  add_headers("x-api-key" = readLines("api_token_example.txt"))
)

onfarm_biomass <- 
  content(onfarm_biomass_rq, as = "text") |> 
  jsonlite::fromJSON()


onfarm_with_bm <- 
  bind_rows(onfarm_summaries) |> 
  mutate(fn = basename(fn)) |> 
  tidyr::separate(
    fn,
    c("box", "proj", "location", "timing", NA, 
      "rep", NA, "scan_date", "uuid"),
    sep = "_"
  ) |> 
  filter(timing == "biomass") |> 
  mutate(
    rep = str_remove_all(rep, "rep")
  ) |> 
  left_join(
    onfarm_biomass,
    by = c("location" = "code", "rep" = "subplot")
  )


onfarm_with_bm |> 
  ggplot(aes(sonar_ht_mean, uncorrected_cc_dry_biomass_kg_ha)) +
  geom_point()

readr::write_csv(onfarm_with_bm, "onfarm_2022_scans_with_biomass.csv")
