
mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}


# labeled blob to datum ----

track_rm_outliers <- function(trk) {

  trk_clean <- trk |> filter(!is.na(SONAR), !is.na(LIDAR)) |> 
    filter(SONAR < 999, LIDAR < 999)
  
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
    trk_clean %>%
    filter(LIDAR < lidar_3sd_high & LIDAR > lidar_3sd_low) %>%
    filter(SONAR < sonar_3sd_high & SONAR > sonar_3sd_low)
}

track_add_datum <- function(trk) {
  trk_lidar_top_decile <-
    trk %>%
    filter(cume_dist(LIDAR) > 0.89) %>%
    pull(LIDAR) |> 
    mode()
  
  trk_sonar_top_decile <-
    trk %>%
    filter(cume_dist(SONAR) > 0.89) %>%
    pull(SONAR) |> 
    mode()
  
  
  trk %>%
    mutate(lidar_datum = trk_lidar_top_decile) %>%
    mutate(sonar_datum = trk_sonar_top_decile) %>%
    mutate(lidar_ht = lidar_datum - LIDAR) %>%
    mutate(sonar_ht = sonar_datum - SONAR)
  
  # TODO: Don't we need coefficients for these units to get cm?
}


onfarm_loops_with_heights <- 
  onfarm_loops |> 
  purrr::map(
    ~track_rm_outliers(.x) |> 
      track_add_datum()
  )

# re-export to blob storage ----





