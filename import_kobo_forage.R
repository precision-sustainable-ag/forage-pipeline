library(httr)
library(purrr)
library(dplyr)
library(dbplyr)
library(DBI)
library(stringr)
library(glue)

unlink("forage_submissions", recursive = T)
dir.create("forage_submissions")



hex_rx <- function(...) {
  n <- c(...)
  hex <- "[a-fA-F0-9]"
  glue::glue("{hex}{{{n}}}") %>% 
    paste0(collapse = "-")
}

uuid_rx <- hex_rx(8, 4, 4, 4, 12)  

# uuid_rx <- 
#   "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"

# Update: ----
known_bad_UUIDs <- c(
  "0a6915a2-e5ea-422a-88f4-b1caa24386e1",
  "a93dcf88-8f41-415c-9f0f-2fdcf9c22f1c",
  "9a15301e-8abd-4f58-994c-417c84492e3b",
  "09ced5c3-5c4f-4614-90f2-9f0f6db02212",
  "188b4dd9-070e-4491-8846-21018b6d9e46",
  "af1cc7d5-ee79-4ced-ab6f-dbabd49aeb94",
  "50f2717f-f1db-4c58-bb00-1c42056220f3",
  "ac8c83a2-fd71-4e93-88d5-41cf44ba5608",
  "35792bc0-1f63-41e4-8c02-5640a7d58217",
  "c6b69f0a-f54c-4685-9001-54ad0c1c5d2c",
  "17bf06cd-5da8-4a4c-aea0-aaae100782bf",
  "2572255b-1cff-48e5-97f9-902c89ab20b9",
  "a4df08b1-f95a-4e55-a2ca-5033f8d11de6",
  "0a6915a2-e5ea-422a-88f4-b1caa24386e1",
  "a93dcf88-8f41-415c-9f0f-2fdcf9c22f1c",
  "9a15301e-8abd-4f58-994c-417c84492e3b",
  "09ced5c3-5c4f-4614-90f2-9f0f6db02212",
  "188b4dd9-070e-4491-8846-21018b6d9e46",
  "af1cc7d5-ee79-4ced-ab6f-dbabd49aeb94",
  "50f2717f-f1db-4c58-bb00-1c42056220f3",
  "ac8c83a2-fd71-4e93-88d5-41cf44ba5608",
  "35792bc0-1f63-41e4-8c02-5640a7d58217",
  "c6b69f0a-f54c-4685-9001-54ad0c1c5d2c",
  "17bf06cd-5da8-4a4c-aea0-aaae100782bf",
  "2572255b-1cff-48e5-97f9-902c89ab20b9",
  "a4df08b1-f95a-4e55-a2ca-5033f8d11de6",
  "2777c2fd-30c0-4992-a87f-c39dd8c9061c",
  "09ced5c3-5c4f-4614-90f2-9f0f6db02212",
  "09ced5c3-5c4f-4614-90f2-9f0f6db02212",
  "0a6915a2-e5ea-422a-88f4-b1caa24386e1",
  "188b4dd9-070e-4491-8846-21018b6d9e46",
  "50f2717f-f1db-4c58-bb00-1c42056220f3",
  "9a15301e-8abd-4f58-994c-417c84492e3b",
  "a93dcf88-8f41-415c-9f0f-2fdcf9c22f1c",
  "af1cc7d5-ee79-4ced-ab6f-dbabd49aeb94"
)


# Authenticate: ----
source("secret.R")
# kobo_token, sas_endpoint, sas_token, this_env

  

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

existing_UUIDs <- 
  existing_blobs %>% 
  stringr::str_extract(
    uuid_rx
  ) %>% 
  na.omit() %>% 
  unique()


# Forage onfarm form is "axnBDev39ubd4UJtqhk2yX"
#   Found via visual inspection
# forage_submissions_response <- GET(
#   url = "https://kf.kobotoolbox.org/api/v2/assets/axnBDev39ubd4UJtqhk2yX/data",
#   add_headers("Authorization" = kobo_token)
# )

forage_submissions_response_list <- 
  purrr::map(
    c("axnBDev39ubd4UJtqhk2yX",  # onfarm
      "aEa2NnJ3sBehTwBnrZskGP",  # CE1
      "a4ySgqduEHWvdoyKDroE5T"), # CE2
    ~GET(
      url = glue("https://kf.kobotoolbox.org/api/v2/assets/{.x}/data"),
      add_headers("Authorization" = kobo_token)
    )
  )

forage_submissions <- 
  purrr::map(
    forage_submissions_response_list,
    ~content(.x)$results
  ) %>% 
  flatten()


# Function definitions: ----
forage_extract_metadata <- function(elt) {
  
  scan_date = (elt[["date"]] %||% elt[["today"]]) %>% str_remove_all("-")
  uuid = elt[["_uuid"]]
  timing = elt[["time"]] %||% elt[["timing"]] %||% "biomass" 
  
  if (uuid %in% known_bad_UUIDs) {
    return(NULL)
  }
  
  sub_time = lubridate::ymd_hms(elt[["_submission_time"]])
  if (sub_time < lubridate::ymd_hms("2021-12-31 00:00:00")) {
    stop("Very old form version, _uuid: ", elt[["_uuid"]])
  }
  
  proj = elt[["experiment"]] %||% str_extract(elt[["form_name"]], "onfarm$|ce1$|ce2$") 
  if (!(proj %in% c("onfarm", "ce1", "ce2"))) {
    proj = "onfarm"
  }
  
  location = 
    (elt[["code"]] %||% elt[["ce1"]] %||% elt[["ce2"]] %||% "") %>% 
    str_trim() %>% 
    str_to_upper()
  
  stop_msg <- jsonlite::toJSON(
    list(code = location, uuid = uuid)
    )
  

  if (proj == "onfarm") {
    if (!str_detect(location, "^[A-Z0-9]{3}$")) {
      stop("Malformed location: ", stop_msg, call. = F)
      }
  } else if (proj == "ce1") {
    if (!str_detect(location, "^[A-Z]{2}CE1$")) {
      stop("Malformed location: ", stop_msg, call. = F)
    }
  } else if (proj == "ce2") {
    if (!str_detect(location, "^[A-Z]{2}CE2$")) {
      stop("Malformed location: ", stop_msg, call. = F)
    }
  }
  
  ret <- tibble(proj, location, timing, scan_date, uuid)
  
  if (!nrow(ret)) {
    stop(
      "Something is missing: ", 
      jsonlite::toJSON(lst(proj, location, timing, scan_date, uuid)),
      call. = F
      )
  }
  
  ret
}


raw_forms_metadata <-
  purrr::map(
    forage_submissions,
    purrr::safely(forage_extract_metadata)
  )

raw_forms_metadata %>% purrr::map("error")
raw_forms_metadata %>% purrr::map("result")


forage_extract_urls <- function(elt) {
  ret <- elt[["_attachments"]] %>% 
    purrr::map(as_tibble) %>% 
    bind_rows() 
  
  if (!nrow(ret)) {
    stop(
      "No attachments detected: ", 
      jsonlite::toJSON(list(uuid = elt[["_uuid"]])),
      call. = F
    )
  }
  
  ret %>% 
    select(download_url, filename) %>% 
    mutate(
      filename = basename(filename)
    )
}


raw_forms_urls <- purrr::map(
  forage_submissions,
  purrr::safely(forage_extract_urls)
)

raw_forms_urls %>% purrr::map("result")

forage_extract_data <- function(elt) {
  err <- function(e) {
    stop(
      "No function yet for version: ",
      e[["__version__"]],
      "; UUID: ", e[["_uuid"]]
      )
  }
  
  f <- switch(
    elt[["__version__"]],
    "vs5DePTxmUxkL5pQrzx5hd" = forage_extract_data_2022,
    "vg6gRM9XuDVyCmgFqF7f4b" = forage_extract_data_2022,
    "v99aTCmLkehhLMXExdD6Yi" = forage_extract_data_2022,
    "vtPieLBJh95pFM7T6CFQdn" = forage_extract_data_2022,
    "vDrzDaJHEW6bNJobTLcjqi" = forage_extract_data_2023_onfarm,
    "vD4CFku8m7exZDidRGtzdW" = forage_extract_data_2023_onfarm,
    "v5gRvjhRAGtyuJaredYtSe" = forage_extract_data_2023_ce1,
    "vDMCUQLU8rgUzdALXfnwes" = forage_extract_data_2023_ce2,
    "vkYzYnhqFb2EyyauMUVMxh" = forage_extract_data_2023_ce2,
    "vBcugdcYhkjJFR5pJCxnPQ" = forage_extract_data_2023_ce1,
    "vDSLduafNzHx2Xo5bNBJXp" = forage_extract_data_2023_ce1,
    err
  )

  f(elt)
}

forage_extract_data_2022 <- function(elt) {
  nms <- c(
    "cover_crop_image_rep1", "cover_crop_image_rep2", 
    "file1", "file2", "file3", "file4", 
    "rep1_rye", "rep1_legume", "rep1_mix", 
    "rep2_rye", "rep2_legume", "rep2_mix", 
    "rep3_rye", "rep3_legume", "rep3_mix", 
    "rep4_rye", "rep4_legume", "rep4_mix"
  )
  
  ret <- elt[nms] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "filename") %>% 
    tidyr::unnest(filename) %>% 
    mutate(
      id_code = paste0("rep", str_extract(field, "[1-4]")),
      species = str_extract(field, "cover|rye|legume|mix"),
      species = replace(species, is.na(species), "all"),
      ext = str_to_lower(str_extract(filename, "jpg|JPG|csv|CSV")),
      type = case_when(
        ext == "jpg" ~ "I",
        ext == "csv" ~ "S",
        T ~ "U"
      )
    )
  
  if (!nrow(ret)) {
    stop(
      "No scan (CSV) or image (jpg) filenames detected: ", 
      jsonlite::toJSON(list(uuid = elt[["_uuid"]])),
      call. = F
    )
  }
  
  if (any(ret$type == "U")) {
    stop(
      "Unknown filetype: ", 
      jsonlite::toJSON(ret %>% filter(type == "U")),
      call. = F
    )
  }
  
  ret
}

forage_extract_data_2023_onfarm <- function(elt) {
  nms <- c(
    "cover_crop_image_rep1", "cover_crop_image_rep2", 
    "bare_rep1", "bare_rep2",
    "file1", "file2"
  )
  
  ret <- elt[nms] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "filename") %>% 
    tidyr::unnest(filename) %>% 
    mutate(
      id_code = paste0("rep", str_extract(field, "[1-4]")),
      species = str_extract(field, "cover|rye|legume|mix"),
      species = replace(species, is.na(species), "all"),
      ext = str_to_lower(str_extract(filename, "jpg|JPG|csv|CSV")),
      type = case_when(
        ext == "jpg" ~ "I",
        ext == "csv" ~ "S",
        T ~ "U"
      )
    )
  
  if (!nrow(ret)) {
    stop(
      "No scan (CSV) or image (jpg) filenames detected: ", 
      jsonlite::toJSON(list(uuid = elt[["_uuid"]])),
      call. = F
    )
  }
  
  if (any(ret$type == "U")) {
    stop(
      "Unknown filetype: ", 
      jsonlite::toJSON(ret %>% filter(type == "U")),
      call. = F
    )
  }
  
  ret
}

forage_extract_data_2023_ce1 <- function(elt) {
  
  if (elt[["__version__"]] == "v5gRvjhRAGtyuJaredYtSe") {
    orig <- names(elt)
    orig[orig == "rep4_bare"] <- "rep3_bare"
    orig[orig == "rep4_bare_001"] <- "rep4_bare"
    names(elt) <- orig
  }
  
  nms <- c(
    "rep1", "rep2", "rep3", "rep4",
    "rep1_rye", "rep1_legume", "rep1_mix", "rep1_bare",
    "rep2_rye", "rep2_legume", "rep2_mix", "rep2_bare",
    "rep3_rye", "rep3_legume", "rep3_mix", "rep3_bare", 
    "rep4_rye", "rep4_legume", "rep4_mix", "rep4_bare" 
  )
  
  orders <- 
    elt[str_detect(names(elt), "rep[1-4]_walking_order[1-4]")] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "treatment") %>% 
    tidyr::unnest(treatment) %>% 
    mutate(
      id_code = str_extract(field, "^rep[1-4]"),
      order = str_extract(field, "[1-4]$"),
      treatment = str_replace_all(treatment, "_", "-")
    ) %>% 
    arrange(id_code, order) %>% 
    group_by(id_code) %>% 
    summarise(treatment = paste(treatment, collapse = "~"))

  if (!length(elt[["timing"]]) & !length(elt[["time"]])) {
    stop("Missing treatment timing, UUID: ", elt[["_uuid"]])
  }
  
  photo_ids <- 
    expand.grid(
      treatment = ifelse(
        (elt[["timing"]] %||% elt[["time"]]) == "no", 
        "early", 
        "biomass"
        ),
      id_code = paste0("rep", as.character(1:4)),
      species = c("rye", "legume", "mix", "bare")
    ) %>% 
    mutate(id_code = paste(id_code, species, sep = "_")) %>% 
    select(-species)

  
  ret <- elt[nms] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "filename") %>% 
    tidyr::unnest(filename) %>% 
    mutate(
      id_code = field,
      species = "all",
      ext = str_to_lower(str_extract(filename, "jpg|JPG|csv|CSV")),
      type = case_when(
        ext == "jpg" ~ "I",
        ext == "csv" ~ "S",
        T ~ "U"
      )
    ) 
  
  ret <- 
    ret %>% 
    left_join(
      bind_rows(orders, photo_ids),
      by = "id_code"
    ) %>% 
    mutate(id_code = paste(id_code, treatment, sep = "~")) 
  
  if (!nrow(ret)) {
    stop(
      "No scan (CSV) or image (jpg) filenames detected: ", 
      jsonlite::toJSON(list(uuid = elt[["_uuid"]])),
      call. = F
    )
  }
  
  if (any(ret$type == "U")) {
    stop(
      "Unknown filetype: ", 
      jsonlite::toJSON(ret %>% filter(type == "U")),
      call. = F
    )
  }
  ret
}

forage_extract_data_2023_ce2 <- function(elt) {
  nms <- c(
    "walkA", "walkB", "walkC", "walkD",
    "block1covercrop", "block1bare", 
    "block2covercrop", "block2bare", 
    "block3covercrop", "block3bare", 
    "block4covercrop", "block4bare", 
    "block5covercrop", "block5bare"
  )

  orders <- 
    elt[str_detect(names(elt), "walk[A-D]_walking_order[1-5]")] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "treatment") %>% 
    tidyr::unnest(treatment) %>% 
    mutate(
      id_code = str_extract(field, "^walk[A-D]"),
      order = str_extract(field, "[1-5]$"),
      treatment = str_replace_all(treatment, "no_cover", "bare"),
      treatment = str_replace_all(treatment, "_", "-")
    ) %>% 
    arrange(id_code, order) %>% 
    group_by(id_code) %>% 
    summarise(treatment = paste(treatment, collapse = "~"))

  if (!length(elt[["time"]])) {
    stop("Missing treatment timing, UUID: ", elt[["_uuid"]])
  }
  
  photo_ids <- 
    expand.grid(
      treatment = elt[["time"]],
      rep = 1:5,
      trt = c("cover", "bare")
    ) %>% 
    mutate(id_code = paste0("block", rep, trt)) %>% 
    select(-rep, -trt)
  
  ret <- elt[nms] %>% 
    purrr::compact() %>% 
    tibble::enframe("field", "filename") %>% 
    tidyr::unnest(filename) %>% 
    mutate(
      id_code = str_extract(field, "walk[A-D]$|block[1-5](bare|cover)"),
      species = "rye",
      ext = str_to_lower(str_extract(filename, "jpg|JPG|csv|CSV")),
      type = case_when(
        ext == "jpg" ~ "I",
        ext == "csv" ~ "S",
        T ~ "U"
      )
    ) 
  
  ret <- 
    ret %>% 
    left_join(
      bind_rows(orders, photo_ids),
      by = "id_code"
      ) %>% 
    mutate(id_code = paste(id_code, treatment, sep = "~"))
  
  if (!nrow(ret)) {
    stop(
      "No scan (CSV) or image (jpg) filenames detected: ", 
      jsonlite::toJSON(list(uuid = elt[["_uuid"]])),
      call. = F
    )
  }
  
  if (any(ret$type == "U")) {
    stop(
      "Unknown filetype: ", 
      jsonlite::toJSON(ret %>% filter(type == "U")),
      call. = F
    )
  }
  ret
}
# TODO fix up UUID 271c5cba

raw_forms_filenames <- purrr::map(
  forage_submissions,
  ~purrr::safely(forage_extract_data)(.x)
)

raw_forms_filenames %>% purrr::map("result")
raw_forms_filenames %>% purrr::map("error")

forage_parse <- function(elt, pre_existing = "") {

  # TODO: generating "argument is of length zero err"
  meta <- forage_extract_metadata(elt)
  if (!length(meta) | meta$uuid %in% pre_existing) {
    return(NULL)
  }

  urls <- forage_extract_urls(elt)
  files <- forage_extract_data(elt)
  
  if (nrow(filter(files, type == "S")) == 0) {
    stop(
      "Form missing all scan files:\n", meta$uuid, "\n",
      jsonlite::toJSON(count(files, ext, type))
      )
  }
  
  # left_join needed here because there was a submission that
  #   had a "ghost" attachment, site YLM. A CSV was attached
  #   without being referred to in the form fields
  ret <- full_join(
    meta,
    left_join(files, urls, by = "filename"),
    by = character()
  )
  
  if (anyNA(ret)) {
    stop("Missing data in form: ", jsonlite::toJSON(ret))
  }
  
  ret
}




forage_download <- function(dl_url, file_nm) {
  if (!dir.exists("forage_submissions")) {
    stop("There's no `/forage_submissions` directory in: ", getwd())
  }
  
  message(file_nm)
  res <- RETRY(
    "GET",
    dl_url,
    add_headers("Authorization" = kobo_token),
    write_disk(file.path("forage_submissions", file_nm), overwrite = T),
    progress()
  )
  
  # check size and status before proceeding
  stop_for_status(res)
  
  local_fn <- normalizePath(file.path("forage_submissions", file_nm))
  res_size <- headers(res)[["content-length"]] %>% 
    as.numeric()
  local_size <- file.size(local_fn)
  
  if (
    local_size == 0 | 
      res_size == 0 | 
      local_size != res_size
  ) {
    # TODO: auto delete zero size files, don't make an error
    # Fixed?
    message(
      "Incompatible sizes: response, ", 
      res_size, "; local, ", local_size,
      "; ", file_nm
      )
    return("")
  }
  
  return(local_fn)
  
}

forage_upload <- function(lcl_nm, cnt) {
  if (length(lcl_nm) > 1) { 
    stop("Too many names: ", paste(lcl_nm, collapse = "\n"))
  }
  
  message("\n", lcl_nm)
  
  lcl_sz <- file.size(lcl_nm)
  base_nm <- basename(lcl_nm)
  
  blob_tbl <- 
    AzureStor::list_blobs(cnt, prefix = base_nm)
  
  if (nrow(blob_tbl)) {
    stop("Pre-existing blob for: ", base_nm)
  }
  
  AzureStor::upload_blob(cnt, lcl_nm, base_nm)
  
  blob_tbl <- 
    AzureStor::list_blobs(cnt, prefix = base_nm)
  
  if (nrow(blob_tbl) == 0) { stop("Unsuccessful upload, no blob") }
  
  if (nrow(blob_tbl) > 1) {
    stop("Name collision, multiple matches for: ", base_nm)
  }
  
  if (blob_tbl$size != lcl_sz) {
    stop("Incompatible sizes: blob, ", blob_tbl$size,
         "; local, ", lcl_sz)
  }
  
  return(blob_tbl)
}

## Execute: ----
raw_forms_tibbles <- 
  purrr::map(
    forage_submissions,
    ~purrr::safely(forage_parse)(
      .x, 
      pre_existing = c(existing_UUIDs, known_bad_UUIDs)
    )
  )

raw_forms_tibbles_dirty <- 
  raw_forms_tibbles %>% 
  purrr::map("error") %>% 
  purrr::compact() %>% 
  purrr::map_chr("message")

raw_forms_tibbles_clean <- 
  raw_forms_tibbles %>% 
  purrr::map("result") %>% 
  bind_rows() %>% 
  mutate(
    fn = glue::glue(
      "box214v2_{proj}_{location}_{timing}_{species}_{id_code}_{type}_{scan_date}_{uuid}.{ext}"
    )
  ) 

raw_forms_local <- 
  raw_forms_tibbles_clean %>% 
  mutate(
    local_fn = purrr::map2(
      download_url, fn, purrr::safely(forage_download)
    )
  )


raw_forms_local_to_push <- 
  raw_forms_local %>% 
  group_by(uuid) %>% 
  filter(
    purrr::map_lgl(
      local_fn, 
      ~length(.x$result) %>% as.logical()
    ) %>% all()
  ) %>% 
  mutate(
    local_fn = purrr::map_chr(local_fn, "result")
  )


raw_forms_local_to_check <- 
  raw_forms_local %>% 
  group_by(uuid) %>% 
  filter(
    purrr::map_lgl(
      local_fn, 
      ~length(.x$error) %>% as.logical()
    ) %>% any()
  ) %>% 
  mutate(
    local_fn = purrr::map_chr(
      local_fn, 
      ~(.x[["error"]] %||% .x[["result"]]) %>% as.character() 
      )
  )

blobs_pushed <- 
  raw_forms_local_to_push %>% 
  pull(local_fn) %>% 
  stringr::str_subset("^$", negate = T) %>% 
  purrr::set_names() %>% 
  purrr::map(
    purrr::safely(forage_upload),
    cnt = blob_ctr
    )

blobs_to_check <- 
  purrr::map(blobs_pushed, "error") %>% 
  purrr::compact()



## Investigate: ----
bind_smartly <- function(..., .id = NULL) {
  l <- list(...)
  purrr::map(l, ~mutate_all(.x, as.character)) %>% 
    bind_rows(.id = .id)
}

forms_to_check <- 
  bind_smartly(
    "Convert raw form to metadata" = 
      tibble(
        err = raw_forms_tibbles_dirty,
        uuid = stringr::str_extract(err, uuid_rx)
      ),
    "Download attachments" =
      raw_forms_local_to_check %>% 
      mutate(local_fn = basename(local_fn)) %>% 
      select(filename, uuid, err = local_fn),
    "Upload blobs" = 
      blobs_to_check %>% 
      tibble::enframe("uuid", "err") %>% 
      mutate(
        uuid = stringr::str_extract(uuid, uuid_rx)
      ),
    .id = "step"
  ) %>% 
  filter(!(uuid %in% known_bad_UUIDs))




## Add any known bad UUIDs to vector at top! ----