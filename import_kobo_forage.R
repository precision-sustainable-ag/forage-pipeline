library(httr)
library(purrr)
library(dplyr)
library(dbplyr)
library(DBI)
library(stringr)
library(glue)

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
  "2777c2fd-30c0-4992-a87f-c39dd8c9061c"
)


# Authenticate: ----
source("secret.R")
# kobo_token, sas_endpoint, sas_token, this_env
# sarah_token <- readr::read_lines("kobo_token.txt")
# sas_token <- 
#   readr::read_lines(
#     "secret.R"
#   ) %>% 
#   purrr::set_names(stringr::word(.)) %>% 
#   purrr::keep(~stringr::str_detect(.x, "^sas|^endpoint")) %>% 
#   bind_rows() %>% 
#   mutate_all(
#     ~stringr::str_extract(., '".+"$') %>% 
#       stringr::str_remove_all('"')
#   )
  

# Fetch: ----
blob_ctr <- 
  AzureStor::blob_container(
    sas_endpoint, 
    sas = sas_token
  ) 

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
forage_submissions_response <- GET(
  url = "https://kf.kobotoolbox.org/api/v2/assets/axnBDev39ubd4UJtqhk2yX/data",
  add_headers("Authorization" = kobo_token)
)



# Function definitions: ----
forage_extract_metadata <- function(elt) {
  proj = elt[["experiment"]] %||% "onfarm"
  scan_date = elt[["today"]] %>% str_remove_all("-")
  uuid = elt[["_uuid"]]
  timing = elt[["time"]] %||% "biomass"

  stop_msg <- jsonlite::toJSON(
    list(code = elt[["code"]], uuid = uuid)
    )
  if (proj == "onfarm") {
    location = str_to_upper(str_trim(elt[["code"]] %||% "")) 
    if (!str_detect(location, "^[A-Z0-9]{3}$")) {
      stop("Malformed location: ", stop_msg, call. = F)
      }
  } else if (proj == "ce1") {
    location = str_to_upper(str_trim(elt[["code"]] %||% elt[["ce1"]] %||% "")) 
    if (!str_detect(location, "^[A-Z]{2}CE1$")) {
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

# raw_forms_metadata <- purrr::map(
#   content(forage_submissions_response)$results,
#   purrr::safely(forage_extract_metadata)
# )




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


# raw_forms_urls <- purrr::map(
#   content(forage_submissions_response)$results,
#   purrr::safely(forage_extract_urls)
# )

forage_extract_data <- function(elt) {
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
      rep = str_extract(field, "[1-4]"),
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

# raw_forms_filenames <- purrr::map(
#   content(forage_submissions_response)$results,
#   purrr::safely(forage_extract_data)
# )

forage_parse <- function(elt, pre_existing = "") {
  
  meta <- forage_extract_metadata(elt)
  if (meta$uuid %in% pre_existing) {
    return(NULL)
  }
  
  
  if (elt[["__version__"]] != "vtPieLBJh95pFM7T6CFQdn") {
    stop(
      "Parsing not defined for version: ",
      jsonlite::toJSON(elt[c("_uuid", "__version__")])
    )
  }
  
  urls <- forage_extract_urls(elt)
  files <- forage_extract_data(elt)
  
  ret <- full_join(
    meta,
    full_join(urls, files, by = "filename"),
    by = character()
  )
  
  if (anyNA(ret)) {
    stop("Missing data in form: ", jsonlite::toJSON(ret))
  }
  
  ret
}




forage_download <- function(dl_url, file_nm) {
  if (!dir.exists("forage_blobs")) {
    stop("There's no blob directory in: ", getwd())
  }
  
  message(file_nm)
  res <- RETRY(
    "GET",
    dl_url,
    add_headers("Authorization" = sarah_token),
    write_disk(file.path("forage_blobs", file_nm), overwrite = T),
    progress()
  )
  
  # check size and status before proceeding
  stop_for_status(res)
  
  local_fn <- normalizePath(file.path("forage_blobs", file_nm))
  res_size <- headers(res)[["content-length"]] %>% 
    as.numeric()
  local_size <- file.size(local_fn)
  
  if (
    local_size == 0 | 
      res_size == 0 | 
      local_size != res_size
  ) {
    stop(
      "Incompatible sizes: response, ", 
      res_size, "; local, ", local_size
      )
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
    content(forage_submissions_response)$results,
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
      "box214v2_{proj}_{location}_{timing}_{species}_rep{rep}_{type}_{scan_date}_{uuid}.{ext}"
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
  group_by(uuid) %>% ### ????
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
      select(fn, uuid, err = local_fn),
    "Upload blobs" = 
      blobs_to_check %>% 
      tibble::enframe("uuid", "err") %>% 
      mutate(
        uuid = stringr::str_extract(uuid, uuid_rx)
      ),
    .id = "step"
  )



content(forage_submissions_response)$results %>% 
  purrr::keep(~.x[["_uuid"]] %in% forms_to_check$uuid)



## Add any known bad UUIDs to vector at top! ----