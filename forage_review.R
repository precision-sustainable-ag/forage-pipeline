library(shiny)
library(shinydashboard)
library(dplyr)


forage_make_sidebar <- function(x, input) {
  purrr::imap(
    x,
    ~{
      f <- ifelse(input[[.x]] == "", strong, em)
      menuItem(
      f(stringr::str_extract(.x, "^.{8}")), 
      tabName = .x, 
      badgeLabel = .y,
      badgeColor = ifelse(input[[.x]] == "", "red", "navy")
      )
    }
  ) 
}

forage_make_tabs <- function(b, a) {

  purrr::pmap(
    b,
    function(step, err, uuid) {
      tabItem(
        tabName = uuid,
        h2("Forage box: ", uuid),

        selectInput(
          uuid, 
          label = "Resolve?", 
          choices = c("", "Keep checking", "This form cannot be saved")
        ),
        tags$button(
          "COPY",
          class = "btn",
          "data-clipboard-target" = paste0("#target_", uuid)
          ),
        div(
          id = paste0("target_", uuid),
          h3(strong("Step:"), step),
          h4(pre(paste0("`", err, "`"), style = "white-space: pre-wrap;")),
          span("```json", style = "opacity: 0;"),
          pre(
            purrr::keep(a, ~.x[["_uuid"]] == uuid)[[1]] %>% 
              jsonlite::toJSON(pretty = T, auto_unbox = T) %>% 
              code(class = 'language-javascript', .noWS = "before"),
            .noWS = "before"
          ),
          span("```", style = "opacity: 0;")
        )
      )
    }
  )
}

forage_review <- function(bad_subs, all_subs) {
  
  bad_subs <- bad_subs %>% 
    group_by(uuid, step) %>% 
    summarise(err = paste(err, collapse = "\n- - - - - -\n"))

  tabs <- forage_make_tabs(bad_subs, all_subs) %>% 
    purrr::lift_dl(tabItems)(.) %>% 
    dashboardBody(
      tags$head(
        tags$script(src = "https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"),
        tags$script(src = "https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.11/clipboard.min.js"),
      ),
      tags$link(rel = "stylesheet", href = "https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.css"),
      tags$script("Prism.highlightAll(); new ClipboardJS('.btn');")
      )

  ui <- dashboardPage(
    dashboardHeader(title = "Review submissions"),
    uiOutput("side"),
    tabs,
    skin = "purple"
  )
  
  server <- function(input, output, session) {
    output$side <- renderUI({
      forage_make_sidebar(bad_subs$uuid, input) %>% 
        tagList() %>% 
        sidebarMenu() %>% 
        dashboardSidebar()
      })
    
    outs <- reactive({
      out = purrr::map_chr(
        purrr::set_names(bad_subs$uuid), 
        ~input[[.x]] 
        ) %>% 
        purrr::discard(~.x == "") %>% 
        tibble::enframe() 
        
      split(as.list(out$name), out$value)
    })
    
    onStop(function() {
      cat(jsonlite::toJSON(isolate(outs()), pretty = T, auto_unbox = T))
    })
  }

  shinyApp(ui = ui, server = server) %>% 
    runApp(launch.browser = .rs.invokeShinyWindowViewer)
}

forage_review(
  forms_to_check %>% filter(!is.na(uuid)),
  forage_submissions
)

# TODO: remove errors from preexisting list, some return before that check