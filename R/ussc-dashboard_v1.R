rm(list = ls())


####Read in Libraries####
library(shiny)
library(shinydashboard)
library(tidyverse)
library(data.table)
library(here)
library(janitor)
library(DT)
library(leaflet)
library(sf)
library(tigris)
library(urbnmapr)
library(DBI)
library(dbplyr)
library(RPostgres)
library(lubridate)
library(scales)


####App####
##UI
ui <- dashboardPage(
  dashboardHeader(title = "USSC Sentencing Outcomes Dashboard"),
  dashboardSidebar(
    selectInput("outcome", "Outcome Metric", 
                choices = c("Probationers" = "probation_sentence_flag",
                            "Supervised Release" = "supervised_release_flag", 
                            "Total Sentenced" = "total_sentenced",
                            "Probation Rate" = "probation_rate", 
                            "Supervised Release Rate" = "supervised_release_rate")),
    selectInput("geo_level", "Geography Level", 
                choices = c("State" = "state_name", 
                            "District" = "state_district", 
                            "PO Office" = "po_office")),
    sliderInput("age", "Age Range", 
                min = min(range(supervision_df$age, na.rm = TRUE)), 
                max = max(range(supervision_df$age, na.rm = TRUE)), 
                value = range(supervision_df$age, na.rm = TRUE)),
    sliderInput("fiscal_year", "Fiscal Year of Sentencing",
                min = min(range(supervision_df$fiscal_year, na.rm = TRUE)),
                max = max(range(supervision_df$fiscal_year, na.rm = TRUE)), 
                value = range(supervision_df$fiscal_year, na.rm = TRUE), 
                sep = ""),
    selectInput("newrace_description", "Race", choices = c("All", as.character(na.omit(unique(supervision_df$newrace_description)))), multiple = TRUE),
    selectInput("neweduc_description", "Education", choices = c("All", as.character(na.omit(unique(supervision_df$neweduc_description)))), multiple = TRUE),
    selectInput("monsex_description", "Gender", choices = c("All", as.character(na.omit(unique(supervision_df$monsex_description)))), multiple = TRUE),
    selectInput("offguide_description", "Offense Category", choices = c("All", as.character(na.omit(unique(supervision_df$offguide_description)))), multiple = TRUE),
    selectInput("sentence_type_description", "Sentence Type", choices = c("All", as.character(na.omit(unique(supervision_df$sentence_type_description)))), multiple = TRUE), 
    downloadButton("download_table", "Download Table")
  ),
  dashboardBody(
    fluidRow(
      box(width = 8, leafletOutput("map", height = 500)),
      box(width = 4, plotOutput("barplot", height = 500))
    ),
    fluidRow(
      box(width = 12, DTOutput("table"))
    )
  )
)

##Server
server <- function(input, output, session){
  
  ####Connect to Database####
  ##Database Connection
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "reform-cjis-rds-cluster.cluster-cl8mgigamxxo.us-east-1.rds.amazonaws.com",
    port = 5432,
    user = "postgres",
    password = "Reform12345!"
  )
  
  on.exit(dbDisconnect(con), add = TRUE)
  
  ##Supervision Data
  supervision_df <- 
    con %>% 
    tbl(in_schema("ussc_federal_data", "sentencing_data_aggregated_dashboard")) %>%
    collect() %>% 
    mutate(across(ends_with("_flag"), 
                  ~.x %>% 
                    replace_na(0))) %>% 
    rename("total_sentenced" = "total_count") %>% 
    mutate(total_sentenced = 
             total_sentenced %>% 
             as.numeric())
  
  ##State Geometries
  states_sf <- 
    states(cb = TRUE) %>%
    st_transform(4326) %>% 
    clean_names() %>% 
    rename("state_name" = "name")
  
  ##Judicial District Geometries
  judicial_sf <- 
    "data-raw" %>% 
    here("shp") %>% 
    here("US_District_Courts.shp") %>% 
    st_read() %>% 
    st_transform(4326) %>% 
    clean_names() %>% 
    rename("state_district" = "name") %>% 
    mutate(state_district = 
             state_district %>% 
             str_replace_all("District Court", "") %>% 
             str_trim(side = "both"))
  
  filtered <- reactive({
    supervision_df %>%
      filter(
        between(age, input$age[1], input$age[2]),
        between(fiscal_year, input$fiscal_year[1], input$fiscal_year[2]),
        (is.null(input$newrace_description) | newrace_description %in% input$newrace_description),
        (is.null(input$neweduc_description) | neweduc_description %in% input$neweduc_description),
        (is.null(input$monsex_description) | monsex_description %in% input$monsex_description),
        (is.null(input$offguide_description) | offguide_description %in% input$offguide_description),
        (is.null(input$sentence_type_description) | sentence_type_description %in% input$sentence_type_description)
      )
  })
  
  # filtered <- reactive({
  #   supervision_df %>%
  #     filter(
  #       between(age, input$age[1], input$age[2]),
  #       between(fiscal_year, input$fiscal_year[1], input$fiscal_year[2]),
  #       if(length(input$newrace_description) > 0) newrace_description %in% input$newrace_description else TRUE,
  #       if(length(input$neweduc_description) > 0) neweduc_description %in% input$neweduc_description else TRUE,
  #       if(length(input$monsex_description) > 0) monsex_description %in% input$monsex_description else TRUE,
  #       if(length(input$offguide_description) > 0) offguide_description %in% input$offguide_description else TRUE,
  #       if(length(input$sentence_type_description) > 0) sentence_type_description %in% input$sentence_type_description else TRUE
  #     )
  # })
  
  # filtered <- reactive({
  #   supervision_df %>%
  #     filter(
  #       between(age, input$age[1], input$age[2]),
  #       between(fiscal_year, input$fiscal_year[1], input$fiscal_year[2]),
  #       ("All" %in% input$newrace_description | newrace_description %in% input$newrace_description),
  #       ("All" %in% input$neweduc_description | neweduc_description %in% input$neweduc_description),
  #       ("All" %in% input$monsex_description | monsex_description %in% input$monsex_description),
  #       ("All" %in% input$offguide_description | offguide_description %in% input$offguide_description),
  #       ("All" %in% input$sentence_type_description | sentence_type_description %in% input$sentence_type_description)
  #     )
  # })
  # 
  
  agg_data <- reactive({
    if(input$outcome == "probation_rate"){
      filtered() %>% 
        group_by(.data[[input$geo_level]]) %>% 
        summarise(value = sum(.data[["probation_sentence_flag"]], na.rm = TRUE) / 
                    sum(.data[["total_sentenced"]], na.rm = TRUE)) %>% 
        ungroup()
      
    }else if(input$outcome == "supervised_release_rate"){
      filtered() %>% 
        group_by(.data[[input$geo_level]]) %>% 
        summarise(value = sum(.data[["supervised_release_flag"]], na.rm = TRUE) / 
                    sum(.data[["total_sentenced"]], na.rm = TRUE)) %>% 
        ungroup()
      
    }else{
      filtered() %>%
        group_by(.data[[input$geo_level]]) %>%
        summarise(value = sum(.data[[input$outcome]], na.rm = TRUE), .groups = "drop") %>% 
        ungroup()
    }
  })
  
  output$map <- renderLeaflet({
    df_map <- agg_data()
    
    if(input$geo_level == "state_name"){
      map_data <- 
        states_sf %>%
        left_join(df_map, by = "state_name")
      
      pal <- colorBin("Reds", domain = map_data$value, bins = 5, na.color = "gray90")
      
      if(input$outcome %in% c("probation_rate", "supervised_release_rate")){
        map_data %>% 
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addPolygons(
            fillColor = ~pal(value),
            color = "white", weight = 1,
            fillOpacity = 0.7,
            label = ~paste0(state_name, ": ", scales::percent_format(accuracy = 0.01)(value)),
            highlightOptions = highlightOptions(
              weight = 3,           
              color = "#00FF00",       
              fillOpacity = 0.9,    
              bringToFront = TRUE   
            )
          ) %>%
          addLegend("bottomright", 
                    pal = pal, 
                    values = ~value,
                    labFormat = labelFormat(
                      transform = function(x) x * 100,
                      suffix = "%"
                    ),
                    title = 
                      input$outcome %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("_", " ") %>% 
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_to_title(), 
                    opacity = 0.7) %>%
          setView(lng = -98.5795, lat = 39.8283, zoom = 4)
      }else{
        map_data %>% 
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addPolygons(
            fillColor = ~pal(value),
            color = "white", weight = 1,
            fillOpacity = 0.7,
            label = ~paste0(state_name, ": ", scales::comma_format()(value)),
            highlightOptions = highlightOptions(
              weight = 3,           
              color = "#00FF00",       
              fillOpacity = 0.9,    
              bringToFront = TRUE   
            )
          ) %>%
          addLegend("bottomright", 
                    pal = pal, 
                    values = ~value,
                    title = 
                      input$outcome %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("_", " ") %>% 
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_to_title(), 
                    opacity = 0.7) %>%
          setView(lng = -98.5795, lat = 39.8283, zoom = 4)
      }
    }else if(input$geo_level == "state_district"){
      map_data <- 
        judicial_sf %>%
        left_join(df_map, by = "state_district")
      
      pal <- colorBin("Reds", domain = map_data$value, bins = 5, na.color = "gray90")
      
      if(input$outcome %in% c("probation_rate", "supervised_release_rate")){
        map_data %>% 
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addPolygons(
            fillColor = ~pal(value),
            color = "white", weight = 1,
            fillOpacity = 0.7,
            label = ~paste0(state_name, ": ", scales::percent_format(accuracy = 0.01)(value)),
            highlightOptions = highlightOptions(
              weight = 3,           
              color = "#00FF00",       
              fillOpacity = 0.9,    
              bringToFront = TRUE   
            )
          ) %>%
          addLegend("bottomright", 
                    pal = pal, 
                    values = ~value,
                    labFormat = labelFormat(
                      transform = function(x) x * 100,
                      suffix = "%"
                    ),
                    title = 
                      input$outcome %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("_", " ") %>% 
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_to_title(), 
                    opacity = 0.7) %>%
          setView(lng = -98.5795, lat = 39.8283, zoom = 4)
      }else{
        map_data %>% 
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addPolygons(
            fillColor = ~pal(value),
            color = "white", weight = 1,
            fillOpacity = 0.7,
            label = ~paste0(state_name, ": ", scales::comma_format()(value)),
            highlightOptions = highlightOptions(
              weight = 3,           
              color = "#00FF00",       
              fillOpacity = 0.9,    
              bringToFront = TRUE   
            )
          ) %>%
          addLegend("bottomright", 
                    pal = pal, 
                    values = ~value,
                    title = 
                      input$outcome %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("_", " ") %>% 
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_to_title(), 
                    opacity = 0.7) %>%
          setView(lng = -98.5795, lat = 39.8283, zoom = 4)
      }
    }
    
  })
  
  output$barplot <- renderPlot({
    if(input$outcome %in% c("probation_rate", "supervised_release_rate")){
      agg_data() %>%
        mutate(across(input$geo_level, 
                      ~.x %>% 
                        replace_na("Missing/Not Applicable"))) %>%
        ggplot(aes(x = reorder(.data[[input$geo_level]], -value), y = value)) +
        geom_col(aes(fill = value)) +
        scale_fill_gradient(low = "pink", high = "red") +
        coord_flip() +
        labs(x = 
               input$geo_level %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District"), 
             y = 
               input$outcome %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("_", " ") %>% 
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_to_title(), 
             title = paste(input$outcome %>% 
                             str_replace_all("_flag", "_count") %>%
                             str_replace_all("_", " ") %>% 
                             str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                             str_to_title(), 
                           "by", 
                           input$geo_level %>% 
                             str_replace_all("state_name", "State") %>% 
                             str_replace_all("state_district", "Judicial District"))) +
        theme_minimal() +
        theme(legend.position = "none") + 
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
    }else{
      agg_data() %>%
        mutate(across(input$geo_level, 
                      ~.x %>% 
                        replace_na("Missing/Not Applicable"))) %>%
        ggplot(aes(x = reorder(.data[[input$geo_level]], -value), y = value)) +
        geom_col(aes(fill = value)) +
        scale_fill_gradient(low = "pink", high = "red") +
        coord_flip() +
        labs(x = 
               input$geo_level %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District"), 
             y = 
               input$outcome %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("_", " ") %>% 
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_to_title(), 
             title = paste(input$outcome %>% 
                             str_replace_all("_flag", "_count") %>%
                             str_replace_all("_", " ") %>% 
                             str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                             str_to_title(), 
                           "by", 
                           input$geo_level %>% 
                             str_replace_all("state_name", "State") %>% 
                             str_replace_all("state_district", "Judicial District"))) +
        theme_minimal() +
        theme(legend.position = "none") + 
        scale_y_continuous(labels = scales::comma_format())
    }
    
  })
  
  output$table <- renderDT({
    filtered() %>% 
      group_by(.data[[input$geo_level]]) %>% 
      summarise(across(c("probation_sentence_flag", "supervised_release_flag", "total_sentenced"), 
                       ~.x %>% 
                         sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced,
             across(ends_with("rate"), 
                    ~.x %>% 
                      round(3)), 
             across(c(input$geo_level), 
                    ~.x %>% 
                      replace_na("Missing/Not Applicable"))) %>%
      arrange(desc(.data[[input$outcome]])) %>% 
      rename_with(~.x %>% 
                    str_replace_all("_flag", "_count") %>%
                    str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                    str_replace_all("_", " ") %>% 
                    str_to_title()) %>%
      datatable(
        extensions = 'Buttons', 
        options = list(
          paging = FALSE,
          scrollY = "400px",
          scrollX = TRUE
          # ,
          # dom = 'Bfrtip',         # show buttons
          # buttons = c('csv', 'excel')  # add CSV and Excel download
        ),
        style = "bootstrap",
        rownames = FALSE
      ) 
  })
  
  output$download_table <- downloadHandler(
    filename = function() {
      paste0("USSC_table_", Sys.Date(), ".csv")
    },
    content = function(file) {
      df <- 
        filtered() %>% 
        group_by(.data[[input$geo_level]]) %>% 
        summarise(across(c("probation_sentence_flag", "supervised_release_flag", "total_sentenced"), 
                         ~.x %>% 
                           sum(na.rm = TRUE))) %>% 
        ungroup() %>% 
        mutate(supervised_release_rate = 
                 supervised_release_flag/total_sentenced,
               probation_rate = 
                 probation_sentence_flag/total_sentenced,
               across(ends_with("rate"), 
                      ~.x %>% 
                        round(3)), 
               across(c(input$geo_level), 
                      ~.x %>% 
                        replace_na("Missing/Not Applicable"))) %>%
        arrange(desc(.data[[input$outcome]])) %>% 
        rename_with(~.x %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_replace_all("_", " ") %>% 
                      str_to_title()) %>%
      
      # Create a filter summary
      filters <- tibble(
        Filter = c("Age Range", "Fiscal Year", "Race", "Education", "Gender", "Offense Category", "Sentence Type", "Outcome", "Geography"),
        Value = c(
          paste(input$age, collapse = " - "),
          paste(input$fiscal_year, collapse = " - "),
          paste(input$newrace_description, collapse = ", "),
          paste(input$neweduc_description, collapse = ", "),
          paste(input$monsex_description, collapse = ", "),
          paste(input$offguide_description, collapse = ", "),
          paste(input$sentence_type_description, collapse = ", "),
          input$outcome %>% 
            str_replace_all("_flag", "_count") %>%
            str_replace_all("_", " ") %>% 
            str_replace_all("probation_rate", "probation_sentence_rate"),
          input$geo_level
        )
      )
      
      # Write filter info first, then data
      write_csv(df, file)
      write_csv(filters, file, append = TRUE)
    }
  )
  
  # observe({
  #   print(agg_data())
  # })
  
  # observe({
  #   filtered() %>% 
  #     filter(state_name == "Alabama") %>% 
  #     group_by(state_name) %>% 
  #     dplyr::summarize(across(c(ends_with("flag"), "total_sentenced"), 
  #                             ~.x %>% 
  #                               sum(na.rm = TRUE)),
  #                      probation_rate = sum(probation_sentence_flag, na.rm = TRUE)/sum(total_sentenced, na.rm = TRUE)) %>% 
  #     ungroup() %>% 
  #     print()
  # })
  # 
  # observe({
  #   supervision_df %>% 
  #     filter(state_name == "Alabama") %>% 
  #     group_by(state_name) %>% 
  #     dplyr::summarize(across(c(ends_with("flag"), "total_sentenced"), 
  #                             ~.x %>% 
  #                               sum(na.rm = TRUE)),
  #                      probation_rate = sum(probation_sentence_flag, na.rm = TRUE)/sum(total_sentenced, na.rm = TRUE)) %>% 
  #     ungroup() %>% 
  #     print()
  # })
  # 
  # observe({
  #   supervision_df %>% 
  #     filter(state_name == "Alabama") %>% 
  #     dplyr::summarize(across(c(ends_with("flag"), "total_sentenced"), 
  #                             ~.x %>% 
  #                               sum(na.rm = TRUE))) %>%
  #     mutate(probation_rate = probation_sentence_flag/total_sentenced) %>%
  #     print()
  # })
  
}


####Run App####
shinyApp(ui, server)
