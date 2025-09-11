rm(list = ls())
options(shiny.autoload.r = FALSE)


####Read in Libraries####
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(tidyverse)
library(data.table)
library(here)
library(janitor)
library(DT)
library(leaflet)
library(sf)
library(tigris)
library(DBI)
library(dbplyr)
library(RPostgres)
library(lubridate)
library(scales)
library(bslib)
library(RColorBrewer)
library(plotly)
library(readr)


####Read in Data####
##Supervision Data
supervision_df <-
  "data-raw" %>% 
  here("aggregated_ussc_sentencing_data.csv") %>% 
  fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
  clean_names()

##State Geometries
states_sf <- 
  states(cb = TRUE) %>%
  st_transform(4326) %>% 
  clean_names() %>% 
  rename("state_name" = "name")

##Read in Judicial SF
judicial_sf <- 
  "data" %>% 
  here("judicial_sf_data.rds") %>% 
  read_rds()

##PO Office Data
po_office_df <- 
  "data-raw" %>% 
  here("po_office_locations.csv") %>% 
  fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
  clean_names() %>% 
  rename("po_office" = "po_office_name")


####App####
##Define Theme
my_theme <- 
  bs_theme(
    version = 4,           
    bg = "#2C2C2C",        
    fg = "#F5F5F5",        
    primary = "#4A90E2",   
    secondary = "#7FB3D5"
    # , 
    # base_font = font_google("Roboto")
  )

##UI
ui <- navbarPage("USSC Federal Sentencing Dashboard",
                 theme = my_theme,
                 # ---- Main Dashboard Tab ----
                 tabPanel("Main Dashboard",
                          sidebarLayout(
                            sidebarPanel(
                              width = 2,
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
                                          min = min(supervision_df$age, na.rm = TRUE), 
                                          max = max(supervision_df$age, na.rm = TRUE), 
                                          value = range(supervision_df$age, na.rm = TRUE)),
                              sliderInput("fiscal_year", "Fiscal Year of Sentencing",
                                          min = min(supervision_df$fiscal_year, na.rm = TRUE),
                                          max = max(supervision_df$fiscal_year, na.rm = TRUE), 
                                          value = range(supervision_df$fiscal_year, na.rm = TRUE), 
                                          sep = "", 
                                          step = 2),
                              selectInput("race", "Race", 
                                          choices = c("All", sort(unique(supervision_df$newrace_description))), 
                                          multiple = TRUE),
                              selectInput("educ", "Education", 
                                          choices = c("All", sort(unique(supervision_df$neweduc_description))), 
                                          multiple = TRUE),
                              selectInput("gender", "Gender", 
                                          choices = c("All", sort(unique(supervision_df$monsex_description))), 
                                          multiple = TRUE),
                              selectInput("offense", "Offense Category", 
                                          choices = c("All", sort(unique(supervision_df$offguide_description))), 
                                          multiple = TRUE),
                              selectInput("sentence", "Sentence Type", 
                                          choices = c("All", sort(unique(supervision_df$sentence_type_description))), 
                                          multiple = TRUE),
                              downloadButton("download_table", "Download Table")
                            ),
                            mainPanel(
                              width = 10,
                              fluidRow(
                                box(width = 8, leafletOutput("map", height = 500)),
                                box(width = 4, plotlyOutput("barplot", height = 500))
                              ),
                              fluidRow(
                                box(width = 12, DTOutput("table"))
                              )
                            )
                          )
                 ),
                 
                 # ---- Custom Visualization Tab ----
                 tabPanel("Custom Visualization",
                          sidebarLayout(
                            sidebarPanel(
                              width = 2,
                              selectInput("custom_chart_type", "Chart Type",
                                          choices = c("Bar Chart" = "bar",
                                                      "Line Chart" = "line",
                                                      "Scatterplot (needs 2 outcomes)" = "scatter",
                                                      "Boxplot" = "boxplot")),
                              selectInput("custom_outcome", "Outcome Metric(s)",
                                          choices = c("Probationers" = "probation_sentence_flag",
                                                      "Supervised Release" = "supervised_release_flag",
                                                      "Total Sentenced" = "total_sentenced",
                                                      "Probation Rate" = "probation_rate",
                                                      "Supervised Release Rate" = "supervised_release_rate"),
                                          selected = "probation_sentence_flag",
                                          multiple = TRUE),
                              selectInput("custom_group", "Group By",
                                          choices = c("State" = "state_name", 
                                                      "District" = "state_district", 
                                                      "PO Office" = "po_office",
                                                      "Race" = "newrace_description",
                                                      "Education" = "neweduc_description",
                                                      "Gender" = "monsex_description",
                                                      "Offense Category" = "offguide_description",
                                                      "Fiscal Year" = "fiscal_year"),
                                          selected = "state_name"),
                              selectInput("custom_facet", "Facet By (max 2)",
                                          choices = c("State" = "state_name", 
                                                      "District" = "state_district", 
                                                      "PO Office" = "po_office",
                                                      "Race" = "newrace_description",
                                                      "Education" = "neweduc_description",
                                                      "Gender" = "monsex_description",
                                                      "Offense Category" = "offguide_description",
                                                      "Fiscal Year" = "fiscal_year"),
                                          multiple = TRUE,
                                          selectize = TRUE),
                              
                              # full filtering system again
                              sliderInput("custom_age", "Age Range", 
                                          min = min(supervision_df$age, na.rm = TRUE), 
                                          max = max(supervision_df$age, na.rm = TRUE), 
                                          value = range(supervision_df$age, na.rm = TRUE)),
                              sliderInput("custom_year", "Fiscal Year of Sentencing",
                                          min = min(supervision_df$fiscal_year, na.rm = TRUE),
                                          max = max(supervision_df$fiscal_year, na.rm = TRUE), 
                                          value = range(supervision_df$fiscal_year, na.rm = TRUE), sep = ""),
                              selectInput("custom_state", "State", 
                                          choices = c("All", sort(unique(supervision_df$state_name))), 
                                          multiple = TRUE),
                              selectInput("custom_race", "Race", 
                                          choices = c("All", sort(unique(supervision_df$newrace_description))), 
                                          multiple = TRUE),
                              selectInput("custom_educ", "Education", 
                                          choices = c("All", sort(unique(supervision_df$neweduc_description))), 
                                          multiple = TRUE),
                              selectInput("custom_gender", "Gender", 
                                          choices = c("All", sort(unique(supervision_df$monsex_description))), 
                                          multiple = TRUE),
                              selectInput("custom_offense", "Offense Category", 
                                          choices = c("All", sort(unique(supervision_df$offguide_description))), 
                                          multiple = TRUE),
                              selectInput("custom_sentence", "Sentence Type", 
                                          choices = c("All", sort(unique(supervision_df$sentence_type_description))), 
                                          multiple = TRUE),
                              
                              actionButton("generate_plot", "Generate Plot"),
                              downloadButton("download_custom_plot", "Download Plot"),
                              downloadButton("download_custom_data", "Download Data")
                            ),
                            mainPanel(
                              width = 10,
                              fluidRow(
                                box(width = 12, plotOutput("custom_plot", height = 700)),
                              ),
                              fluidRow(
                                box(width = 12, DTOutput("custom_table"))
                              )
                            )
                          ),
                          
                          tags$script(HTML("
                                            $(document).on('change', '#custom_facet', function() {
                                             var selected = $(this).val();
                                             if (selected && selected.length > 2) {
                                               selected = selected.slice(0,2);  // keep only first 2
                                               $(this).val(selected);
                                               $(this).trigger('change');
                                               alert('You can only select up to 2 facets.');
                                             }
                                           });
                                         "))
                 )
)

##Server
server <- function(input, output, session){
  
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
      
      pal <- colorBin(
        palette = "BuGn",        
        domain = map_data$value, 
        bins = 7,                 
        na.color = "gray90"
      )
      
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
              color = "#E69F00",       
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
              color = "#E69F00",       
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
      
      pal <- colorBin(
        palette = "BuGn",        # Blue-Green sequential palette
        domain = map_data$value, 
        bins = 7,                 # adjust as needed
        na.color = "gray90"
      )
      
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
              color = "#E69F00",       
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
              color = "#E69F00",       
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
    }else if(input$geo_level == "po_office"){
      map_data <- 
        po_office_df %>%
        left_join(df_map, by = "po_office")
      
      pal <- colorBin(
        palette = "BuGn",        # Blue-Green sequential palette
        domain = map_data$value, 
        bins = 7,                 # adjust as needed
        na.color = "gray90"
      )
      
      if(input$outcome %in% c("probation_rate", "supervised_release_rate")){
        map_data %>% 
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addCircleMarkers(
            lng = ~long, lat = ~lat,
            radius = ~scales::rescale(value, to = c(4, 12), from = range(value, na.rm = TRUE)), 
            fillColor = ~pal(value),
            color = "black", weight = 1,
            fillOpacity = 0.8,
            label = ~paste0(po_office, ": ", scales::percent_format(accuracy = 0.01)(value)),
            labelOptions = labelOptions(
              style = list("font-weight" = "normal", padding = "3px 8px"),
              textsize = "13px",
              direction = "auto"
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
          setView(lng = -98.5795, lat = 39.8283, zoom = 4) %>% 
          htmlwidgets::onRender("
            function(el, x) {
              var map = this;
              map.eachLayer(function(layer) {
                if (layer.options && layer.options.radius) {
                  // save the original radius if not already stored
                  if (!layer.options.baseRadius) {
                    layer.options.baseRadius = layer.options.radius;
                  }
                  layer.on('mouseover', function(e) {
                    this.setStyle({weight: 3, color: '#E69F00'});
                    this.setRadius(this.options.baseRadius * 1.5);
                  });
                  layer.on('mouseout', function(e) {
                    this.setStyle({weight: 1, color: 'black'});
                    this.setRadius(this.options.baseRadius);
                  });
                }
              });
            }
          ")
      }else{
        map_data %>%
          filter(is.na(.data[[input$geo_level]]) == FALSE) %>% 
          mutate(value = 
                   value %>% 
                   replace_na(0) %>% 
                   as.numeric()) %>% 
          leaflet() %>%
          addProviderTiles("CartoDB.Positron") %>%
          addCircleMarkers(
            lng = ~long, lat = ~lat,
            radius = ~scales::rescale(value, to = c(4, 12), from = range(value, na.rm = TRUE)), 
            fillColor = ~pal(value),
            color = "black", weight = 1,
            fillOpacity = 0.8,
            label = ~paste0(po_office, ": ", scales::comma_format()(value)),
            labelOptions = labelOptions(
              style = list("font-weight" = "normal", padding = "3px 8px"),
              textsize = "13px",
              direction = "auto"
            )
          ) %>%
          addLegend("bottomright",
                    pal = pal,
                    values = ~value,
                    title = input$outcome %>%
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("_", " ") %>%
                      str_replace_all("probation_rate", "probation_sentence_rate") %>%
                      str_to_title(),
                    opacity = 0.7) %>%
          setView(lng = -98.5795, lat = 39.8283, zoom = 4) %>%
          htmlwidgets::onRender("
            function(el, x) {
              var map = this;
              map.eachLayer(function(layer) {
                if (layer.options && layer.options.radius) {
                  // save the original radius if not already stored
                  if (!layer.options.baseRadius) {
                    layer.options.baseRadius = layer.options.radius;
                  }
                  layer.on('mouseover', function(e) {
                    this.setStyle({weight: 3, color: '#E69F00'});
                    this.setRadius(this.options.baseRadius * 1.5);
                  });
                  layer.on('mouseout', function(e) {
                    this.setStyle({weight: 1, color: 'black'});
                    this.setRadius(this.options.baseRadius);
                  });
                }
              });
            }
          ")
      }
    }
    
  })
  
  output$barplot <- renderPlotly({
    if(input$outcome %in% c("probation_rate", "supervised_release_rate")){
      df <- agg_data() %>%
        mutate(across(all_of(input$geo_level), ~replace_na(.x, "Missing/Not Applicable")),
               value = as.numeric(value))  # ensure numeric for coloring
      
      # Create a 100-step color palette like your ggplot
      pal <- colorRampPalette(brewer.pal(7, "BuGn"))(100)
      
      # Map values to color indices
      df$color_index <- cut(df$value, breaks = 100, labels = FALSE)
      df$fill_color <- pal[df$color_index]
      
      barplot_main <- 
        plot_ly(
          data = df,
          x = ~value,
          y = ~reorder(.data[[input$geo_level]], -value),  # flip like coord_flip
          type = "bar",
          orientation = "h",
          # text = ~paste0(.data[[input$geo_level]], ": ", scales::percent(value, accuracy = 0.1)),
          # hoverinfo = "text",
          text = NULL,  # prevents the label from showing on the bar
          hoverinfo = "text",
          hovertext = ~paste0(.data[[input$geo_level]], ": ", scales::comma(value)),
          marker = list(color = ~fill_color)
        ) %>%
        layout(
          title = paste(
            input$outcome %>% 
              str_replace_all("_flag", "_count") %>%
              str_replace_all("_", " ") %>% 
              str_replace_all("probation_rate", "probation_sentence_rate") %>% 
              str_to_title(),
            "by",
            input$geo_level %>% 
              str_replace_all("state_name", "State") %>% 
              str_replace_all("state_district", "Judicial District") %>% 
              str_replace_all("po_office", "PO Office")
          ),
          xaxis = list(
            title = input$outcome %>% 
              str_replace_all("_flag", "_count") %>%
              str_replace_all("_", " ") %>% 
              str_replace_all("probation_rate", "probation_sentence_rate") %>% 
              str_to_title(),
            tickformat = ".1%"  # percent labels like ggplot
          ),
          yaxis = list(
            title = input$geo_level %>% 
              str_replace_all("state_name", "State") %>% 
              str_replace_all("state_district", "Judicial District") %>% 
              str_replace_all("po_office", "PO Office"),
            tickfont = list(size = 8),
            dtick = 1,
            side = "left", 
            automargin = TRUE
          ),
          yaxis2 = list(
            overlaying = "y",
            side = "right",
            showticklabels = FALSE
          ),
          hovermode = "closest"
        )
        
      
      
      # barplot_main <- 
      #   agg_data() %>%
      #   mutate(across(input$geo_level, 
      #                 ~.x %>% 
      #                   replace_na("Missing/Not Applicable"))) %>%
      #   ggplot(aes(x = reorder(.data[[input$geo_level]], -value), y = value)) +
      #   geom_col(aes(fill = value)) +
      #   scale_fill_gradientn(
      #     colors = colorRampPalette(brewer.pal(7, "BuGn"))(100),  # 100-step smooth gradient
      #     na.value = "gray90"
      #   ) +
      #   coord_flip() +
      #   labs(x = 
      #          input$geo_level %>% 
      #          str_replace_all("state_name", "State") %>% 
      #          str_replace_all("state_district", "Judicial District") %>% 
      #          str_replace_all("po_office", "PO Office"), 
      #        y = 
      #          input$outcome %>% 
      #          str_replace_all("_flag", "_count") %>%
      #          str_replace_all("_", " ") %>% 
      #          str_replace_all("probation_rate", "probation_sentence_rate") %>% 
      #          str_to_title(), 
      #        title = paste(input$outcome %>% 
      #                        str_replace_all("_flag", "_count") %>%
      #                        str_replace_all("_", " ") %>% 
      #                        str_replace_all("probation_rate", "probation_sentence_rate") %>% 
      #                        str_to_title(), 
      #                      "by", 
      #                      input$geo_level %>% 
      #                        str_replace_all("state_name", "State") %>% 
      #                        str_replace_all("state_district", "Judicial District") %>% 
      #                        str_replace_all("po_office", "PO Office"))) +
      #   theme_minimal() +
      #   theme(legend.position = "none") + 
      #   scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
    }else{
      df <- agg_data() %>%
        mutate(across(all_of(input$geo_level), ~replace_na(.x, "Missing/Not Applicable")),
               value = as.numeric(value))  # ensure numeric for coloring
      
      # Create a 100-step color palette like your ggplot
      pal <- colorRampPalette(brewer.pal(7, "BuGn"))(100)
      
      # Map values to color indices
      df$color_index <- cut(df$value, breaks = 100, labels = FALSE)
      df$fill_color <- pal[df$color_index]
      
      barplot_main <-
        plot_ly(
          data = df,
          x = ~value,
          y = ~reorder(.data[[input$geo_level]], -value),  # flip like coord_flip
          type = "bar",
          orientation = "h",
          # text = ~paste0(.data[[input$geo_level]], ": ", scales::comma(value)),
          # hoverinfo = "text",
          text = NULL,  # prevents the label from showing on the bar
          hoverinfo = "text",
          hovertext = ~paste0(.data[[input$geo_level]], ": ", scales::comma(value)),
          marker = list(color = ~fill_color)
        ) %>%
        layout(
          title = paste(
            input$outcome %>% 
              str_replace_all("_flag", "_count") %>%
              str_replace_all("_", " ") %>% 
              str_replace_all("probation_rate", "probation_sentence_rate") %>% 
              str_to_title(),
            "by",
            input$geo_level %>% 
              str_replace_all("state_name", "State") %>% 
              str_replace_all("state_district", "Judicial District") %>% 
              str_replace_all("po_office", "PO Office")
          ),
          xaxis = list(
            title = input$outcome %>% 
              str_replace_all("_flag", "_count") %>%
              str_replace_all("_", " ") %>% 
              str_replace_all("probation_rate", "probation_sentence_rate") %>% 
              str_to_title(),
            tickformat = ","
          ),
          yaxis = list(
            title = input$geo_level %>% 
              str_replace_all("state_name", "State") %>% 
              str_replace_all("state_district", "Judicial District") %>% 
              str_replace_all("po_office", "PO Office"),
            tickfont = list(size = 8),
            dtick = 1,
            side = "left",
            automargin = TRUE
          ),
          yaxis2 = list(
            overlaying = "y",
            side = "right",
            showticklabels = FALSE
          ),
          hovermode = "closest",
          showlegend = FALSE
        )
      
      # 
      # barplot_main <- 
      #   agg_data() %>%
      #   mutate(across(input$geo_level, 
      #                 ~.x %>% 
      #                   replace_na("Missing/Not Applicable"))) %>%
      #   ggplot(aes(x = reorder(.data[[input$geo_level]], -value), y = value)) +
      #   geom_col(aes(fill = value)) +
      #   scale_fill_gradientn(
      #     colors = colorRampPalette(brewer.pal(7, "BuGn"))(100),  # 100-step smooth gradient
      #     na.value = "gray90"
      #   ) +
      #   coord_flip() +
      #   labs(x = 
      #          input$geo_level %>% 
      #          str_replace_all("state_name", "State") %>% 
      #          str_replace_all("state_district", "Judicial District") %>% 
      #          str_replace_all("po_office", "PO Office"), 
      #        y = 
      #          input$outcome %>% 
      #          str_replace_all("_flag", "_count") %>%
      #          str_replace_all("_", " ") %>% 
      #          str_replace_all("probation_rate", "probation_sentence_rate") %>% 
      #          str_to_title(), 
      #        title = paste(input$outcome %>% 
      #                        str_replace_all("_flag", "_count") %>%
      #                        str_replace_all("_", " ") %>% 
      #                        str_replace_all("probation_rate", "probation_sentence_rate") %>% 
      #                        str_to_title(), 
      #                      "by", 
      #                      input$geo_level %>% 
      #                        str_replace_all("state_name", "State") %>% 
      #                        str_replace_all("state_district", "Judicial District") %>% 
      #                        str_replace_all("po_office", "PO Office"))) +
      #   theme_minimal() +
      #   theme(legend.position = "none") + 
      #   scale_y_continuous(labels = scales::comma_format())
    }
    
    if(input$geo_level %in% c("po_office", "state_district")){
      barplot_main <- 
        barplot_main %>%
        layout(
          yaxis = list(
            showticklabels = FALSE,   # hide y-axis labels
            showline = TRUE,
            showgrid = FALSE
          )
        )
    }
    
    barplot_main
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
                    str_to_title() %>% 
                    str_replace_all("Po Office", "PO Office") %>% 
                    str_replace_all("State Name", "State") %>% 
                    str_replace_all("State District", "District")) %>%
      datatable(
        extensions = 'Buttons', 
        class = 'compact stripe hover',
        filter = "top",
        width = "100%",
        options = list(
          paging = FALSE,
          scrollY = "400px",
          scrollX = TRUE
        ),
        style = "bootstrap",
        rownames = FALSE
      ) 
  })
  
  output$download_table <- downloadHandler(
    filename = function(){
      paste0("USSC_table_", Sys.Date(), ".csv")
    },
    content = function(file){
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
                      str_to_title())
        
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

  custom_filtered <- reactive({
    supervision_df %>%
      filter(
        between(age, input$custom_age[1], input$custom_age[2]),
        between(fiscal_year, input$custom_year[1], input$custom_year[2]),
        (is.null(input$custom_state) | state_name %in% input$custom_state),
        (is.null(input$custom_race) | newrace_description %in% input$custom_race),
        (is.null(input$custom_educ) | neweduc_description %in% input$custom_educ),
        (is.null(input$custom_gender) | monsex_description %in% input$custom_gender),
        (is.null(input$custom_offense) | offguide_description %in% input$custom_offense),
        (is.null(input$custom_sentence) | sentence_type_description %in% input$custom_sentence)
      )
  })
  
  # Aggregated data for plotting
  custom_agg <- eventReactive(input$generate_plot, {
    req(input$custom_outcome, input$custom_group)
    df <- custom_filtered()
    
    group_cols <- c(input$custom_group, input$custom_facet)
    group_cols <- group_cols[!is.null(group_cols) & group_cols != ""]
    
    df %>% 
      group_by(across(all_of(group_cols))) %>% 
      dplyr::summarize(across(c(ends_with("_flag", ), "total_sentenced"), 
                              ~.x %>% 
                                sum(na.rm = TRUE)),
                       supervised_release_rate = 
                         supervised_release_flag/total_sentenced,
                       probation_rate = 
                         probation_sentence_flag/total_sentenced) %>% 
      ungroup() %>% 
      dplyr::select(all_of(c(group_cols, input$custom_outcome)))
  })
  
  # Plotting logic
  output$custom_plot <- renderPlot({
    
    if(is.null(input$generate_plot) || input$generate_plot == 0){
      plot.new()
      text(0.5, 0.5, "Click 'Generate Plot' to display the visualization.", 
           cex = 1.5, col = "darkgreen")
      return()
    }
    
    df <- 
      custom_agg() %>% 
      mutate(across(where(is_character), 
                    ~.x %>% 
                      replace_na("Missing/Not Applicable")))
    
    if(!all(input$custom_outcome %in% names(df)) || nrow(df) == 0){
      plot.new()
      text(0.5, 0.5, "No data to display. Adjust filters and click 'Generate Plot'.", cex = 1.5)
      return()
    }
    
    if(input$custom_chart_type == "bar"){
      p <- 
        df %>%
        pivot_longer(cols = input$custom_outcome, names_to = "Outcome", values_to = "Value") %>%
        mutate(Outcome = 
                 Outcome %>% 
                 str_replace_all("state_name", "State") %>% 
                 str_replace_all("state_district", "Judicial District") %>% 
                 str_replace_all("_flag", "_count") %>%
                 str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                 str_replace_all("_", " ") %>% 
                 str_to_title()) %>%
        ggplot(aes(x = reorder(.data[[input$custom_group]], -Value), y = Value, fill = Outcome)) +
        geom_col(position = "dodge") +
        coord_flip() +
        scale_fill_brewer(palette = "BrBG", direction = 1) +
        theme_minimal() +
        labs(x = 
               input$custom_group %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(), 
             y = input$custom_outcome %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(),
             fill = "Sentencing Outcome(s)")
      
    }else if(input$custom_chart_type == "line"){
      p <- 
        df %>%
        pivot_longer(cols = input$custom_outcome, names_to = "Outcome", values_to = "Value") %>%
        mutate(Outcome = 
                 Outcome %>% 
                 str_replace_all("state_name", "State") %>% 
                 str_replace_all("state_district", "Judicial District") %>% 
                 str_replace_all("_flag", "_count") %>%
                 str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                 str_replace_all("_", " ") %>% 
                 str_to_title()) %>%
        ggplot(aes(x = .data[[input$custom_group]], y = Value, color = Outcome, group = Outcome)) +
        geom_line(size = 1.2) + 
        geom_point() +
        scale_color_brewer(palette = "BrBG", direction = 1) +
        theme_minimal() +
        labs(x = 
               input$custom_group %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(), 
             y = input$custom_outcome %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(),
             fill = "Sentencing Outcome(s)")
      
    }else if(input$custom_chart_type == "scatter"){
      req(length(input$custom_outcome) == 2)
      p <- 
        df %>% 
        ggplot(aes(x = .data[[input$custom_outcome[1]]], y = .data[[input$custom_outcome[2]]],
                     label = .data[[input$custom_group]])) +
        geom_point(size = 3, alpha = 0.7, color = "blue") +
        geom_text(vjust = -0.5, size = 3) +
        theme_minimal() +
        labs(x = 
               input$custom_outcome[1] %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(), 
             y = input$custom_outcome[2] %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title())
      
    }else if(input$custom_chart_type == "boxplot"){
      p <- 
        df %>% 
        pivot_longer(cols = input$custom_outcome, names_to = "Outcome", values_to = "Value") %>%
        mutate(Outcome = 
                 Outcome %>% 
                 str_replace_all("state_name", "State") %>% 
                 str_replace_all("state_district", "Judicial District") %>% 
                 str_replace_all("_flag", "_count") %>%
                 str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                 str_replace_all("_", " ") %>% 
                 str_to_title()) %>%
        ggplot(aes(x = .data[[input$custom_group]], y = Value, fill = Outcome)) +
        geom_boxplot() +
        coord_flip() +
        scale_fill_brewer(palette = "BrBG", direction = 1) + 
        theme_minimal() +
        labs(x = 
               input$custom_group %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(), 
             y = input$custom_outcome %>% 
               str_replace_all("state_name", "State") %>% 
               str_replace_all("state_district", "Judicial District") %>% 
               str_replace_all("_flag", "_count") %>%
               str_replace_all("probation_rate", "probation_sentence_rate") %>% 
               str_replace_all("_", " ") %>% 
               str_to_title(),
             fill = "Sentencing Outcome(s)")
    }
    
    if(length(input$custom_facet) == 1){
      p <- p + facet_wrap(vars(.data[[input$custom_facet[1]]]))
    }else if(length(input$custom_facet) == 2){
      p <- p + facet_grid(
        rows = vars(.data[[input$custom_facet[1]]]),
        cols = vars(.data[[input$custom_facet[2]]])
      )
    }
    p
  })
  
  output$custom_table <- renderDT({
    if(is.null(input$generate_plot) || input$generate_plot == 0){
      return(datatable(data.frame(Message = "Click 'Generate Plot' to view table")))
    }
    
    df <- custom_agg()
    
    # Check that all the current outcomes exist in the aggregated data
    if(!all(input$custom_outcome %in% names(df)) || nrow(df) == 0){
      return(datatable(data.frame(Message = "Click 'Generate Plot' to view table")))
    }
    
    df %>%
      arrange(across(all_of(input$custom_outcome), ~desc(.x))) %>%
      rename_with(~.x %>% 
                    str_replace_all("_flag", "_count") %>%
                    str_replace_all("^custom_", "") %>% 
                    str_replace_all("_description$", "") %>% 
                    str_replace_all("offguide", "offense_category") %>% 
                    str_replace_all("^new", "") %>% 
                    str_replace_all("race", "Race/Ethnicity") %>% 
                    str_replace_all("monsex", "sex") %>% 
                    str_replace_all("educ", "educational_status") %>% 
                    str_replace_all("state_name", "State") %>% 
                    str_replace_all("state_district", "Judicial District") %>% 
                    str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                    str_replace_all("_", " ") %>% 
                    str_to_title()) %>%
      datatable(
        extensions = 'Buttons', 
        class = 'compact stripe hover',
        filter = "top",
        width = "100%",
        options = list(
          paging = FALSE,
          scrollY = "400px",
          scrollX = TRUE
        ),
        style = "bootstrap",
        rownames = FALSE
      ) 
  })
  
  # Download plot
  output$download_custom_plot <- downloadHandler(
    filename = function() paste0("custom_plot_", Sys.Date(), ".png"),
    content = function(file){
      ggsave(file, plot = custom_plot(), width = 10, height = 7)
    }
  )
  
  output$download_custom_data <- downloadHandler(
    filename = function(){
      paste0("custom_USSC_table_", Sys.Date(), ".csv")
    },
    content = function(file){
      df <- 
        custom_agg() %>%
        arrange(across(all_of(input$custom_outcome), ~desc(.x))) %>%
        rename_with(~.x %>% 
                      str_replace_all("_flag", "_count") %>%
                      str_replace_all("^custom_", "") %>% 
                      str_replace_all("_description$", "") %>% 
                      str_replace_all("offguide", "offense_category") %>% 
                      str_replace_all("^new", "") %>% 
                      str_replace_all("race", "Race/Ethnicity") %>% 
                      str_replace_all("monsex", "sex") %>% 
                      str_replace_all("educ", "educational_status") %>% 
                      str_replace_all("state_name", "State") %>% 
                      str_replace_all("state_district", "Judicial District") %>% 
                      str_replace_all("probation_rate", "probation_sentence_rate") %>% 
                      str_replace_all("_", " ") %>% 
                      str_to_title()) %>%
        
        # Create a filter summary
        filters <- tibble(
          Filter = c("Age Range", "Fiscal Year", "State", "Race", "Education", "Gender", "Offense Category", "Sentence Type", "Outcome", "Geography"),
          Value = c(
            paste(input$custom_age, collapse = " - "),
            paste(input$custom_year, collapse = " - "),
            paste(input$custom_state, collapse = ", "),
            paste(input$custom_race, collapse = ", "),
            paste(input$custom_educ, collapse = ", "),
            paste(input$custom_gender, collapse = ", "),
            paste(input$custom_offense, collapse = ", "),
            paste(input$custome_sentence, collapse = ", "),
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
}


####Run App####
shinyApp(ui, server)

