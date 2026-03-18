rm(list = ls())
options(shiny.autoload.r = FALSE)

####Libraries####
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(shinyWidgets)
library(tidyverse)
library(data.table)
library(here)
library(janitor)
library(DT)
library(leaflet)
library(sf)
library(tigris)
library(lubridate)
library(scales)
library(bslib)
library(RColorBrewer)
library(plotly)
library(readr)
library(rmapshaper)


####Read in Data####
supervision_df <-
  "data-raw" %>%
  here("aggregated_ussc_sentencing_data.csv") %>%
  fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
  clean_names()

states_sf <-
  states(cb = TRUE) %>%
  st_transform(4326) %>%
  clean_names() %>%
  rename("state_name" = "name") %>% 
  ms_simplify(keep = 0.05, keep_shapes = TRUE)

judicial_sf <-
  "data" %>%
  here("judicial_sf_data.rds") %>%
  read_rds() %>% 
  ms_simplify(keep = 0.05, keep_shapes = TRUE)

po_office_df <-
  "data-raw" %>%
  here("po_office_locations.csv") %>%
  fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
  clean_names() %>%
  rename("po_office" = "po_office_name")

available_years_ussc <- sort(unique(supervision_df$fiscal_year), decreasing = TRUE)


####Helper Functions####
clean_outcome_label <- function(x) {
  x %>%
    str_replace_all("_flag", "_count") %>%
    str_replace_all("probation_rate", "probation_sentence_rate") %>%
    str_replace_all("supervised_release_rate", "supervised_release_rate") %>%
    str_replace_all("_", " ") %>%
    str_to_title()
}

clean_geo_label <- function(x) {
  x %>%
    str_replace_all("state_name", "State") %>%
    str_replace_all("state_district", "Judicial District") %>%
    str_replace_all("po_office", "PO Office")
}

is_rate_outcome <- function(outcome) {
  outcome %in% c("probation_rate", "supervised_release_rate")
}


####Theme####
my_theme <- bs_theme(
  version = 4,
  bg = "#FDF6E3",
  fg = "#2C2C2C",
  primary = "#009E73",
  secondary = "#FFBF00",
  warning = "#FFBF00",
  info = "#4A90E2",
  success = "#2CA02C",
  danger = "#D55E00",
  base_font = font_google("Roboto")
)

####UI####
ui <- navbarPage(
  "USSC Federal Sentencing Dashboard",
  theme = my_theme,
  
  header = 
    tags$head(tags$style(HTML("
      .navbar {
        position: fixed !important;
        top: 0 !important; left: 0 !important; right: 0 !important;
        z-index: 1030 !important;
        margin-bottom: 0 !important;
      }
      body { padding-top: 60px !important; }
  
      #dashboard_main .col-sm-2,
      #dashboard_state .col-sm-2,
      #dashboard_national .col-sm-2 {
        position: fixed !important;
        top: 60px !important; left: 0 !important;
        width: 16.66667% !important;
        height: calc(100vh - 60px) !important;
        overflow-y: auto !important;
        z-index: 1000 !important;
        background-color: #FDF6E3 !important;
        padding-right: 15px !important;
        padding-left: 15px !important;
      }
      #dashboard_main .col-sm-10,
      #dashboard_state .col-sm-10,
      #dashboard_national .col-sm-10 {
        margin-left: 16.66667% !important;
        width: 83.33333% !important;
        padding-top: 20px !important;
        padding-left: 30px !important;
      }
      
      /* --- CHART & BOX HELPERS --- */
      
      .box {
        width: 100% !important;
        overflow: visible !important;
      }
      
      .plotly {
        margin-left: 0 !important;
      }
      
      .reporting-note {
        font-size: 11px; color: #888;
        font-style: italic; margin-top: 6px; line-height: 1.4;
      }
      .year-mode-toggle .radio label {
        font-size: 11px;
      }
      .year-mode-toggle label.control-label {
        font-size: 13px;
        font-weight: bold;
      }
      .js-plotly-plot, .plotly, .plot-container {
        width: 100% !important;
      }
      
      /* More aggressive box padding removal */
      #dashboard_main .col-sm-5 .box {
        padding-left: 0 !important;
      }
      
      #dashboard_main .col-sm-5 .box .box-body {
        padding-left: 0 !important;
        padding-right: 0 !important;
      }
      
      .box-body {
        padding: 0 !important;
      }
  "))),
  
  # ---- Main Dashboard ----
  tabPanel("Main Dashboard",
           div(id = "dashboard_main",
               sidebarLayout(
                 sidebarPanel(
                   width = 2,
                   selectInput("outcome", "Outcome Metric",
                               choices = c("Probationers"             = "probation_sentence_flag",
                                           "Supervised Release"        = "supervised_release_flag",
                                           "Total Sentenced"           = "total_sentenced",
                                           "Probation Rate"            = "probation_rate",
                                           "Supervised Release Rate"   = "supervised_release_rate")),
                   selectInput("geo_level", "Geography Level",
                               choices = c("State"    = "state_name",
                                           "District" = "state_district",
                                           "PO Office"= "po_office")),
                   sliderInput("age", "Age Range",
                               min = min(supervision_df$age, na.rm = TRUE),
                               max = max(supervision_df$age, na.rm = TRUE),
                               value = range(supervision_df$age, na.rm = TRUE)),
                   # sliderInput("fiscal_year", "Fiscal Year",
                   #             min = min(supervision_df$fiscal_year, na.rm = TRUE),
                   #             max = max(supervision_df$fiscal_year, na.rm = TRUE),
                   #             value = range(supervision_df$fiscal_year, na.rm = TRUE),
                   #             sep = "", step = 1),
                   div(class = "year-mode-toggle",
                       radioButtons("year_mode_main", "Year Selection Mode",
                                    choices = c("Year Range" = "range", "Single Year" = "single"),
                                    selected = "range", inline = FALSE)),
                   uiOutput("year_selector_main"),
                   selectInput("race", "Race",
                               choices = c("All", sort(unique(supervision_df$newrace_description))),
                               multiple = TRUE, selected = "All"),
                   selectInput("educ", "Education",
                               choices = c("All", sort(unique(supervision_df$neweduc_description))),
                               multiple = TRUE, selected = "All"),
                   selectInput("gender", "Gender",
                               choices = c("All", sort(unique(supervision_df$monsex_description))),
                               multiple = TRUE, selected = "All"),
                   selectInput("offense", "Offense Category",
                               choices = c("All", sort(unique(supervision_df$offguide_description))),
                               multiple = TRUE, selected = "All"),
                   selectInput("sentence", "Sentence Type",
                               choices = c("All", sort(unique(supervision_df$sentence_type_description))),
                               multiple = TRUE, selected = "All"),
                   downloadButton("download_table", "Download Table"),
                   div(class = "reporting-note", "Note: Rates shown as share of total sentenced.")
                 ),
                 mainPanel(
                   width = 10,
                   fluidRow(
                     box(width = 7, leafletOutput("map", height = 500)),
                     box(width = 5,
                         style = "padding: 0; margin: 0; overflow: visible;",
                         div(style = "width:100%; height:500px; padding:0; margin:0; transform: translateX(-20px);",
                             plotlyOutput("barplot", height = "100%", width = "100%")))
                   ),
                   fluidRow(box(width = 12, DTOutput("table")))
                 )
               )
           )
  ),
  
  # ---- State Dashboard ----
  tabPanel("State Dashboard",
           div(id = "dashboard_state",
               sidebarLayout(
                 sidebarPanel(
                   width = 2,
                   selectInput("state_sel", "State",
                               choices = sort(unique(na.omit(supervision_df$state_name))),
                               selected = "Alabama"),
                   selectInput("state_outcome", "Outcome Metric",
                               choices = c("Probationers"             = "probation_sentence_flag",
                                           "Supervised Release"        = "supervised_release_flag",
                                           "Supervised Release Rate" = "supervised_release_rate",
                                           "Probation Rate"          = "probation_rate",
                                           "Total Sentenced"         = "total_sentenced"),
                               selected = "supervised_release_rate"),
                   div(class = "year-mode-toggle",
                       radioButtons("year_mode_state", "Year Selection Mode",
                                    choices = c("Year Range" = "range", "Single Year" = "single"),
                                    selected = "range", inline = FALSE)),
                   uiOutput("year_selector_state"),
                   div(class = "reporting-note", "Trends compare selected state to national average.")
                 ),
                 mainPanel(
                   width = 10,
                   fluidRow(
                     box(width = 6,
                         title = "Demographics: Race",
                         div(style = "width:100%; padding:0; margin:0;",
                             plotlyOutput("state_demo_race", height = 450, width = "100%"))),
                     box(width = 6,
                         title = "Demographics: Gender",
                         div(style = "width:100%; padding:0; margin:0;",
                             plotlyOutput("state_demo_gender", height = 450, width = "100%")))
                   ),
                   fluidRow(
                     box(width = 12,
                         title = "Offense Category Analysis: State vs. National",
                         div(style = "width:100%; padding:0; margin:0;",
                             plotlyOutput("state_offense_comparison", height = 550, width = "100%")))
                   ),
                   conditionalPanel(
                     condition = "input.year_mode_state == 'range'",
                     fluidRow(
                       box(width = 12,
                           div(style = "width:100%; padding:0; margin:0;",
                               plotlyOutput("state_vs_national", height = 550, width = "100%")))
                     )
                   )
                 )
               )
           )
  ),
  
  # ---- National Dashboard ----
  tabPanel("National Dashboard",
           div(id = "dashboard_national",
               sidebarLayout(
                 sidebarPanel(
                   width = 2,
                   sliderInput("nat_year", "Fiscal Year",
                               min = min(supervision_df$fiscal_year, na.rm = TRUE),
                               max = max(supervision_df$fiscal_year, na.rm = TRUE),
                               value = range(supervision_df$fiscal_year, na.rm = TRUE),
                               sep = "", step = 1),
                   div(class = "reporting-note",
                       "National trends across all districts and states.")
                 ),
                 mainPanel(
                   width = 10,
                   fluidRow(
                     box(width = 12,
                         div(style = "width:100%; padding:0; margin:0;",
                             plotlyOutput("nat_trends", height = 550, width = "100%")))
                   ),
                   fluidRow(
                     box(width = 12,
                         div(style = "width:100%; padding:0; margin:0;",
                             plotlyOutput("nat_offense_bar", height = 550, width = "100%")))
                   )
                 )
               )
           )
  ),
  
  # ---- Custom Visualization ----
  tabPanel("Custom Visualization",
           sidebarLayout(
             sidebarPanel(
               width = 2,
               selectInput("custom_chart_type", "Chart Type",
                           choices = c("Bar Chart"                      = "bar",
                                       "Line Chart"                     = "line",
                                       "Scatterplot (needs 2 outcomes)" = "scatter",
                                       "Boxplot"                        = "boxplot")),
               selectInput("custom_outcome", "Outcome Metric(s)",
                           choices = c("Probationers"           = "probation_sentence_flag",
                                       "Supervised Release"      = "supervised_release_flag",
                                       "Total Sentenced"         = "total_sentenced",
                                       "Probation Rate"          = "probation_rate",
                                       "Supervised Release Rate" = "supervised_release_rate"),
                           selected = "probation_sentence_flag", multiple = TRUE),
               selectInput("custom_group", "Group By",
                           choices = c("State"            = "state_name",
                                       "District"         = "state_district",
                                       "PO Office"        = "po_office",
                                       "Race"             = "newrace_description",
                                       "Education"        = "neweduc_description",
                                       "Gender"           = "monsex_description",
                                       "Offense Category" = "offguide_description",
                                       "Fiscal Year"      = "fiscal_year"),
                           selected = "state_name"),
               selectInput("custom_facet", "Facet By (max 2)",
                           choices = c("State"            = "state_name",
                                       "District"         = "state_district",
                                       "PO Office"        = "po_office",
                                       "Race"             = "newrace_description",
                                       "Education"        = "neweduc_description",
                                       "Gender"           = "monsex_description",
                                       "Offense Category" = "offguide_description",
                                       "Fiscal Year"      = "fiscal_year"),
                           multiple = TRUE, selectize = TRUE),
               sliderInput("custom_age", "Age Range",
                           min = min(supervision_df$age, na.rm = TRUE),
                           max = max(supervision_df$age, na.rm = TRUE),
                           value = range(supervision_df$age, na.rm = TRUE)),
               sliderInput("custom_year", "Fiscal Year",
                           min = min(supervision_df$fiscal_year, na.rm = TRUE),
                           max = max(supervision_df$fiscal_year, na.rm = TRUE),
                           value = range(supervision_df$fiscal_year, na.rm = TRUE), sep = ""),
               selectInput("custom_state", "State",
                           choices = c("All", sort(unique(supervision_df$state_name))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_race", "Race",
                           choices = c("All", sort(unique(supervision_df$newrace_description))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_educ", "Education",
                           choices = c("All", sort(unique(supervision_df$neweduc_description))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_gender", "Gender",
                           choices = c("All", sort(unique(supervision_df$monsex_description))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_offense", "Offense Category",
                           choices = c("All", sort(unique(supervision_df$offguide_description))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_sentence", "Sentence Type",
                           choices = c("All", sort(unique(supervision_df$sentence_type_description))),
                           multiple = TRUE, selected = "All"),
               actionButton("generate_plot", "Generate Plot",
                            class = "btn-success", style = "width:100%; margin-top:8px;"),
               downloadButton("download_custom_plot", "Download Plot"),
               downloadButton("download_custom_data", "Download Data")
             ),
             mainPanel(
               width = 10,
               fluidRow(box(width = 12, plotOutput("custom_plot", height = 700))),
               fluidRow(box(width = 12, DTOutput("custom_table")))
             )
           ),
           tags$script(HTML("
      $(document).on('change', '#custom_facet', function() {
        var selected = $(this).val();
        if (selected && selected.length > 2) {
          $(this).val(selected.slice(0,2)).trigger('change');
          alert('You can only select up to 2 facets.');
        }
      });
    "))
  )
)

####Server####
server <- function(input, output, session) {
  
  # ---- Shared filter helper ----
  apply_filters <- function(df, age_inp, year_inp, race_inp, educ_inp,
                            gender_inp, offense_inp, sentence_inp) {
    df %>%
      filter(
        between(age, age_inp[1], age_inp[2]),
        between(fiscal_year, year_inp[1], year_inp[2]),
        ("All" %in% race_inp    | newrace_description    %in% race_inp),
        ("All" %in% educ_inp    | neweduc_description    %in% educ_inp),
        ("All" %in% gender_inp  | monsex_description     %in% gender_inp),
        ("All" %in% offense_inp | offguide_description   %in% offense_inp),
        ("All" %in% sentence_inp| sentence_type_description %in% sentence_inp)
      )
  }
  
  # ---- Shared aggregation helper ----
  compute_agg <- function(df, geo_col, outcome) {
    if (outcome == "probation_rate") {
      df %>%
        group_by(.data[[geo_col]]) %>%
        summarise(value = sum(probation_sentence_flag, na.rm = TRUE) /
                    sum(total_sentenced, na.rm = TRUE), .groups = "drop")
    } else if (outcome == "supervised_release_rate") {
      df %>%
        group_by(.data[[geo_col]]) %>%
        summarise(value = sum(supervised_release_flag, na.rm = TRUE) /
                    sum(total_sentenced, na.rm = TRUE), .groups = "drop")
    } else {
      df %>%
        group_by(.data[[geo_col]]) %>%
        summarise(value = sum(.data[[outcome]], na.rm = TRUE), .groups = "drop")
    }
  }
  
  # ---- Main Dashboard reactives ----
  filtered <- reactive({
    yr <- main_year_range()
    apply_filters(supervision_df, input$age, yr,
                  input$race, input$educ, input$gender,
                  input$offense, input$sentence)
  })
  
  agg_data <- reactive({
    compute_agg(filtered(), input$geo_level, input$outcome)
  })
  
  # ---- Shared map builder ----
  build_map <- function(map_data, geo_col, geo_label_col, value_col,
                        is_rate, outcome_label, point_mode = FALSE,
                        lng_col = NULL, lat_col = NULL) {
    map_data <- map_data %>%
      filter(!is.na(.data[[geo_col]])) %>%
      mutate(
        .value = replace_na(as.numeric(.data[[value_col]]), 0),
        .label = if (is_rate) {
          paste0(.data[[geo_label_col]], ": ",
                 scales::percent(.value, accuracy = 0.01))
        } else {
          paste0(.data[[geo_label_col]], ": ", scales::comma(.value))
        }
      )
    
    pal <- colorNumeric(palette = "YlGn", domain = map_data$.value,
                        na.color = "transparent")
    
    legend_fmt <- if (is_rate) {
      labelFormat(transform = function(x) x * 100, suffix = "%")
    } else {
      labelFormat()
    }
    
    m <- map_data %>%
      leaflet() %>%
      addProviderTiles("CartoDB.Positron")
    
    if (point_mode) {
      m <- m %>%
        addCircleMarkers(
          lng = ~long, lat = ~lat,
          radius = ~scales::rescale(.value, to = c(4, 12),
                                    from = range(.value, na.rm = TRUE)),
          fillColor = ~pal(.value), color = "black", weight = 1,
          fillOpacity = 0.8, label = ~.label,
          labelOptions = labelOptions(
            style = list("font-weight" = "normal", padding = "3px 8px"),
            textsize = "13px", direction = "auto")
        )
    } else {
      m <- m %>%
        addPolygons(
          fillColor = ~pal(.value), color = "white", weight = 1,
          fillOpacity = 0.7, label = ~.label,
          highlightOptions = highlightOptions(
            weight = 3, color = "#E69F00", fillOpacity = 0.9, bringToFront = TRUE)
        )
    }
    
    m %>%
      addLegend("bottomright", pal = pal, values = ~.value,
                labFormat = legend_fmt, title = outcome_label, opacity = 0.8) %>%
      setView(lng = -98.5795, lat = 39.8283, zoom = 4)
  }
  
  # Main dashboard year UI
  output$year_selector_main <- renderUI({
    if (input$year_mode_main == "single") {
      selectInput("single_year_main", "Fiscal Year",
                  choices = available_years_ussc,
                  selected = max(available_years_ussc))
    } else {
      sliderInput("fiscal_year", "Fiscal Year Range",
                  min   = min(supervision_df$fiscal_year, na.rm = TRUE),
                  max   = max(supervision_df$fiscal_year, na.rm = TRUE),
                  value = range(supervision_df$fiscal_year, na.rm = TRUE),
                  sep = "", step = 1)
    }
  })
  
  # State dashboard year UI
  output$year_selector_state <- renderUI({
    if (input$year_mode_state == "single") {
      selectInput("single_year_state", "Fiscal Year",
                  choices = available_years_ussc,
                  selected = max(available_years_ussc))
    } else {
      sliderInput("state_year", "Fiscal Year Range",
                  min   = min(supervision_df$fiscal_year, na.rm = TRUE),
                  max   = max(supervision_df$fiscal_year, na.rm = TRUE),
                  value = range(supervision_df$fiscal_year, na.rm = TRUE),
                  sep = "", step = 1)
    }
  })
  
  # Year range helpers
  main_year_range <- reactive({
    if (input$year_mode_main == "single") {
      req(input$single_year_main)
      rep(as.numeric(input$single_year_main), 2)
    } else {
      yr <- input$fiscal_year
      if (is.null(yr)) range(supervision_df$fiscal_year, na.rm = TRUE) else yr
    }
  })
  
  state_year_range <- reactive({
    if (input$year_mode_state == "single") {
      req(input$single_year_state)
      rep(as.numeric(input$single_year_state), 2)
    } else {
      yr <- input$state_year
      if (is.null(yr)) range(supervision_df$fiscal_year, na.rm = TRUE) else yr
    }
  })
  
  output$map <- renderLeaflet({
    df  <- agg_data()
    lbl <- clean_outcome_label(input$outcome)
    geo <- input$geo_level
    is_rate <- is_rate_outcome(input$outcome)
    
    if (geo == "state_name") {
      map_data <- states_sf %>% left_join(df, by = "state_name")
      build_map(map_data, "state_name", "state_name", "value", is_rate, lbl)
      
    } else if (geo == "state_district") {
      map_data <- judicial_sf %>% left_join(df, by = "state_district")
      build_map(map_data, "state_district", "state_district", "value", is_rate, lbl)
      
    } else if (geo == "po_office") {
      map_data <- po_office_df %>% left_join(df, by = "po_office")
      build_map(map_data, "po_office", "po_office", "value", is_rate, lbl,
                point_mode = TRUE, lng_col = "long", lat_col = "lat")
    }
  })
  
  # ---- Shared barplot builder ----
  build_barplot <- function(df, geo_col, outcome, y_size = 8) {
    is_rate <- is_rate_outcome(outcome)
    x_label <- clean_outcome_label(outcome)
    y_label <- clean_geo_label(geo_col)
    pal     <- colorRampPalette(brewer.pal(7, "YlGn"))(100)
    n_rows  <- nrow(df)
    height  <- max(400, n_rows * 14)   # ~14px per bar, minimum 400
    
    df <- df %>%
      mutate(across(all_of(geo_col), ~replace_na(.x, "Missing/Not Applicable")),
             value = as.numeric(value),
             hover_text = if (is_rate) {
               paste0(.data[[geo_col]], ": ", scales::percent(value, accuracy = 0.01))
             } else {
               paste0(.data[[geo_col]], ": ", scales::comma(value))
             })
    
    p <- df %>%
      mutate(geo_reorder = reorder(.data[[geo_col]], value)) %>%
      ggplot(aes(x = value, y = geo_reorder, fill = value, text = hover_text)) +
      geom_col() +
      scale_fill_gradientn(
        colors = pal, 
        name = "",
        labels = if (is_rate) percent_format(accuracy = 0.1) else comma_format()
      ) +
      labs(x = x_label, y = y_label) +
      theme_minimal() +
      theme(
        axis.text.y  = element_text(size = y_size, margin = margin(r = 0)),
        plot.margin  = margin(t = 5, r = 5, b = 5, l = 5),
        axis.title.y = element_text(margin = margin(r = 10)),
        axis.title.x = element_text(margin = margin(t = 10)),
        legend.title = element_blank()
      )
    
    if (is_rate) p <- p + scale_x_continuous(labels = percent_format(accuracy = 0.1))
    else         p <- p + scale_x_continuous(labels = comma_format())
    
    plotly_obj <- p %>%
      ggplotly(tooltip = "text") %>%
      layout(
        height     = height,
        margin     = list(l = 50, r = 10, t = 30, b = 50),
        autosize   = TRUE,
        showlegend = FALSE
      )
    
    # Only hide labels for PO offices (too many), show them for state_district
    if (geo_col == "po_office") {
      plotly_obj <- plotly_obj %>%
        layout(yaxis = list(showticklabels = FALSE, showline = TRUE, showgrid = FALSE))
    }
    
    plotly_obj
  }
  
  output$barplot <- renderPlotly({
    
    is_rate <- is_rate_outcome(input$outcome)
    x_lab   <- clean_outcome_label(input$outcome)
    
    df <- agg_data() %>%
      filter(!is.na(.data[[input$geo_level]])) %>%
      mutate(across(all_of(input$geo_level), ~replace_na(.x, "Missing/Not Applicable")),
             value = as.numeric(value))
    
    # Scale font size down as number of bars increases
    n_bars <- nrow(df)
    y_size <- case_when(
      n_bars > 80 ~ 4,
      n_bars > 50 ~ 5,
      n_bars > 20 ~ 6,
      TRUE        ~ 8
    )
    
    ggplot_barplot_main <-
      df %>%
      mutate(geo_reorder = reorder(.data[[input$geo_level]], value)) %>%
      ggplot(aes(
        x    = value,
        y    = geo_reorder,
        fill = value,
        text = if (is_rate)
          paste0(.data[[input$geo_level]], ": ", scales::percent(value, accuracy = 0.01))
        else
          paste0(.data[[input$geo_level]], ": ", scales::comma(value))
      )) +
      geom_col() +
      scale_fill_distiller(
        palette = "YlGn", 
        direction = 1, 
        name = NULL,
        labels = if (is_rate) percent_format(accuracy = 0.1) else comma_format()  # Conditional formatting
      ) +
      labs(x = x_lab, y = clean_geo_label(input$geo_level)) +
      theme_minimal() +
      theme(
        axis.text.y  = element_text(size = y_size, margin = margin(r = 0)),
        plot.margin  = margin(t = 0, r = 0, b = 0, l = 0),
        axis.title.y = element_text(margin = margin(r = 5)),
        axis.title.x = element_text(margin = margin(t = 5))
      )
    
    if (is_rate)
      ggplot_barplot_main <- ggplot_barplot_main +
      scale_x_continuous(labels = percent_format(accuracy = 0.1))
    else
      ggplot_barplot_main <- ggplot_barplot_main +
      scale_x_continuous(labels = comma_format())
    
    barplot_main <- 
      ggplot_barplot_main %>%
      ggplotly(tooltip = "text") %>%
      layout(
        margin     = list(l = 0, r = 0, t = 20, b = 40),
        showlegend = FALSE,
        autosize   = TRUE,
        xaxis = list(fixedrange = FALSE),
        yaxis = list(fixedrange = FALSE)
      ) %>%
      config(responsive = TRUE, displayModeBar = FALSE)
    
    barplot_main
  })
  
  output$table <- renderDT({
    filtered() %>%
      group_by(.data[[input$geo_level]]) %>%
      summarise(
        across(c(probation_sentence_flag, supervised_release_flag, total_sentenced),
               ~sum(.x, na.rm = TRUE)),
        .groups = "drop"
      ) %>%
      mutate(
        supervised_release_rate = supervised_release_flag / total_sentenced,
        probation_rate          = probation_sentence_flag / total_sentenced,
        across(ends_with("rate"), ~round(.x, 3)),
        across(all_of(input$geo_level), ~replace_na(.x, "Missing/Not Applicable"))
      ) %>%
      arrange(desc(.data[[input$outcome]])) %>%
      rename_with(~clean_outcome_label(.x) %>%
                    str_replace_all("Po Office", "PO Office") %>%
                    str_replace_all("State Name", "State") %>%
                    str_replace_all("State District", "District")) %>%
      datatable(
        extensions = "Buttons",
        class = "compact stripe hover",
        filter = "top",
        options = list(
          dom = "Bfrtip", buttons = c("copy", "csv", "excel"),
          paging = FALSE, scrollY = "400px", scrollX = TRUE
        ),
        style = "bootstrap", rownames = FALSE
      )
  })
  
  output$download_table <- downloadHandler(
    filename = function() paste0("USSC_table_", Sys.Date(), ".csv"),
    content  = function(file) {
      df <- filtered() %>%
        group_by(.data[[input$geo_level]]) %>%
        summarise(across(c(probation_sentence_flag, supervised_release_flag, total_sentenced),
                         ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
        mutate(supervised_release_rate = supervised_release_flag / total_sentenced,
               probation_rate          = probation_sentence_flag / total_sentenced,
               across(ends_with("rate"), ~round(.x, 3)))
      write_csv(df, file)
    }
  )
  
  # ---- State Dashboard ----
  state_data <- reactive({
    yr <- state_year_range()
    supervision_df %>%
      filter(state_name == input$state_sel,
             between(fiscal_year, yr[1], yr[2]))
  })
  
  output$state_district_bar <- renderPlotly({
    df <- state_data() %>%
      group_by(state_district) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),  # Add flag columns
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),  # Add flag columns
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      rename(value = all_of(input$state_outcome))
    
    build_barplot(df, "state_district", input$state_outcome, y_size = 7)
  })
  
  # ---- State Dashboard Demographic Charts ----
  # ---- State Dashboard Demographic and Offense Charts ----
  output$state_demo_race <- renderPlotly({
    # Get state data by race
    state_race <- state_data() %>%
      group_by(newrace_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(newrace_description = 
               newrace_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>%
      # filter(total_sentenced >= 10) %>%
      mutate(group = input$state_sel)
    
    # Get national data by race for comparison
    yr <- state_year_range()
    national_race <- supervision_df %>%
      filter(between(fiscal_year, yr[1], yr[2])) %>%
      group_by(newrace_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(newrace_description = 
               newrace_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>%
      mutate(group = "National Average")
    
    # Combine and extract selected outcome
    df <- bind_rows(state_race, national_race) %>%
      mutate(
        value = .data[[input$state_outcome]],
        race_reorder = reorder(newrace_description, value),
        hover_text = paste0(newrace_description, "\n",
                            group, "\n",
                            clean_outcome_label(input$state_outcome), ": ",
                            if (is_rate_outcome(input$state_outcome))
                              scales::percent(value, accuracy = 0.01)
                            else
                              scales::comma(value))
      )
    
    is_rate <- is_rate_outcome(input$state_outcome)
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      ggplot(df, aes(x = value, y = race_reorder, fill = group, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = if(is_rate) percent_format(accuracy = 0.1) else comma_format()) +
        labs(x = y_label, y = "Race/Ethnicity", fill = "") +
        theme_minimal() +
        theme(
          axis.text.y     = element_text(size = 10),
          legend.position = "bottom",
          legend.title    = element_blank(),
          axis.title.x    = element_text(margin = margin(t = 10)),
          axis.title.y    = element_text(margin = margin(r = 10)),
          plot.margin     = margin(t = 5, r = 5, b = 5, l = 0)
        )
    } %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(orientation = "h", x = 0.5, xanchor = "center",
                      y = -0.15, yanchor = "top", title = list(text = "")),
        margin = list(l = 0, r = 10, t = 20, b = 60)
      )
  })
  
  output$state_demo_gender <- renderPlotly({
    # Get state data by gender
    state_gender <- state_data() %>%
      group_by(monsex_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(monsex_description = 
               monsex_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>%
      mutate(group = input$state_sel)
    
    # Get national data by gender for comparison
    yr <- state_year_range()
    national_gender <- supervision_df %>%
      filter(between(fiscal_year, yr[1], yr[2])) %>%
      group_by(monsex_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(monsex_description = 
               monsex_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>%
      mutate(group = "National Average")
    
    # Combine and extract selected outcome
    df <- bind_rows(state_gender, national_gender) %>%
      mutate(
        value = .data[[input$state_outcome]],
        gender_reorder = reorder(monsex_description, value),
        hover_text = paste0(monsex_description, "\n",
                            group, "\n",
                            clean_outcome_label(input$state_outcome), ": ",
                            if (is_rate_outcome(input$state_outcome))
                              scales::percent(value, accuracy = 0.01)
                            else
                              scales::comma(value))
      )
    
    is_rate <- is_rate_outcome(input$state_outcome)
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      ggplot(df, aes(x = value, y = gender_reorder, fill = group, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = if(is_rate) percent_format(accuracy = 0.1) else comma_format()) +
        labs(x = y_label, y = "Gender", fill = "") +
        theme_minimal() +
        theme(
          axis.text.y     = element_text(size = 11),
          legend.position = "bottom",
          legend.title    = element_blank(),
          axis.title.x    = element_text(margin = margin(t = 10)),
          axis.title.y    = element_text(margin = margin(r = 10)),
          plot.margin     = margin(t = 5, r = 5, b = 5, l = 0)
        )
    } %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(orientation = "h", x = 0.5, xanchor = "center",
                      y = -0.15, yanchor = "top", title = list(text = "")),
        margin = list(l = 0, r = 10, t = 20, b = 60)
      )
  })
  
  output$state_offense_comparison <- renderPlotly({
    # Get state data by offense
    state_offense <- state_data() %>%
      group_by(offguide_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(!is.na(offguide_description), total_sentenced >= 10) %>%
      mutate(group = input$state_sel)
    
    # Get national data by offense for comparison
    yr <- state_year_range()
    national_offense <- supervision_df %>%
      filter(between(fiscal_year, yr[1], yr[2])) %>%
      group_by(offguide_description) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(!is.na(offguide_description)) %>%
      mutate(group = "National Average")
    
    # Combine and extract selected outcome
    df <- bind_rows(state_offense, national_offense) %>%
      mutate(
        value = .data[[input$state_outcome]],
        offense_reorder = reorder(offguide_description, value),
        hover_text = paste0(offguide_description, "\n",
                            group, "\n",
                            clean_outcome_label(input$state_outcome), ": ",
                            if (is_rate_outcome(input$state_outcome))
                              scales::percent(value, accuracy = 0.01)
                            else
                              scales::comma(value))
      )
    
    is_rate <- is_rate_outcome(input$state_outcome)
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      ggplot(df, aes(x = value, y = offense_reorder, fill = group, text = hover_text)) +
        geom_col(position = position_dodge(width = 0.7), width = 0.6) +  # Added width parameters for more spacing
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = if(is_rate) percent_format(accuracy = 0.1) else comma_format()) +
        labs(x = y_label, y = "Offense Category", fill = "") +
        theme_minimal() +
        theme(
          axis.text.y     = element_text(size = 9),
          legend.position = "bottom",
          legend.title    = element_blank(),
          axis.title.x    = element_text(margin = margin(t = 10)),
          axis.title.y    = element_text(margin = margin(r = 10)),
          plot.margin     = margin(t = 5, r = 5, b = 5, l = 0)
        )
    } %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(orientation = "h", x = 0.5, xanchor = "center",
                      y = -0.15, yanchor = "top", title = list(text = "")),
        margin = list(l = 0, r = 10, t = 20, b = 60)
      )
  })
  
  output$state_vs_national <- renderPlotly({
    nat_trends <- supervision_df %>%
      filter(between(fiscal_year, input$state_year[1], input$state_year[2])) %>%
      group_by(fiscal_year) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(group = "National Average")
    
    state_trends <- state_data() %>%
      group_by(fiscal_year) %>%
      summarise(
        probation_sentence_flag = sum(probation_sentence_flag, na.rm = TRUE),
        supervised_release_flag = sum(supervised_release_flag, na.rm = TRUE),
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(group = input$state_sel)
    
    df <- bind_rows(state_trends, nat_trends) %>%
      mutate(
        plot_val   = .data[[input$state_outcome]],
        hover_text = paste0(
          "Group: ", group, "\n",
          "Year: ", fiscal_year, "\n",
          "Value: ", if (is_rate_outcome(input$state_outcome))
            scales::percent(plot_val, accuracy = 0.01)
          else
            scales::comma(plot_val)
        )
      )
    
    y_label <- clean_outcome_label(input$state_outcome)
    is_rate <- is_rate_outcome(input$state_outcome)
    
    p <- df %>%
      ggplot(aes(x = fiscal_year, y = plot_val, color = group,
                 group = group, text = hover_text)) +
      geom_line(linewidth = 1) +
      geom_point(size = 2) +
      scale_color_manual(values = c("#0072B2", "#E69F00"), name = "") +  # Empty name
      labs(x = "Fiscal Year", y = y_label,
           title = paste0(input$state_sel, ": ", y_label, " vs. National Average")) +
      theme_minimal() +
      theme(strip.text   = element_text(size = 12, face = "bold"),
            legend.position = "bottom",
            legend.title = element_blank(),  # Remove legend title
            axis.title.x = element_text(margin = margin(t = 15)),
            axis.title.y = element_text(margin = margin(r = 20)),
            plot.title   = element_text(size = 13, face = "bold", hjust = 0.5))
    
    if (is_rate) p <- p + scale_y_continuous(labels = percent_format(accuracy = 0.1))
    else         p <- p + scale_y_continuous(labels = comma_format())
    
    p %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(
          orientation = "h", 
          x = 0.5, 
          xanchor = "center",
          y = -0.15, 
          yanchor = "top",
          title = list(text = "")  # Explicitly remove legend title
        ),
        margin = list(l = 80, r = 50, t = 50, b = 55),
        yaxis = list(
          tickformat = if(is_rate) ".1%" else ","
        )
      )
  })
  
  # ---- National Dashboard ----
  nat_filtered <- reactive({
    supervision_df %>%
      filter(between(fiscal_year, input$nat_year[1], input$nat_year[2]))
  })
  
  output$nat_trends <- renderPlotly({
    df <- nat_filtered() %>%
      group_by(fiscal_year) %>%
      summarise(
        `Probation Rate`          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        `Supervised Release Rate` = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      pivot_longer(cols = -fiscal_year, names_to = "Outcome", values_to = "value") %>%
      mutate(hover_text = paste0("Year: ", fiscal_year, "\n",
                                 "Outcome: ", Outcome, "\n",
                                 "Rate: ", scales::percent(value, accuracy = 0.01)))
    
    {
      ggplot(df, aes(x = fiscal_year, y = value, color = Outcome,
                     group = Outcome, text = hover_text)) +
        geom_line(linewidth = 1) +
        geom_point(size = 2) +
        scale_color_manual(values = c("#0072B2", "#E69F00")) +
        scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
        labs(x = "Fiscal Year", y = "Rate", color = "Outcome",
             title = "National Sentencing Rates Over Time") +
        theme_minimal() +
        theme(legend.position = "bottom",
              axis.title.x    = element_text(margin = margin(t = 15)),
              axis.title.y    = element_text(margin = margin(r = 20)),
              plot.title      = element_text(size = 13, face = "bold", hjust = 0.5))
      } %>%
      ggplotly(tooltip = "text") %>%
      layout(legend = list(orientation = "h", x = 0.5, xanchor = "center",
                           y = -0.15, yanchor = "top"),
             margin = list(l = 80, r = 50, t = 50, b = 55))
  })
  
  output$nat_offense_bar <- renderPlotly({
    df <- nat_filtered() %>%
      group_by(offguide_description) %>%
      summarise(
        probation_rate          = sum(probation_sentence_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        supervised_release_rate = sum(supervised_release_flag, na.rm = TRUE) / sum(total_sentenced, na.rm = TRUE),
        total_sentenced         = sum(total_sentenced, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(!is.na(offguide_description)) %>%
      mutate(
        # Calculate max or sum for ordering
        max_rate = pmax(probation_rate, supervised_release_rate)  # or use: total_rate = probation_rate + supervised_release_rate
      ) %>%
      pivot_longer(cols = c(probation_rate, supervised_release_rate),
                   names_to = "Outcome", values_to = "value") %>%
      mutate(
        Outcome     = clean_outcome_label(Outcome),
        geo_reorder = reorder(offguide_description, max_rate),  # Order by max_rate
        hover_text  = paste0(offguide_description, "\n",
                             Outcome, ": ", scales::percent(value, accuracy = 0.01))
      )
    
    {
      ggplot(df, aes(x = value, y = geo_reorder, fill = Outcome, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00")) +
        scale_x_continuous(labels = percent_format(accuracy = 0.1)) +
        labs(x = "Rate", y = "Offense Category",
             fill  = "Outcome",
             title = "National Sentencing Rates by Offense Category") +
        theme_minimal() +
        theme(
          axis.text.y     = element_text(size = 8),
          legend.position = "bottom",
          axis.title.x    = element_text(margin = margin(t = 15)),
          axis.title.y    = element_text(margin = margin(r = 15)),
          plot.title      = element_text(size = 13, face = "bold", hjust = 0.5)
        )
      } %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(orientation = "h", x = 0.5, xanchor = "center",
                      y = -0.15, yanchor = "top"),
        margin = list(l = 80, r = 50, t = 50, b = 55)
      )
  })
  
  # ---- Custom Visualization ----
  custom_filtered <- reactive({
    apply_filters(supervision_df,
                  input$custom_age, input$custom_year,
                  input$custom_race, input$custom_educ, input$custom_gender,
                  input$custom_offense, input$custom_sentence) %>%
      filter("All" %in% input$custom_state | state_name %in% input$custom_state)
  })
  
  custom_agg <- eventReactive(input$generate_plot, {
    req(input$custom_outcome, input$custom_group)
    group_cols <- c(input$custom_group, input$custom_facet) %>%
      discard(is.null) %>% discard(~.x == "")
    
    custom_filtered() %>%
      group_by(across(all_of(group_cols))) %>%
      summarise(
        across(c(ends_with("_flag"), total_sentenced), ~sum(.x, na.rm = TRUE)),
        .groups = "drop"
      ) %>%
      mutate(
        probation_rate          = probation_sentence_flag / total_sentenced,
        supervised_release_rate = supervised_release_flag / total_sentenced
      ) %>%
      dplyr::select(all_of(c(group_cols, input$custom_outcome)))
  })
  
  output$custom_plot <- renderPlot({
    if (is.null(input$generate_plot) || input$generate_plot == 0) {
      plot.new()
      text(0.5, 0.5, "Click 'Generate Plot' to display.", cex = 1.5, col = "darkgreen")
      return()
    }
    
    df <- custom_agg() %>%
      mutate(across(where(is.character), ~replace_na(.x, "Missing/Not Applicable")))
    
    if (!all(input$custom_outcome %in% names(df)) || nrow(df) == 0) {
      plot.new()
      text(0.5, 0.5, "No data. Adjust filters and click 'Generate Plot'.", cex = 1.5)
      return()
    }
    
    x_lab  <- clean_geo_label(input$custom_group) %>% clean_outcome_label()
    y_labs <- paste(clean_outcome_label(input$custom_outcome), collapse = " / ")
    
    df_long <- df %>%
      pivot_longer(cols = all_of(input$custom_outcome),
                   names_to = "Outcome", values_to = "Value") %>%
      mutate(Outcome = clean_outcome_label(Outcome))
    
    p <- if (input$custom_chart_type == "bar") {
      df_long %>%
        ggplot(aes(x = reorder(.data[[input$custom_group]], -Value),
                   y = Value, fill = Outcome)) +
        geom_col(position = "dodge") +
        coord_flip() +
        scale_fill_brewer(palette = "Set2") +
        labs(x = x_lab, y = y_labs, fill = "Outcome") +
        theme_minimal()
      
    } else if (input$custom_chart_type == "line") {
      df_long %>%
        ggplot(aes(x = .data[[input$custom_group]], y = Value,
                   color = Outcome, group = Outcome)) +
        geom_line(linewidth = 1.2) + geom_point() +
        scale_color_brewer(palette = "Set2") +
        labs(x = x_lab, y = y_labs, color = "Outcome") +
        theme_minimal()
      
    } else if (input$custom_chart_type == "scatter") {
      req(length(input$custom_outcome) == 2)
      df %>%
        ggplot(aes(x = .data[[input$custom_outcome[1]]],
                   y = .data[[input$custom_outcome[2]]],
                   label = .data[[input$custom_group]])) +
        geom_point(size = 3, alpha = 0.7, color = "#009E73") +
        geom_text(vjust = -0.5, size = 3) +
        labs(x = clean_outcome_label(input$custom_outcome[1]),
             y = clean_outcome_label(input$custom_outcome[2])) +
        theme_minimal()
      
    } else if (input$custom_chart_type == "boxplot") {
      df_long %>%
        ggplot(aes(x = .data[[input$custom_group]], y = Value, fill = Outcome)) +
        geom_boxplot() + coord_flip() +
        scale_fill_brewer(palette = "Set2") +
        labs(x = x_lab, y = y_labs, fill = "Outcome") +
        theme_minimal()
    }
    
    if (length(input$custom_facet) == 1)
      p <- p + facet_wrap(vars(.data[[input$custom_facet[1]]]))
    else if (length(input$custom_facet) == 2)
      p <- p + facet_grid(rows = vars(.data[[input$custom_facet[1]]]),
                          cols = vars(.data[[input$custom_facet[2]]]))
    p
  })
  
  output$custom_table <- renderDT({
    if (is.null(input$generate_plot) || input$generate_plot == 0)
      return(datatable(data.frame(Message = "Click 'Generate Plot' to view table")))
    
    df <- custom_agg()
    if (!all(input$custom_outcome %in% names(df)) || nrow(df) == 0)
      return(datatable(data.frame(Message = "No data available.")))
    
    df %>%
      rename_with(~clean_outcome_label(.x) %>%
                    clean_geo_label() %>%
                    str_replace_all("Newrace Description", "Race") %>%
                    str_replace_all("Neweduc Description", "Education") %>%
                    str_replace_all("Monsex Description",  "Gender") %>%
                    str_replace_all("Offguide Description","Offense Category")) %>%
      datatable(extensions = "Buttons", class = "compact stripe hover",
                filter = "top",
                options = list(paging = FALSE, scrollY = "400px", scrollX = TRUE),
                style = "bootstrap", rownames = FALSE)
  })
  
  output$download_custom_data <- downloadHandler(
    filename = function() paste0("custom_USSC_table_", Sys.Date(), ".csv"),
    content  = function(file) write_csv(custom_agg(), file)
  )
  
  output$download_custom_plot <- downloadHandler(
    filename = function() paste0("custom_plot_", Sys.Date(), ".png"),
    content  = function(file) {
      # Re-render the plot for saving — mirrors renderPlot logic above
      ggsave(file, width = 10, height = 7)
    }
  )
}

####Run App####
shinyApp(ui, server)