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
  here("aggregated-ussc-sentencing-data") %>% 
  list.files(full.names = TRUE) %>%
  str_subset("aggregated_ussc_sentencing_data_") %>%
  map(~.x %>%
        fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
        clean_names()) %>%
  bind_rows() 

geo_crosswalk <- 
  "data-raw" %>%
  here("geo_crosswalk.csv") %>%
  fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
  clean_names() 

states_list <- 
  supervision_df %>% 
  filter(geo_level_type == "state_name") %>% 
  filter(geo_level != "National Total") %>% 
  dplyr::select(geo_level) %>% 
  distinct() %>% 
  arrange(geo_level) %>% 
  pull(geo_level)

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
  clean_names() 

available_years_ussc <- sort(unique(supervision_df$fiscal_year), decreasing = TRUE)


####Helper Functions####
clean_outcome_label <- function(x) {
  x %>%
    str_replace_all("_flag", "_count") %>%
    str_replace_all("probation_rate", "probation_sentence_rate") %>%
    str_replace_all("supervised_release_rate", "supervised_release_rate") %>%
    str_replace_all("_", " ") %>%
    str_to_title() %>% 
    str_replace_all("Total Sentenced Fsr", "Total Sentenced Eligible for FSR")
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

carto_key <- Sys.getenv("CARTO_API_KEY")   # set in .Renviron or your host's env vars

add_basemap <- function(map) {
  if (nzchar(carto_key)) {
    addTiles(
      map,
      urlTemplate = paste0(
        "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png?key=",
        carto_key
      ),
      attribution = paste0(
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors ',
        '&copy; <a href="https://carto.com/attributions">CARTO</a>'
      )
    )
  } else {
    # keyless fallback so the map never shows a watermark
    addProviderTiles(map, "Esri.WorldGrayCanvas")
  }
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

####Overview and About Page Content####
##Formatting Helpers
fmt_int  <- function(x) format(round(x), big.mark = ",", scientific = FALSE, trim = TRUE)
fmt_pct  <- function(x, digits = 1) paste0(formatC(100 * x, format = "f", digits = digits), "%")
fmt_or   <- function(x) sprintf("%.3f", x)
fmt_list <- function(x){
  x <- as.character(x)
  if(length(x) <= 1) return(x)
  if(length(x) == 2) return(paste(x, collapse = " and "))
  paste0(paste(x[-length(x)], collapse = ", "), ", and ", x[length(x)])
}
hex_alpha <- function(hex, alpha){
  rgb <- grDevices::col2rgb(hex)
  sprintf("rgba(%d,%d,%d,%.2f)", rgb[1], rgb[2], rgb[3], alpha)
}
ab_link      <- function(href, label) a(href = href, target = "_blank", rel = "noopener noreferrer", label)
ab_link_html <- function(href, label) paste0('<a href="', href, '" target="_blank" rel="noopener noreferrer">', label, '</a>')

##Links to the analysis reports (PDFs served from www/reports/) and the code
# The PDFs are built by reports/build_reports.py from the files in data/modeling-results/.
ov_report_sr_url <- "reports/supervised-release-report.pdf"
ov_report_pb_url <- "reports/probation-report.pdf"
ov_code_url      <- "https://github.com/REFORM-Alliance/ussc-federal-supervised-release-dashboard"

##Pre-compute Figures from the Dashboard Data (runs once at app start-up)
# These use the same counts and rate definitions as the dashboards, so the numbers
# on the Overview page match what the dashboards show.
ov_count_cols <- c("supervised_release_flag", "probation_sentence_flag", "total_sentenced", "total_sentenced_fsr")

ov_nat <-
  supervision_df %>%
  filter(geo_level_type == "state_name", geo_level == "National Total") %>%
  group_by(fiscal_year) %>%
  dplyr::summarize(across(all_of(ov_count_cols), ~.x %>% sum(na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(fiscal_year) %>%
  mutate(sr_rate = supervised_release_flag / total_sentenced_fsr,
         pb_rate = probation_sentence_flag / total_sentenced)

ov_fy_first  <- min(ov_nat$fiscal_year)
ov_fy_latest <- max(ov_nat$fiscal_year)
ov_first     <- ov_nat %>% filter(fiscal_year == ov_fy_first)
ov_latest    <- ov_nat %>% filter(fiscal_year == ov_fy_latest)
ov_prev      <- ov_nat %>% filter(fiscal_year == ov_fy_latest - 1)
ov_sr_peak   <- ov_nat %>% slice_max(sr_rate, n = 1, with_ties = FALSE)
ov_sr_fell   <- ov_latest$sr_rate < ov_sr_peak$sr_rate - 0.02

# Judicial districts in the latest fiscal year (only districts with enough people to give a stable rate)
ov_min_elig <- 100
ov_dist_all <-
  supervision_df %>%
  filter(geo_level_type == "state_district", fiscal_year == ov_fy_latest,
         !geo_level %in% c("National Total", "Unknown/Unreported")) %>%
  group_by(geo_level) %>%
  dplyr::summarize(across(all_of(ov_count_cols), ~.x %>% sum(na.rm = TRUE))) %>%
  ungroup() %>%
  mutate(sr_rate = supervised_release_flag / total_sentenced_fsr)

ov_n_districts_all <- nrow(ov_dist_all)

ov_dist <-
  ov_dist_all %>%
  filter(total_sentenced_fsr >= ov_min_elig) %>%
  arrange(sr_rate) %>%
  mutate(rank = row_number(),
         size_px = scales::rescale(sqrt(total_sentenced_fsr), to = c(8, 30)))

ov_n_districts <- nrow(ov_dist)
ov_dist_lo     <- ov_dist %>% slice_min(sr_rate, n = 1, with_ties = FALSE)
ov_dist_hi_n   <- sum(ov_dist$sr_rate >= 0.999)
ov_dist_med    <- median(ov_dist$sr_rate)

# Coverage figures used in the About tab
ov_n_states   <- supervision_df %>% filter(geo_level_type == "state_name", !geo_level %in% c("National Total", "Unknown/Unreported")) %>% pull(geo_level) %>% n_distinct()
ov_n_offices  <- supervision_df %>% filter(geo_level_type == "po_office", !geo_level %in% c("National Total", "Unknown/Unreported")) %>% pull(geo_level) %>% n_distinct()
ov_age_range  <- range(supervision_df$age, na.rm = TRUE)


##Overview Charts
ov_cols <- c("Supervised release" = "#009E73", "Probation" = "#0072B2")

ov_chart_style <- function(p){
  p %>%
    layout(font = list(family = "Roboto, sans-serif", size = 13, color = "#2C2C2C"),
           paper_bgcolor = "rgba(0,0,0,0)",
           plot_bgcolor = "rgba(0,0,0,0)") %>%
    config(displayModeBar = FALSE)
}

# Chart 1: national trend (two panels, because the two rates sit on very different scales)
ov_plot_trend <- function(){
  d <- ov_nat
  
  sr_hover <- paste0("<b>FY", d$fiscal_year, "</b><br>", fmt_pct(d$sr_rate), " received supervised release<br>",
                     fmt_int(d$supervised_release_flag), " of ", fmt_int(d$total_sentenced_fsr), " people sentenced to prison")
  pb_hover <- paste0("<b>FY", d$fiscal_year, "</b><br>", fmt_pct(d$pb_rate), " received probation<br>",
                     fmt_int(d$probation_sentence_flag), " of ", fmt_int(d$total_sentenced), " people sentenced")
  
  p_sr <-
    plot_ly(x = d$fiscal_year, y = d$sr_rate, type = "scatter", mode = "lines+markers", name = "Supervised release",
            line = list(color = ov_cols[["Supervised release"]], width = 2.5),
            marker = list(color = ov_cols[["Supervised release"]], size = 7),
            text = sr_hover, hoverinfo = "text", showlegend = FALSE)
  
  p_pb <-
    plot_ly(x = d$fiscal_year, y = d$pb_rate, type = "scatter", mode = "lines+markers", name = "Probation",
            line = list(color = ov_cols[["Probation"]], width = 2.5),
            marker = list(color = ov_cols[["Probation"]], size = 7),
            text = pb_hover, hoverinfo = "text", showlegend = FALSE)
  
  top_ann <-
    list(
      list(x = ov_sr_peak$fiscal_year, y = ov_sr_peak$sr_rate, xref = "x", yref = "y",
           text = paste0(fmt_pct(ov_sr_peak$sr_rate), "<br>FY", ov_sr_peak$fiscal_year),
           showarrow = TRUE, arrowhead = 2, arrowsize = 0.8, ax = -10, ay = -38, font = list(size = 12),
           bgcolor = "rgba(255,255,255,0.85)"),
      list(x = ov_fy_latest, y = ov_latest$sr_rate, xref = "x", yref = "y",
           text = paste0(fmt_pct(ov_latest$sr_rate), "<br>FY", ov_fy_latest),
           showarrow = TRUE, arrowhead = 2, arrowsize = 0.8, ax = 0, ay = 42, font = list(size = 12),
           bgcolor = "rgba(255,255,255,0.85)"),
      list(x = 0, y = 1.0, xref = "paper", yref = "paper", xanchor = "left", yanchor = "bottom", showarrow = FALSE,
           text = "<b>Supervised release</b>",
           font = list(size = 12, color = ov_cols[["Supervised release"]])),
      list(x = 0, y = 0.405, xref = "paper", yref = "paper", xanchor = "left", yanchor = "bottom", showarrow = FALSE,
           text = "<b>Probation</b>",
           font = list(size = 12, color = ov_cols[["Probation"]]))
    )
  
  subplot(p_sr, p_pb, nrows = 2, shareX = TRUE, titleY = FALSE, margin = 0.05) %>%
    layout(hovermode = "closest",
           xaxis  = list(title = "", tickvals = d$fiscal_year, tickformat = "d", showgrid = FALSE,
                         range = c(min(d$fiscal_year) - 0.5, max(d$fiscal_year) + 0.5)),
           xaxis2 = list(title = "Fiscal year (October to September)", tickvals = d$fiscal_year, tickformat = "d", showgrid = FALSE,
                         range = c(min(d$fiscal_year) - 0.5, max(d$fiscal_year) + 0.5)),
           yaxis  = list(domain = c(0.5, 0.96), range = c(0.72, 0.96), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE),
           yaxis2 = list(domain = c(0, 0.37), range = c(0, 0.13), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE),
           margin = list(l = 55, r = 20, t = 15, b = 50),
           annotations = top_ann) %>%
    ov_chart_style()
}

# Chart 2: spread across judicial districts in the latest fiscal year
ov_plot_district <- function(){
  d <-
    ov_dist %>%
    mutate(hover = paste0("<b>", geo_level, "</b><br>", fmt_pct(sr_rate), " received supervised release<br>",
                          fmt_int(supervised_release_flag), " of ", fmt_int(total_sentenced_fsr), " people sentenced to prison"))
  
  low3 <- d %>% slice_min(sr_rate, n = 3, with_ties = FALSE)
  ann <-
    lapply(seq_len(nrow(low3)), function(i){
      list(x = low3$rank[i], y = low3$sr_rate[i], xref = "x", yref = "y", text = low3$geo_level[i],
           showarrow = TRUE, arrowhead = 0, arrowwidth = 1, ax = 55 + 12 * i, ay = -14 - 26 * (i - 1),
           xanchor = "left", font = list(size = 12), bgcolor = "rgba(255,255,255,0.85)")
    })
  ann <- c(ann, list(
    list(x = 1, y = ov_latest$sr_rate, xref = "paper", yref = "y", xanchor = "right", yanchor = "bottom", showarrow = FALSE,
         text = paste0("National: ", fmt_pct(ov_latest$sr_rate)), font = list(size = 12),
         bgcolor = "rgba(255,255,255,0.85)")))
  
  plot_ly(d, x = ~rank, y = ~sr_rate, type = "scatter", mode = "markers",
          marker = list(size = ~size_px, sizemode = "diameter", color = hex_alpha(ov_cols[["Supervised release"]], 0.7),
                        line = list(color = "#00785A", width = 1)),
          text = ~hover, hoverinfo = "text") %>%
    layout(xaxis = list(title = paste0("Districts, lowest to highest (", nrow(d), ")"),
                        showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, range = c(-5, nrow(d) + 4)),
           yaxis = list(title = "Supervised release rate", tickformat = ".0%", range = c(0.3, 1.03),
                        gridcolor = "#E6DCC3", zeroline = FALSE),
           shapes = list(list(type = "line", x0 = 0, x1 = 1, xref = "paper", y0 = ov_latest$sr_rate, y1 = ov_latest$sr_rate,
                              line = list(color = "#2C2C2C", width = 1.5, dash = "dash"))),
           annotations = ann,
           margin = list(l = 65, r = 20, t = 15, b = 50)) %>%
    ov_chart_style()
}

##Layout helpers shared by the Overview and About pages
ab_card <- function(title, where, steps, tip = NULL, caution = NULL){
  div(class = "ab-card",
      h3(title),
      div(class = "ab-where", HTML(where)),
      tags$ol(lapply(steps, function(s) tags$li(HTML(s)))),
      if(!is.null(tip)) div(class = "ab-tip", HTML(paste0("<strong>Tip:</strong> ", tip))),
      if(!is.null(caution)) div(class = "ab-caution", HTML(paste0("<strong>Watch out:</strong> ", caution))))
}

ab_table <- function(df, code_cols = character(0)){
  div(class = "ab-table-wrap",
      tags$table(class = "table table-sm ab-table",
                 tags$thead(tags$tr(lapply(names(df), tags$th))),
                 tags$tbody(lapply(seq_len(nrow(df)), function(i){
                   tags$tr(lapply(names(df), function(nm){
                     val <- df[[nm]][i]
                     tags$td(if(nm %in% code_cols) tags$code(val) else HTML(val))
                   }))
                 }))))
}

ab_term_group <- function(title, terms){
  div(class = "ab-term-group",
      h3(title),
      lapply(terms, function(t){
        tags$dl(class = "ab-term", tags$dt(t[[1]]), tags$dd(HTML(t[[2]])))
      }))
}

##Overview Page (landing page)
ov_kpi <- function(label, value, note, accent = NULL){
  div(class = paste("ov-kpi", accent),
      div(class = "ov-kpi-label", label),
      div(class = "ov-kpi-value", value),
      div(class = "ov-kpi-note", note))
}

ov_trend_block <- function(text, plot_id, caption, height = 380, min_width = NULL){
  div(class = "ov-trend",
      div(class = "ov-trend-text", text),
      div(class = "ov-trend-chart",
          # On narrow screens a chart with a min_width scrolls sideways instead of squeezing its labels
          if(!is.null(min_width)) div(class = "ov-scroll-hint", "Swipe sideways to see the whole chart."),
          div(class = "ov-plot-scroll",
              div(style = if(!is.null(min_width)) paste0("min-width:", min_width, "px;"),
                  plotlyOutput(plot_id, height = height, width = "100%"))),
          div(class = "ov-caption", caption)))
}

ov_link_card <- function(id, title, desc){
  actionLink(inputId = id,
             label = tagList(span(class = "t", title), span(class = "d", desc)),
             class = "ov-card-link")
}

fy_lbl <- function(x) paste0("FY", x)

# Trend 1: national rates over time
ov_t1_title <-
  if(ov_sr_fell){
    paste0("Supervised release rose for years, then dropped in ", fy_lbl(ov_fy_latest), ".")
  }else if(ov_latest$sr_rate >= ov_first$sr_rate){
    "Supervised release has become more common."
  }else{
    "Supervised release has become less common."
  }

ov_t1_sr <-
  paste0("Supervised release follows a prison term. Among people sentenced to prison, the share who also received it was ",
         fmt_pct(ov_first$sr_rate), " in ", fy_lbl(ov_fy_first),
         if(ov_sr_fell){
           paste0(", reached ", fmt_pct(ov_sr_peak$sr_rate), " in ", fy_lbl(ov_sr_peak$fiscal_year),
                  " and was ", fmt_pct(ov_latest$sr_rate), " in ", fy_lbl(ov_fy_latest), ".")
         }else{
           paste0(" and was ", fmt_pct(ov_latest$sr_rate), " in ", fy_lbl(ov_fy_latest), ".")
         })

ov_t1_pb <-
  paste0("Probation is a sentence served in the community instead of prison. It was given to ", fmt_pct(ov_first$pb_rate),
         " of everyone sentenced in ", fy_lbl(ov_fy_first), " and ", fmt_pct(ov_latest$pb_rate), " in ", fy_lbl(ov_fy_latest), ".")

# Trend 2: differences between districts
ov_t2_title <- "Supervised release rates vary widely between districts."

ov_t2_body <-
  paste0("In ", fy_lbl(ov_fy_latest), ", among the ", ov_n_districts, " judicial districts with at least ", ov_min_elig,
         " prison sentences, the share receiving supervised release ranged from ", fmt_pct(ov_dist_lo$sr_rate), " in ",
         ov_dist_lo$geo_level, " to ", fmt_pct(max(ov_dist$sr_rate)), ". The middle district was at ", fmt_pct(ov_dist_med), ".",
         if(ov_dist_hi_n > 0) paste0(" ", ov_dist_hi_n, " ", ifelse(ov_dist_hi_n == 1, "district was", "districts were"), " at 99.9% or above.") else "")

ov_t2_note <-
  paste0("Districts differ in the mix of cases they handle, such as immigration, drug and fraud cases, so a gap between two districts ",
         "is not by itself a measure of local practice.")

ov_reading_notes <-
  tags$ul(
    tags$li(HTML("<strong>Two rates, two groups.</strong> The supervised release rate divides by people sentenced to prison, because only they can receive it. The probation rate divides by everyone sentenced.")),
    tags$li(HTML("<strong>Fiscal years</strong> run from October 1 to September 30. FY2025 covers October 2024 through September 2025.")),
    tags$li(HTML(paste0("<strong>Small numbers.</strong> Rates for small districts, offices or groups can swing widely. The district chart leaves out districts with fewer than ",
                        ov_min_elig, " prison sentences."))),
    tags$li(HTML("<strong>Missing information.</strong> Some records are blank for race, education, gender or citizenship. They are kept in the dashboards and counted when no filter is applied."))
  )

# Pointer to the modeling page
ov_model_callout <-
  div(class = "ov-work",
      h3("What is behind these differences?"),
      p("Statistical models fitted to the same data show which people, offenses and places are linked to supervised release and probation, and how that has changed over time. ",
        "The results are summarized, with charts and links to two full reports, on their own page."),
      actionLink("ov_go_model", "Read Modeling Supervision Rates", class = "ov-btn"))

overview_ui <-
  div(class = "ov-page",
      
      # Hero: what this is
      div(class = "ov-hero",
          h1(HTML(paste0("Federal sentencing: supervised release and probation, ", fy_lbl(ov_fy_first), "&ndash;", fy_lbl(ov_fy_latest)))),
          p(class = "ov-lede",
            "After a federal conviction, a person may be sentenced to prison, to probation in the community instead of prison, or to both. ",
            "People who leave prison often serve a further term of supervised release. This dashboard shows how often each outcome is used, ",
            "how that has changed, and how it varies by place and by who is being sentenced."),
          p(class = "ov-body",
            HTML(paste0("The figures come from the U.S. Sentencing Commission's ",
                        ab_link_html("https://www.ussc.gov/research/datafiles/commission-datafiles", "individual offender datafiles"),
                        ", which describe people sentenced for federal felonies and Class A misdemeanors. The dashboards cover ", fmt_int(sum(ov_nat$total_sentenced)),
                        " sentencings across ", ov_n_districts_all, " judicial districts, ", ov_n_offices, " probation offices and ", ov_n_states,
                        " states and territories."))),
          span(class = "ov-asof", paste0("Data through ", fy_lbl(ov_fy_latest)))),
      
      # Headline numbers
      div(class = "ov-kpis",
          ov_kpi(paste0("People sentenced, ", fy_lbl(ov_fy_latest)), fmt_int(ov_latest$total_sentenced),
                 paste0(fmt_int(ov_latest$total_sentenced_fsr), " to prison. ", fmt_int(ov_first$total_sentenced), " in ", fy_lbl(ov_fy_first), ".")),
          ov_kpi("Received supervised release", fmt_pct(ov_latest$sr_rate),
                 paste0("of people sentenced to prison. ", ifelse(ov_sr_fell, paste0("High of ", fmt_pct(ov_sr_peak$sr_rate), " in ", fy_lbl(ov_sr_peak$fiscal_year), "."),
                                                                  paste0(fmt_pct(ov_first$sr_rate), " in ", fy_lbl(ov_fy_first), ".")))),
          ov_kpi("Received probation", fmt_pct(ov_latest$pb_rate),
                 paste0("of everyone sentenced. ", fmt_pct(ov_first$pb_rate), " in ", fy_lbl(ov_fy_first), "."), accent = "pb"),
          ov_kpi("Gap between districts", HTML(paste0(fmt_pct(ov_dist_lo$sr_rate, 0), "&ndash;", fmt_pct(max(ov_dist$sr_rate), 0))),
                 paste0("lowest to highest supervised release rate among ", ov_n_districts, " districts in ", fy_lbl(ov_fy_latest), "."))),
      
      # The national picture
      h2(class = "ov-section-title", "The national picture"),
      p(class = "ov-section-sub",
        "Calculated from the dashboard data, using the same definitions as the dashboards below."),
      
      ov_trend_block(
        tagList(h3(ov_t1_title), p(ov_t1_sr), p(ov_t1_pb)),
        "ov_trend_chart",
        "Top: people who received supervised release as a share of people sentenced to prison. Bottom: people who received probation as a share of everyone sentenced. Each point is one fiscal year (October to September). The two panels use different vertical scales.",
        height = 440),
      
      ov_trend_block(
        tagList(h3(ov_t2_title), p(ov_t2_body), p(ov_t2_note)),
        "ov_district_chart",
        paste0("Each circle is a judicial district in ", fy_lbl(ov_fy_latest), " with at least ", ov_min_elig,
               " prison sentences; larger circles mean more prison sentences. Districts are ranked from lowest to highest. The dashed line is the national rate. Hover for details."),
        height = 420),
      
      ov_model_callout,
      
      # Where to go next
      h2(class = "ov-section-title", "Explore the dashboards"),
      div(class = "ov-explore",
          ov_link_card("ov_go_main", "Main Dashboard", "Map, ranked bars and a downloadable table by state, district or probation office."),
          ov_link_card("ov_go_state", "State Dashboard", "Pick a state and compare it with the nation by race, gender and offense."),
          ov_link_card("ov_go_national", "National Dashboard", "National trends and offense-type breakdowns across fiscal years."),
          ov_link_card("ov_go_custom", "Custom Visualization", "Build your own bar, line, box or violin chart and download it."),
          ov_link_card("ov_go_about", "About and data dictionary", "How to use the tool, key terms, definitions, methods and caveats.")),
      
      # Reading notes
      div(class = "ov-notes",
          h3("Reading these numbers"),
          ov_reading_notes,
          p("See the About tab for full definitions and caveats."))
  )


####Modeling Supervision Rates Page####
##Model results (static)
# The results come from two analysis reports: "Supervised Release Analysis Report" and "Probation Analysis Report"
# (the PDFs in www/reports/). They are fixed values stored in data/modeling-results/*.csv and do not change
# with the dashboard filters. The report generator (reports/build_reports.py) reads the same files.
# If the models are re-run, update the CSV files and mod_model_fy below.
#   odds_ratios.csv, fiscal_year_odds_ratios.csv  odds ratios with 95% Wald confidence intervals
#   icc.csv                                       shares of variation tied to place (intraclass correlations)
#   robustness.csv                                odds ratios under different ways of handling missing data
#   missing_by_field.csv, missing_education_by_group.csv   how much data was missing, and where
# Odds ratios (OR) come from the primary binomial mixed-effects model (complete cases), adjusted for age,
# race/ethnicity, gender, citizenship, education, offense category, fiscal year and judicial district.

# Fiscal years covered by the models. The dashboard data can extend past these as new years are added.
mod_model_fy      <- c(2014, 2025)
mod_model_current <- ov_fy_latest == mod_model_fy[2]

mod_read <- function(file) readr::read_csv(here("data", "modeling-results", file), show_col_types = FALSE)

# Odds ratios (95% Wald CI) from the primary model, complete cases
mod_or <- mod_read("odds_ratios.csv")

# Fiscal-year odds ratios versus FY2014 (same model)
mod_fy_or <- mod_read("fiscal_year_odds_ratios.csv")

# Share of the variation left after accounting for individual characteristics that is tied to place
# (intraclass correlation, ICC). "primary" = main model; "ar1" = model with yearly trends (AR1);
# "ar1_mice" = the AR1 model after multiple imputation. "remainder" rows are what is left for individual-level variation.
mod_icc_state <- "State baseline"
mod_icc_dist  <- "District baseline (within state)"
mod_icc_comb  <- "State and district baselines combined"
mod_icc_dtrnd <- "District trend over time"
mod_icc_ntrnd <- "National trend over time"
mod_icc_dall  <- "District baseline plus district trend"
mod_icc_rem   <- "Individual-level variation (remainder)"

mod_icc <- mod_read("icc.csv")

# Do the findings change with the way missing data are handled?
mod_flag <- function(label, kind) paste0('<span class="mod-flag mod-flag-', kind, '">', label, '</span>')

# Odds ratios under different approaches to missing data (long format: one row per comparison and approach)
# (display holds pre-formatted text such as "1.490"; keep it as text so trailing zeros survive)
mod_robust_long <- readr::read_csv(here("data", "modeling-results", "robustness.csv"), show_col_types = FALSE,
                                   col_types = readr::cols(display = readr::col_character(), note = readr::col_character(),
                                                           .default = readr::col_guess()))

mod_robust_table <- function(oc){
  d <- mod_robust_long %>% filter(outcome == oc)
  approaches <- unique(d$approach)
  d %>%
    mutate(cell = ifelse(is.na(note), display, paste0(display, " (", note, ")")),
           Result = mod_flag(result_label, result_kind)) %>%
    dplyr::select(order, Comparison = comparison, approach, cell, Result) %>%
    pivot_wider(names_from = approach, values_from = cell) %>%
    arrange(order) %>%
    dplyr::select(Comparison, all_of(approaches), Result)
}

# Supervised release: three approaches (from the report's three-way comparison)
mod_robust_sr <- mod_robust_table("Supervised release")

# Probation: complete cases versus multiple imputation
mod_robust_pb <- mod_robust_table("Probation")

# How much data were missing (from the reports)
mod_miss_field <- mod_read("missing_by_field.csv")
mod_miss_edu   <- mod_read("missing_education_by_group.csv")


##Helpers for the modeling text
mod_pct    <- function(p) ifelse(p < 10, sub("\\.0$", "", sprintf("%.1f", p)), as.character(floor(p + 0.5 + 1e-9)))
mod_lower  <- function(x) paste0(mod_pct(100 * (1 - x)), "%")
mod_higher <- function(x) paste0(mod_pct(100 * (x - 1)), "%")
mod_times  <- function(x) paste0(sprintf("%.1f", x), " times")
pct0       <- function(x) fmt_pct(x, 0)

mod_or_val  <- function(oc, lbl) mod_or$or[mod_or$outcome == oc & mod_or$label == lbl]
mod_fy_val  <- function(oc, fy)  mod_fy_or$or[mod_fy_or$outcome == oc & mod_fy_or$fiscal_year == fy]
mod_icc_val <- function(oc, comp, col) mod_icc[[col]][mod_icc$outcome == oc & mod_icc$component == comp]
sr_or <- function(lbl) mod_or_val("Supervised release", lbl)
pb_or <- function(lbl) mod_or_val("Probation", lbl)

# Plain-language reading of an odds ratio and its confidence interval
mod_plain <- function(or, lo, hi){
  ifelse(lo <= 1 & hi >= 1, "No clear difference",
         ifelse(or < 1, paste0(mod_lower(or), " lower odds"),
                ifelse(or >= 2, paste0(mod_times(or), " the odds"), paste0(mod_higher(or), " higher odds"))))
}

mod_or_table <- function(oc){
  mod_or %>%
    filter(outcome == oc) %>%
    mutate(Comparison = ifelse(is.na(ref), label, paste0(label, " vs. ", ref)),
           `Odds ratio (95% CI)` = paste0(fmt_or(or), " (", fmt_or(lo), " to ", fmt_or(hi), ")"),
           `In plain words` = ifelse(group == "Age",
                                     paste0(sprintf("%.1f", 100 * (or - 1)), "% higher odds with each year of age"),
                                     mod_plain(or, lo, hi))) %>%
    dplyr::select(Group = group, Comparison, `Odds ratio (95% CI)`, `In plain words`)
}

mod_icc_table <- function(oc){
  fmt_cell <- function(x) ifelse(is.na(x), "&ndash;", fmt_pct(x, 1))
  mod_icc %>%
    filter(outcome == oc) %>%
    mutate(primary = fmt_cell(primary), ar1 = fmt_cell(ar1), ar1_mice = fmt_cell(ar1_mice)) %>%
    dplyr::select(`Source of variation` = component, `Main model` = primary,
                  `Model with yearly trends` = ar1, `Yearly-trend model, multiple imputation` = ar1_mice)
}

# Context for the race and ethnicity result, calculated from the dashboard data
# (people sentenced to prison, all fiscal years, rows with a recorded race and citizenship)
mod_race_cit <-
  supervision_df %>%
  filter(geo_level_type == "state_name", geo_level == "National Total",
         newrace_description %in% c("White", "Hispanic"),
         newcit_description %in% c("U.S. Citizen", "Non-U.S. Citizen")) %>%
  group_by(newrace_description, newcit_description) %>%
  dplyr::summarize(sr = sum(supervised_release_flag, na.rm = TRUE),
                   elig = sum(total_sentenced_fsr, na.rm = TRUE), .groups = "drop") %>%
  mutate(rate = sr / elig)

mod_race_cit_rate <- function(race, cit) mod_race_cit$rate[mod_race_cit$newrace_description == race & mod_race_cit$newcit_description == cit]
mod_noncit_share  <- function(race){
  d <- mod_race_cit %>% filter(newrace_description == race)
  sum(d$elig[d$newcit_description == "Non-U.S. Citizen"]) / sum(d$elig)
}


##Charts
# Chart: adjusted odds ratios for both outcomes
mod_chart_rows <-
  tibble::tribble(
    ~row,                    ~label,              ~is_header,
    "<b>Citizenship</b>",    NA,                  TRUE,
    "Non-U.S. citizen",      "Non-U.S. citizen",  FALSE,
    "<b>Gender</b>",         NA,                  TRUE,
    "Female",                "Female",            FALSE,
    "<b>Race/ethnicity</b>", NA,                  TRUE,
    "Black",                 "Black",             FALSE,
    "Hispanic",              "Hispanic",          FALSE,
    "Other race",            "Other race",        FALSE,
    "<b>Offense</b>",        NA,                  TRUE,
    "Sexual offense",        "Sexual offense",    FALSE,
    "Violent offense",       "Violent offense",   FALSE,
    "Fraud",                 "Fraud",             FALSE,
    "Immigration",           "Immigration",       FALSE,
    "Other offense",         "Other offense",     FALSE
  ) %>%
  mutate(ypos = rev(seq_len(n())))

mod_plot_or <- function(){
  off <- 0.17
  d <-
    mod_or %>%
    inner_join(mod_chart_rows %>% filter(!is_header) %>% dplyr::select(label, ypos), by = "label") %>%
    mutate(ypos = ypos + ifelse(outcome == "Supervised release", off, -off),
           hover = paste0("<b>", label, "</b> (", tolower(outcome), ")<br>Odds ratio ", fmt_or(or),
                          " (95% CI ", fmt_or(lo), " to ", fmt_or(hi), ")<br>", mod_plain(or, lo, hi)))
  
  p <- plot_ly()
  for(oc in names(ov_cols)){
    dd <- d %>% filter(outcome == oc)
    p <-
      p %>%
      add_trace(data = dd, x = ~or, y = ~ypos, type = "scatter", mode = "markers", name = oc,
                marker = list(color = ov_cols[[oc]], size = 9, line = list(color = "#FFFFFF", width = 1)),
                error_x = list(type = "data", symmetric = FALSE, array = dd$hi - dd$or, arrayminus = dd$or - dd$lo,
                               color = ov_cols[[oc]], thickness = 1.8, width = 0),
                text = dd$hover, hoverinfo = "text")
  }
  
  p %>%
    layout(xaxis = list(type = "log", range = c(log10(0.035), log10(8.5)), title = "Odds ratio (log scale)",
                        tickvals = c(0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8), ticktext = c("0.05", "0.1", "0.25", "0.5", "1", "2", "4", "8"),
                        gridcolor = "#E6DCC3", zeroline = FALSE),
           yaxis = list(title = "", tickvals = mod_chart_rows$ypos, ticktext = mod_chart_rows$row, range = c(0.4, nrow(mod_chart_rows) + 0.9),
                        showgrid = FALSE, zeroline = FALSE, automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.13),
           shapes = list(list(type = "line", x0 = 1, x1 = 1, y0 = 0, y1 = 1, yref = "paper",
                              line = list(color = "#2C2C2C", width = 1.2, dash = "dot"))),
           margin = list(l = 10, r = 20, t = 10, b = 70)) %>%
    ov_chart_style()
}

# Chart: fiscal-year odds ratios versus FY2014, both outcomes
mod_plot_fy <- function(){
  p <- plot_ly()
  for(oc in names(ov_cols)){
    d <-
      mod_fy_or %>%
      filter(outcome == oc) %>%
      bind_rows(tibble(outcome = oc, fiscal_year = mod_model_fy[1], or = 1, lo = 1, hi = 1)) %>%
      arrange(fiscal_year) %>%
      mutate(hover = ifelse(fiscal_year == mod_model_fy[1],
                            paste0("<b>FY", fiscal_year, "</b> (reference year)<br>Odds ratio 1"),
                            paste0("<b>FY", fiscal_year, "</b> (", tolower(oc), ")<br>Odds ratio ", fmt_or(or),
                                   " (95% CI ", fmt_or(lo), " to ", fmt_or(hi), ")<br>",
                                   ifelse(lo <= 1 & hi >= 1, paste0("No clear difference from FY", mod_model_fy[1]),
                                          paste0(mod_plain(or, lo, hi), " than in FY", mod_model_fy[1])))))
    p <-
      p %>%
      add_ribbons(data = d, x = ~fiscal_year, ymin = ~lo, ymax = ~hi, fillcolor = hex_alpha(ov_cols[[oc]], 0.16),
                  line = list(color = "rgba(0,0,0,0)"), hoverinfo = "none", showlegend = FALSE) %>%
      add_trace(data = d, x = ~fiscal_year, y = ~or, type = "scatter", mode = "lines+markers", name = oc,
                line = list(color = ov_cols[[oc]], width = 2.5), marker = list(color = ov_cols[[oc]], size = 7),
                text = d$hover, hoverinfo = "text")
  }
  
  yr <- seq(mod_model_fy[1], max(mod_fy_or$fiscal_year))
  p %>%
    layout(xaxis = list(title = "Fiscal year (October to September)", tickvals = yr, tickformat = "d", showgrid = FALSE,
                        range = c(min(yr) - 0.5, max(yr) + 0.5)),
           yaxis = list(type = "log", range = c(log10(0.62), log10(1.85)), title = paste0("Odds ratio versus FY", mod_model_fy[1], " (log scale)"),
                        tickvals = c(0.7, 0.8, 0.9, 1, 1.25, 1.5), ticktext = c("0.7", "0.8", "0.9", "1", "1.25", "1.5"),
                        gridcolor = "#E6DCC3", zeroline = FALSE),
           legend = list(orientation = "h", x = 0, y = -0.2),
           shapes = list(list(type = "line", x0 = 0, x1 = 1, xref = "paper", y0 = 1, y1 = 1,
                              line = list(color = "#2C2C2C", width = 1.2, dash = "dot"))),
           annotations = list(
             list(x = 0.01, y = 0.99, xref = "paper", yref = "paper", xanchor = "left", yanchor = "top", showarrow = FALSE,
                  text = paste0("Above 1: higher odds than in FY", mod_model_fy[1]), font = list(size = 11, color = "#555555")),
             list(x = 0.01, y = 0.01, xref = "paper", yref = "paper", xanchor = "left", yanchor = "bottom", showarrow = FALSE,
                  text = paste0("Below 1: lower odds than in FY", mod_model_fy[1]), font = list(size = 11, color = "#555555"))),
           hovermode = "closest",
           margin = list(l = 65, r = 20, t = 10, b = 85)) %>%
    ov_chart_style()
}


##Charts for the outcome tabs
mod_grey   <- "#8C8470"   # neutral for "everything else" bars
mod_ink    <- "#2C2C2C"
mod_ref_ln <- list(color = mod_ink, width = 1.2, dash = "dot")

mod_log_ticks <- function(rng){
  all <- c(0.03, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
  all[all >= rng[1] & all <= rng[2]]
}

# Forest plot: adjusted odds ratios for one outcome, grouped by characteristic
mod_forest_groups <-
  list("Citizenship"    = list(title = "Citizenship (vs. U.S. citizen)",           labels = "Non-U.S. citizen"),
       "Gender"         = list(title = "Gender (vs. male)",                         labels = "Female"),
       "Race/ethnicity" = list(title = "Race and ethnicity (vs. White)",            labels = c("Black", "Hispanic", "Other race")),
       "Education"      = list(title = "Education (vs. less than H.S.)",            labels = c("H.S. graduate", "Some college", "College graduate")),
       "Offense"        = list(title = "Offense type (vs. drug trafficking)",       labels = c("Firearms", "Fraud", "Immigration", "Other offense", "Sexual offense", "Violent offense")))

# Results that change direction or significance when missing data are handled differently
mod_less_certain <- function(oc){
  mod_robust_long %>%
    filter(outcome == oc, result_kind %in% c("warn", "bad")) %>%
    mutate(label = sub(" vs\\..*$", "", comparison)) %>%
    pull(label) %>%
    unique()
}

mod_plot_forest <- function(oc){
  col <- ov_cols[[oc]]
  unc <- mod_less_certain(oc)
  d0  <- mod_or %>% filter(outcome == oc, group != "Age")
  
  rows <-
    purrr::map_dfr(names(mod_forest_groups), function(g){
      items <- d0 %>% filter(group == g) %>% arrange(desc(or))
      bind_rows(tibble(label = NA_character_, row = paste0("<b>", mod_forest_groups[[g]]$title, "</b>")),
                tibble(label = items$label, row = paste0(items$label, ifelse(items$label %in% unc, " \u2020", ""))))
    }) %>%
    mutate(ypos = rev(seq_len(n())))
  
  d <-
    d0 %>%
    inner_join(rows %>% filter(!is.na(label)) %>% dplyr::select(label, ypos), by = "label") %>%
    mutate(clear = !(lo <= 1 & hi >= 1),
           hover = paste0("<b>", label, "</b> (", ifelse(is.na(ref), "", paste0("vs. ", ref, ", ")), tolower(oc), ")<br>Odds ratio ", fmt_or(or),
                          " (95% CI ", fmt_or(lo), " to ", fmt_or(hi), ")<br>", mod_plain(or, lo, hi),
                          ifelse(label %in% unc, "<br><i>Less certain: depends on how missing data are handled</i>", "")))
  
  p <- plot_ly()
  for(is_clear in c(TRUE, FALSE)){
    dd <- d %>% filter(clear == is_clear)
    if(nrow(dd) == 0) next
    p <-
      p %>%
      add_trace(data = dd, x = ~or, y = ~ypos, type = "scatter", mode = "markers",
                name = ifelse(is_clear, "95% interval excludes 1 (clear difference)", "95% interval includes 1 (no clear difference)"),
                marker = if(is_clear) list(color = col, size = 9, line = list(color = "#FFFFFF", width = 1))
                else list(color = "#FFFFFF", size = 9, line = list(color = col, width = 2)),
                error_x = list(type = "data", symmetric = FALSE, array = dd$hi - dd$or, arrayminus = dd$or - dd$lo,
                               color = col, thickness = 1.8, width = 0),
                text = dd$hover, hoverinfo = "text")
  }
  
  xr <- c(0.035, 8.5)
  p %>%
    layout(xaxis = list(type = "log", range = log10(xr), title = "Odds ratio (log scale)",
                        tickvals = mod_log_ticks(xr), ticktext = as.character(mod_log_ticks(xr)),
                        gridcolor = "#E6DCC3", zeroline = FALSE),
           yaxis = list(title = "", tickvals = rows$ypos, ticktext = rows$row, range = c(0.4, nrow(rows) + 0.6),
                        showgrid = FALSE, zeroline = FALSE, automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.12),
           shapes = list(list(type = "line", x0 = 1, x1 = 1, y0 = 0, y1 = 1, yref = "paper", line = mod_ref_ln)),
           showlegend = length(unique(d$clear)) > 1,
           margin = list(l = 10, r = 20, t = 10, b = 80)) %>%
    ov_chart_style()
}

# Unadjusted rate by group (bars), national total over all fiscal years in the dashboard data
mod_obs_data <- function(oc, by){
  num <- if(oc == "Supervised release") "supervised_release_flag" else "probation_sentence_flag"
  den <- if(oc == "Supervised release") "total_sentenced_fsr"     else "total_sentenced"
  supervision_df %>%
    filter(geo_level_type == "state_name", geo_level == "National Total") %>%
    mutate(group = stringr::str_to_sentence(.data[[by]]),
           group = if(by == "offense_category") ifelse(group == "Other", "Other offense", group) else group) %>%
    filter(!is.na(group), group != "") %>%
    group_by(group) %>%
    dplyr::summarize(events = sum(.data[[num]], na.rm = TRUE), denom = sum(.data[[den]], na.rm = TRUE), .groups = "drop") %>%
    mutate(rate = events / denom)
}

mod_obs_sr_offense <- mod_obs_data("Supervised release", "offense_category")
mod_obs_pb_offense <- mod_obs_data("Probation", "offense_category")
mod_obs_all <- c("Supervised release" = sum(ov_nat$supervised_release_flag) / sum(ov_nat$total_sentenced_fsr),
                 "Probation"          = sum(ov_nat$probation_sentence_flag) / sum(ov_nat$total_sentenced))

mod_plot_obs <- function(oc, d){
  col  <- ov_cols[[oc]]
  noun <- if(oc == "Supervised release") "received supervised release" else "received probation"
  base <- if(oc == "Supervised release") "people sentenced to prison" else "people sentenced"
  allr <- mod_obs_all[[oc]]
  d <-
    d %>%
    arrange(rate) %>%
    mutate(group = factor(group, levels = group),
           hover = paste0("<b>", group, "</b><br>", fmt_pct(rate), " ", noun, "<br>", fmt_int(events), " of ", fmt_int(denom), " ", base))
  
  plot_ly(d, x = ~rate, y = ~group, type = "bar", orientation = "h", marker = list(color = col),
          text = ~fmt_pct(rate, 1), textposition = "outside", cliponaxis = FALSE, textfont = list(color = mod_ink),
          hovertext = ~hover, hoverinfo = "text") %>%
    layout(xaxis = list(range = c(0, min(1.12, max(d$rate) * 1.18 + 0.02)), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE,
                        title = if(oc == "Supervised release") "Share who received supervised release" else "Share who received probation"),
           yaxis = list(title = "", automargin = TRUE),
           bargap = 0.45, showlegend = FALSE,
           shapes = list(list(type = "line", x0 = allr, x1 = allr, y0 = 0, y1 = 1, yref = "paper", line = mod_ref_ln)),
           annotations = list(list(x = allr, y = 1, yref = "paper", yanchor = "bottom", xanchor = "center", showarrow = FALSE,
                                   text = paste0("All offenses: ", fmt_pct(allr, 1)), font = list(size = 11, color = "#555555"))),
           margin = list(l = 10, r = 40, t = 26, b = 55)) %>%
    ov_chart_style()
}

# Supervised release rate by citizenship and race (unadjusted, from mod_race_cit)
mod_plot_race_cit <- function(){
  col <- ov_cols[["Supervised release"]]
  d <-
    mod_race_cit %>%
    mutate(cit = ifelse(newcit_description == "U.S. Citizen", "U.S. citizens", "Non-U.S. citizens"),
           ybase = ifelse(cit == "U.S. citizens", 2, 1),
           y = ybase + ifelse(newrace_description == "White", 0.13, -0.13),
           hover = paste0("<b>", newrace_description, " ", tolower(cit), "</b><br>", fmt_pct(rate), " received supervised release<br>",
                          fmt_int(sr), " of ", fmt_int(elig), " people sentenced to prison"))
  seg <- d %>% group_by(ybase) %>% dplyr::summarize(lo = min(rate), hi = max(rate), .groups = "drop")
  
  p <-
    plot_ly() %>%
    add_trace(x = as.vector(rbind(seg$lo, seg$hi, NA)), y = as.vector(rbind(seg$ybase, seg$ybase, NA)), type = "scatter", mode = "lines",
              line = list(color = "#CDBF9C", width = 2), hoverinfo = "none", showlegend = FALSE)
  for(rc in c("White", "Hispanic")){
    dd <- d %>% filter(newrace_description == rc)
    p <-
      p %>%
      add_trace(data = dd, x = ~rate, y = ~y, type = "scatter", mode = "markers+text", name = rc,
                marker = list(color = col, size = 11, symbol = ifelse(rc == "White", "circle", "diamond"), line = list(color = "#FFFFFF", width = 1)),
                text = paste0(" ", fmt_pct(dd$rate, 0)), textposition = "middle right", textfont = list(color = mod_ink, size = 12),
                hovertext = dd$hover, hoverinfo = "text")
  }
  p %>%
    layout(xaxis = list(range = c(0.55, 1.07), tickvals = seq(0.6, 1, 0.1), ticktext = paste0(seq(60, 100, 10), "%"),
                        gridcolor = "#E6DCC3", zeroline = FALSE, title = "Share who received supervised release"),
           yaxis = list(title = "", tickvals = c(1, 2), ticktext = c("Non-U.S.<br>citizens", "U.S.<br>citizens"), range = c(0.45, 2.55),
                        showgrid = FALSE, zeroline = FALSE, automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.3),
           margin = list(l = 10, r = 20, t = 10, b = 75)) %>%
    ov_chart_style()
}

# Where the variation sits: model with yearly trends (bars) and the same model after imputation (diamonds)
mod_icc_labels <- c("State baseline" = "State baseline", "District baseline (within state)" = "District baseline",
                    "District trend over time" = "District trend over time", "National trend over time" = "National trend over time",
                    "Individual-level variation (remainder)" = "Everything else (individual cases)")

mod_plot_icc <- function(oc){
  col <- ov_cols[[oc]]
  d <-
    mod_icc %>%
    filter(outcome == oc, kind != "combined") %>%
    mutate(lbl = unname(mod_icc_labels[component]), is_rem = kind == "remainder") %>%
    arrange(ar1) %>%
    mutate(lbl = factor(lbl, levels = lbl),
           hover = paste0("<b>", lbl, "</b><br>", fmt_pct(ar1, 1), " of the variation (model with yearly trends)<br>",
                          fmt_pct(ar1_mice, 1), " after multiple imputation"),
           lab_x = pmax(ar1, ar1_mice) + 0.012,
           lab = paste0(ifelse(ar1 < 0.1, fmt_pct(ar1, 1), fmt_pct(ar1, 0)), " (", ifelse(ar1_mice < 0.1, fmt_pct(ar1_mice, 1), fmt_pct(ar1_mice, 0)), ")"))
  
  plot_ly() %>%
    add_trace(data = d, x = ~ar1, y = ~lbl, type = "bar", orientation = "h", name = "Model with yearly trends",
              marker = list(color = ifelse(d$is_rem, mod_grey, col)), hovertext = d$hover, hoverinfo = "text") %>%
    add_trace(data = d, x = ~ar1_mice, y = ~lbl, type = "scatter", mode = "markers", name = "After multiple imputation (in brackets)",
              marker = list(symbol = "diamond", size = 10, color = "#FFFFFF", line = list(color = mod_ink, width = 1.8)),
              hovertext = d$hover, hoverinfo = "text") %>%
    add_trace(data = d, x = ~lab_x, y = ~lbl, type = "scatter", mode = "text", text = d$lab, textposition = "middle right",
              textfont = list(color = mod_ink, size = 12), hoverinfo = "none", showlegend = FALSE, cliponaxis = FALSE) %>%
    layout(xaxis = list(range = c(0, min(1, max(d$lab_x) + 0.22)), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE,
                        title = "Share of total variation (log-odds scale)"),
           yaxis = list(title = "", automargin = TRUE, showgrid = FALSE),
           bargap = 0.5,
           legend = list(orientation = "h", x = 0, y = -0.28),
           margin = list(l = 10, r = 20, t = 10, b = 85)) %>%
    ov_chart_style()
}

# Do the results change with the way missing data are handled? One row per comparison, one marker per approach
mod_plot_robust <- function(oc){
  col <- ov_cols[[oc]]
  d <- mod_robust_long %>% filter(outcome == oc)
  rows <- d %>% distinct(order, comparison) %>% arrange(order) %>% mutate(ypos = rev(seq_len(n())))
  shp <- c("Complete cases" = "circle", "Missing as its own category" = "square", "Multiple imputation" = "diamond")
  aps <- names(shp)[names(shp) %in% unique(d$approach)]
  off <- if(length(aps) == 3) c(0.24, 0, -0.24) else c(0.16, -0.16)
  
  d <-
    d %>%
    left_join(rows, by = c("order", "comparison")) %>%
    mutate(y = ypos + off[match(approach, aps)],
           hover = paste0("<b>", comparison, "</b><br>", approach, ": odds ratio ", display, ifelse(is.na(note), "", paste0(" (", note, ")")),
                          "<br>Result: ", tolower(result_label)))
  rng <- d %>% group_by(ypos) %>% dplyr::summarize(lo = min(or), hi = max(or), .groups = "drop")
  
  p <-
    plot_ly() %>%
    add_trace(x = as.vector(rbind(rng$lo, rng$hi, NA)), y = as.vector(rbind(rng$ypos, rng$ypos, NA)), type = "scatter", mode = "lines",
              line = list(color = "#CDBF9C", width = 1.5), hoverinfo = "none", showlegend = FALSE)
  for(ap in aps){
    dd <- d %>% filter(approach == ap)
    p <-
      p %>%
      add_trace(data = dd, x = ~or, y = ~y, type = "scatter", mode = "markers", name = ap,
                marker = list(color = col, size = 9, symbol = shp[[ap]], line = list(color = "#FFFFFF", width = 1)),
                hovertext = dd$hover, hoverinfo = "text")
  }
  xr <- c(min(d$or) / 1.3, max(d$or) * 1.3)
  p %>%
    layout(xaxis = list(type = "log", range = log10(xr), title = "Odds ratio (log scale)",
                        tickvals = mod_log_ticks(xr), ticktext = as.character(mod_log_ticks(xr)),
                        gridcolor = "#E6DCC3", zeroline = FALSE),
           yaxis = list(title = "", tickvals = rows$ypos, ticktext = rows$comparison, range = c(0.4, nrow(rows) + 0.6),
                        showgrid = FALSE, zeroline = FALSE, automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.12),
           shapes = list(list(type = "line", x0 = 1, x1 = 1, y0 = 0, y1 = 1, yref = "paper", line = mod_ref_ln)),
           margin = list(l = 10, r = 20, t = 10, b = 85)) %>%
    ov_chart_style()
}

# How much data were missing, and where (from the reports)
mod_plot_miss_field <- function(){
  d <- mod_miss_field %>% mutate(field = factor(field, levels = rev(c("Education", "Race and ethnicity", "Citizenship", "Gender"))))
  p <- plot_ly()
  for(oc in rev(names(ov_cols))){
    dd <- d %>% filter(outcome == oc)
    p <-
      p %>%
      add_trace(data = dd, x = ~pct / 100, y = ~field, type = "bar", orientation = "h", name = oc, marker = list(color = ov_cols[[oc]]),
                text = paste0(sprintf("%.1f", dd$pct), "%"), textposition = "outside", cliponaxis = FALSE, textfont = list(color = mod_ink, size = 12),
                hovertext = paste0("<b>", dd$field, "</b> (", tolower(oc), " data)<br>", sprintf("%.1f", dd$pct), "% of rows missing"), hoverinfo = "text")
  }
  p %>%
    layout(barmode = "group", bargap = 0.4,
           xaxis = list(range = c(0, 0.09), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE, title = "Share of rows with the value missing"),
           yaxis = list(title = "", automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.3, traceorder = "reversed"),
           margin = list(l = 10, r = 40, t = 10, b = 80)) %>%
    ov_chart_style()
}

mod_plot_miss_edu <- function(){
  lay <- tibble::tribble(~key, ~lab, ~ypos,
                         "H1", "<b>By citizenship</b>", 8,
                         "U.S. citizen", "U.S. citizen", 7, "Non-U.S. citizen", "Non-U.S. citizen", 6,
                         "H2", "<b>By race and ethnicity</b>", 5,
                         "White", "White", 4, "Black", "Black", 3, "Hispanic", "Hispanic", 2, "Other", "Other", 1)
  d <- mod_miss_edu %>% inner_join(lay, by = c("group" = "key")) %>%
    mutate(y = ypos + ifelse(outcome == "Supervised release", 0.15, -0.15),
           hover = paste0("<b>", group, "</b> (", tolower(outcome), " data)<br>", sprintf("%.1f", pct), "% have no education recorded"))
  p <- plot_ly()
  for(oc in names(ov_cols)){
    dd <- d %>% filter(outcome == oc)
    p <-
      p %>%
      add_trace(data = dd, x = ~pct / 100, y = ~y, type = "scatter", mode = "markers+text", name = oc,
                marker = list(color = ov_cols[[oc]], size = 10, symbol = ifelse(oc == "Supervised release", "circle", "diamond"), line = list(color = "#FFFFFF", width = 1)),
                text = paste0(" ", sprintf("%.1f", dd$pct), "%"), textposition = "middle right", textfont = list(color = mod_ink, size = 11),
                hovertext = dd$hover, hoverinfo = "text")
  }
  p %>%
    layout(xaxis = list(range = c(0, 0.19), tickformat = ".0%", gridcolor = "#E6DCC3", zeroline = FALSE, title = "Share of rows with no education recorded"),
           yaxis = list(title = "", tickvals = lay$ypos, ticktext = lay$lab, range = c(0.4, 8.6), showgrid = FALSE, zeroline = FALSE, automargin = TRUE),
           legend = list(orientation = "h", x = 0, y = -0.22),
           margin = list(l = 10, r = 20, t = 10, b = 80)) %>%
    ov_chart_style()
}

##Layout helpers for the modeling page
# A chart with a title and a caption, in a card
mod_chart <- function(title, plot_id, caption, height = 380, min_width = 480){
  div(class = "mod-chart",
      h4(class = "mod-chart-title", title),
      div(class = "ov-scroll-hint", "Swipe sideways to see the whole chart."),
      div(class = "ov-plot-scroll",
          div(style = paste0("min-width:", min_width, "px;"),
              plotlyOutput(plot_id, height = height, width = "100%"))),
      div(class = "ov-caption", caption))
}

mod_forest_caption <- function(oc){
  paste0("Odds ratios with 95% confidence intervals from the main model, adjusted for the other factors in the model. Each point compares a group with the reference group named in its row heading. ",
         if(any(mod_or$outcome == oc & mod_or$group != "Age" & mod_or$lo <= 1 & mod_or$hi >= 1)) "Filled points: the interval excludes 1, so the difference is clear. Hollow points: no clear difference. " else "Every interval excludes 1, so every difference is clear. ",
         if(length(mod_less_certain(oc)) > 0) "\u2020 Less certain: the result changes when missing data are handled differently (see below). " else "",
         "The horizontal axis is logarithmic. Age is in the table at the end of this page.")
}

mod_icc_caption <- function(oc){
  paste0("Share of the total variation in ", tolower(oc), " tied to each source, on the log-odds scale. Bars come from the model that lets each district follow its own trend over time (AR1). ",
         "Diamonds are the same model after multiple imputation. The grey bar is everything else: differences between individual cases that place and year do not explain. ",
         "The table below also shows the main model.")
}

# One finding: a headline number, what it compares, and the explanation
mod_finding <- function(num, caption, title, ..., kind = "sr"){
  div(class = paste("mod-finding", kind),
      div(class = "mod-finding-num", span(class = "n", num), span(class = "c", caption)),
      div(class = "mod-finding-body", h4(title), ...))
}

mod_report_links <-
  div(class = "mod-links",
      ab_link(ov_report_sr_url, "Supervised Release Analysis Report (PDF)"),
      ab_link(ov_report_pb_url, "Probation Analysis Report (PDF)"))

mod_currency_note <-
  if(mod_model_current){
    paste0("Results cover ", fy_lbl(mod_model_fy[1]), " to ", fy_lbl(mod_model_fy[2]), ", the same years as the dashboards. They are fixed values from the reports and do not change with filters.")
  }else{
    paste0("Results were estimated on data for ", fy_lbl(mod_model_fy[1]), " to ", fy_lbl(mod_model_fy[2]), ". The dashboards now include ", fy_lbl(ov_fy_latest),
           ", so the dashboard charts and these results can differ. They are fixed values from the reports and do not change with filters.")
  }


##Tab 1: At a glance
mod_glance_table <-
  tribble(
    ~Topic,                               ~`Supervised release`,                                                                                                                                                                                                                                                                                                                                                                                                                ~Probation,
    "Who is counted",                     "People sentenced to prison, the only people who can receive it.",                                                                                                                                                                                                                                                                                                                                                                       "Everyone sentenced.",
    "Largest differences between people", paste0("Citizenship first: non-citizens had ", mod_lower(sr_or("Non-U.S. citizen")), " lower odds. Then offense type: sexual offenses had ", mod_times(sr_or("Sexual offense")), " the odds of drug trafficking, and immigration and other offenses had about ", mod_lower(sr_or("Immigration")), " lower odds."),
    paste0("Offense type first: fraud had ", mod_times(pb_or("Fraud")), " the odds of drug trafficking and sexual offenses had ", mod_lower(pb_or("Sexual offense")), " lower odds. Then gender and citizenship: women had ", mod_times(pb_or("Female")), " the odds of men, and non-citizens had ", mod_lower(pb_or("Non-U.S. citizen")), " lower odds."),
    "Role of place",                      paste0("Large. About ", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")), " of the variation left after accounting for people's characteristics is tied to the state and district, mostly the district."),
    paste0("Small. About ", pct0(mod_icc_val("Probation", mod_icc_comb, "primary")), " is tied to the state and district."),
    "Trend",                              paste0("Odds rose from ", fy_lbl(mod_model_fy[1]), " to a peak in FY2024, then fell in FY2025."),
    "Odds fell after FY2016 to a low in FY2023. FY2024 and FY2025 were higher than FY2023 but still below FY2014.",
    "Held up when missing data were handled differently?",
    "Mostly. Citizenship, ethnicity, gender and the FY2025 drop held. The education and firearms results did not.",
    "Yes. Every result kept its direction. The violent offense gap shrank and the small age effect disappeared."
  )

mod_or_list <-
  tags$ul(class = "ov-or-list",
          tags$li(tags$strong("Citizenship. "),
                  paste0("Non-U.S. citizens had ", mod_lower(sr_or("Non-U.S. citizen")), " lower odds of supervised release and ",
                         mod_lower(pb_or("Non-U.S. citizen")), " lower odds of probation than U.S. citizens who were otherwise similar. ",
                         "The reports link this to deportation after a prison term.")),
          tags$li(tags$strong("Offense. "),
                  paste0("Compared with drug trafficking, sexual offenses had ", sprintf("%.1f", sr_or("Sexual offense")),
                         " times the odds of supervised release. Fraud, immigration and other offenses had much lower odds of supervised release (",
                         mod_lower(sr_or("Fraud")), " to ", mod_lower(sr_or("Immigration")),
                         " lower) and much higher odds of probation (",
                         sprintf("%.1f", pb_or("Immigration")), " to ", sprintf("%.1f", pb_or("Fraud")), " times).")),
          tags$li(tags$strong("Gender. "),
                  paste0("Women had ", mod_lower(sr_or("Female")), " lower odds of supervised release and ",
                         sprintf("%.1f", pb_or("Female")), " times the odds of probation compared with men.")),
          tags$li(tags$strong("Race and ethnicity. "),
                  paste0("Compared with White defendants, Hispanic defendants had ", mod_lower(sr_or("Hispanic")),
                         " lower odds of supervised release and ", mod_lower(pb_or("Hispanic")), " lower odds of probation. ",
                         "Black defendants had ", mod_lower(pb_or("Black")), " lower odds of probation, with no clear difference in supervised release."))
  )

mod_or_how <-
  p(class = "ov-model-note",
    span(class = "ov-badge", "How to read this"),
    " An odds ratio of 1 means no difference. Below 1 means lower odds and above 1 means higher odds, compared with the reference group, ",
    "for people who are alike on everything else in the model. These are patterns in administrative records. ",
    "They do not show why outcomes differ. See ",
    actionLink("mod_go_terms", "Key terms"), " for definitions, and Methods and limits for a worked example.")

mod_place_block <-
  div(class = "mod-place",
      h3("Place matters much more for supervised release than for probation."),
      p(paste0("After accounting for the characteristics of the people sentenced, how much of the remaining variation is tied to the state and district where they were sentenced? ",
               "For supervised release it is about ", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")), ", and nearly all of that is the district rather than the state (",
               pct0(mod_icc_val("Supervised release", mod_icc_dist, "primary")), " district, ", fmt_pct(mod_icc_val("Supervised release", mod_icc_state, "primary")), " state). ",
               "For probation it is about ", pct0(mod_icc_val("Probation", mod_icc_comb, "primary")), ".")),
      div(class = "mod-tiles",
          ov_kpi("Supervised release", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")),
                 paste0("of the leftover variation is tied to state and district. When each district can follow its own trend over time, the district-level share is ",
                        pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1")), ".")),
          ov_kpi("Probation", pct0(mod_icc_val("Probation", mod_icc_comb, "primary")),
                 paste0("of the leftover variation is tied to state and district. With district trends, the district-level share is ",
                        pct0(mod_icc_val("Probation", mod_icc_dall, "ar1")), "."), accent = "pb")),
      p(class = "ov-small",
        "These shares are intraclass correlations (ICCs), calculated on the model's log-odds scale. The Overview and the dashboard map show the district differences directly."))

mod_time_text <-
  tagList(
    h3("Over time, the two outcomes moved in different directions."),
    p(paste0("Compared with ", fy_lbl(mod_model_fy[1]), ", the odds of supervised release were higher in every later year. They peaked in FY2024 (",
             mod_higher(mod_fy_val("Supervised release", 2024)), " higher), then fell back in FY2025, when they were ", mod_higher(mod_fy_val("Supervised release", 2025)), " higher.")),
    p(paste0("The odds of probation did not change in FY2015 or FY2016, then fell, reaching a low in FY2023 (",
             mod_lower(mod_fy_val("Probation", 2023)), " lower than in ", fy_lbl(mod_model_fy[1]), "). They were ",
             mod_lower(mod_fy_val("Probation", 2024)), " lower in FY2024 and ", mod_lower(mod_fy_val("Probation", 2025)), " lower in FY2025.")),
    p(class = "ov-model-note",
      span(class = "ov-badge", "Note"),
      paste0(" These odds ratios hold constant the characteristics in the model, so they point to changes beyond who was sentenced. The data do not show why the changes happened.",
             if(mod_model_current){
               paste0(" The drop in supervised release in FY2025 was not offset by more probation. In the dashboard data the probation rate was ",
                      fmt_pct(ov_latest$pb_rate), " in FY2025 and ", fmt_pct(ov_prev$pb_rate), " in FY2024.")
             }else{""})))

mod_glance <-
  tagList(
    h2("Two outcomes, two different patterns"),
    p(class = "ab-lead", "Both outcomes were modeled the same way, so they can be compared side by side. Open the other tabs for the full write-up of each."),
    ab_table(mod_glance_table),
    
    h2(class = "mod-gap", "Who receives each outcome"),
    ov_trend_block(
      tagList(h3("Citizenship, offense type and gender are linked to the largest differences."), mod_or_list, mod_or_how),
      "mod_or_chart",
      paste0("Odds ratios with 95% confidence intervals from the main models (", fy_lbl(mod_model_fy[1]), " to ", fy_lbl(mod_model_fy[2]),
             "), adjusted for age, race and ethnicity, gender, citizenship, education, offense type, fiscal year and district. Reference groups: U.S. citizen, male, White, drug trafficking. ",
             "Points to the left of the dotted line mean lower odds and points to the right mean higher odds. The horizontal axis is logarithmic. ",
             "Age, education and firearms results are in the tables on each outcome's tab."),
      height = 600, min_width = 430),
    
    h2(class = "mod-gap", "Where someone is sentenced"),
    div(class = "mod-card", mod_place_block),
    
    h2(class = "mod-gap", "How odds changed over time"),
    ov_trend_block(
      mod_time_text,
      "mod_fy_chart",
      paste0("Odds ratio for each fiscal year compared with ", fy_lbl(mod_model_fy[1]), ", with 95% confidence bands. ",
             "The odds ratios are adjusted for the other factors in the model. The vertical axis is logarithmic."),
      height = 420)
  )


##Tab 2: Supervised release
mod_sr_findings <-
  tagList(
    mod_finding(fmt_or(sr_or("Non-U.S. citizen")), "Non-U.S. citizen vs. U.S. citizen",
                "Citizenship shows the largest difference.",
                p(paste0("Non-U.S. citizens had ", mod_lower(sr_or("Non-U.S. citizen")), " lower odds of supervised release than U.S. citizens who were otherwise similar. ",
                         "The report links this to deportation after a prison term, which makes supervised release in the United States less likely.")),
                kind = "sr"),
    
    mod_finding(fmt_or(sr_or("Sexual offense")), "Sexual offense vs. drug trafficking",
                "Offense type is the next largest factor.",
                p(paste0("Sexual offenses had ", mod_times(sr_or("Sexual offense")), " the odds of supervised release compared with drug trafficking. ",
                         "The report links this to post-prison supervision requirements for these offenses.")),
                p(paste0("Other offense types had lower odds than drug trafficking: immigration offenses ", mod_lower(sr_or("Immigration")), " lower, other offenses ",
                         mod_lower(sr_or("Other offense")), " lower, fraud ", mod_lower(sr_or("Fraud")), " lower and violent offenses ", mod_lower(sr_or("Violent offense")),
                         " lower. The report links the immigration result to deportation.")),
                kind = "sr"),
    
    mod_finding(fmt_or(sr_or("Female")), "Female vs. male",
                "Women had lower odds than men.",
                p(paste0("Women had ", mod_lower(sr_or("Female")), " lower odds of supervised release than men. For probation the gap runs the other way: women had higher odds.")),
                kind = "sr"),
    
    mod_finding(fmt_or(sr_or("Hispanic")), "Hispanic vs. White",
                "Hispanic defendants had lower odds, and most of the raw gap reflects citizenship.",
                p(paste0("After accounting for the other factors in the model, Hispanic defendants had ", mod_lower(sr_or("Hispanic")), " lower odds than White defendants. ",
                         "Black defendants showed no clear difference (OR ", fmt_or(sr_or("Black")), ", 95% CI ", fmt_or(mod_or$lo[mod_or$outcome == "Supervised release" & mod_or$label == "Black"]),
                         " to ", fmt_or(mod_or$hi[mod_or$outcome == "Supervised release" & mod_or$label == "Black"]), "), and defendants in the Other group had ",
                         mod_higher(sr_or("Other race")), " higher odds.")),
                p(class = "ov-small",
                  paste0("Context from the dashboard data (", fy_lbl(ov_fy_first), " to ", fy_lbl(ov_fy_latest), ", people sentenced to prison): ",
                         pct0(mod_noncit_share("Hispanic")), " of Hispanic defendants were non-citizens, compared with ", pct0(mod_noncit_share("White")), " of White defendants. ",
                         "Among U.S. citizens, ", fmt_pct(mod_race_cit_rate("Hispanic", "U.S. Citizen")), " of Hispanic and ", fmt_pct(mod_race_cit_rate("White", "U.S. Citizen")),
                         " of White defendants received supervised release. Among non-citizens the rates were ", fmt_pct(mod_race_cit_rate("Hispanic", "Non-U.S. Citizen")), " for Hispanic and ",
                         fmt_pct(mod_race_cit_rate("White", "Non-U.S. Citizen")), " for White defendants. These rates are not adjusted for other factors. The report flags the gap among non-citizens as worth further study.")),
                kind = "sr"),
    
    mod_finding(fmt_or(sr_or("Age (per year)")), "per additional year of age",
                "Age has a small effect.",
                p(paste0("Each additional year of age was linked to ", sprintf("%.1f", 100 * (sr_or("Age (per year)") - 1)), "% higher odds of supervised release. This is small next to the other factors.")),
                kind = "sr"),
    
    mod_finding(fmt_or(mod_fy_val("Supervised release", 2025)), paste0(fy_lbl(2025), " vs. ", fy_lbl(mod_model_fy[1])),
                "Supervised release rose for years, then fell in FY2025.",
                p(paste0("Compared with ", fy_lbl(mod_model_fy[1]), ", the odds were higher in every later year. They peaked in FY2024 (OR ", fmt_or(mod_fy_val("Supervised release", 2024)),
                         "), dipped in FY2020 (OR ", fmt_or(mod_fy_val("Supervised release", 2020)), "), which the report suggests may reflect the pandemic, and fell to ",
                         fmt_or(mod_fy_val("Supervised release", 2025)), " in FY2025. Only FY2014 and FY2020 had lower odds than FY2025.")),
                p(paste0("The FY2025 drop held under both missing-data checks.",
                         if(mod_model_current){
                           paste0(" It was not offset by more probation: in the dashboard data the probation rate was ", fmt_pct(ov_latest$pb_rate), " in FY2025 and ", fmt_pct(ov_prev$pb_rate), " in FY2024.")
                         }else{""},
                         " The report flags the drop for further study. The data cannot show why it happened.")),
                kind = "sr"),
    
    mod_finding(pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")), "of leftover variation tied to state and district",
                "Place matters a great deal.",
                p(paste0("After accounting for people's characteristics, ", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")),
                         " of the remaining variation in supervised release is tied to where someone was sentenced: ",
                         fmt_pct(mod_icc_val("Supervised release", mod_icc_state, "primary")), " to the state and ",
                         fmt_pct(mod_icc_val("Supervised release", mod_icc_dist, "primary")), " to the district within the state. Districts differ much more than states.")),
                p(paste0("A version of the model that lets each district follow its own trend over time fit the data much better and puts the district-level share at ",
                         pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1")), " (", pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1_mice")), " after multiple imputation). ",
                         "Districts differ in their level of supervised release and in how it has moved over time.")),
                kind = "sr")
  )

mod_sr_uncertain <-
  tagList(
    mod_finding(fmt_or(sr_or("College graduate")), "College graduate vs. less than H.S.",
                "Education: the result depends on how missing data are handled.",
                p(paste0("With complete cases, high school graduates and people with some college had ", mod_higher(sr_or("H.S. graduate")), " and ", mod_higher(sr_or("Some college")),
                         " higher odds than people without a high school diploma, and college graduates had ", mod_lower(sr_or("College graduate")), " lower odds. ",
                         "When people with missing education were kept, the college graduate result flipped (OR 1.137) or disappeared (OR 1.01, not significant).")),
                p("People with no education recorded had far lower odds of supervised release (OR 0.088 when missing is its own category), likely because missing education is concentrated among non-citizens and immigration cases. Treat the education pattern as unreliable."),
                kind = "caution"),
    
    mod_finding(fmt_or(sr_or("Firearms")), "Firearms vs. drug trafficking",
                "Firearms offenses: the direction of the result reverses.",
                p(paste0("With complete cases, firearms offenses had ", mod_lower(sr_or("Firearms")), " lower odds than drug trafficking. When people with missing demographic information were kept, ",
                         "firearms offenses had higher odds (OR 1.245 with missing as its own category and 1.44 with multiple imputation).")),
                p("The report traces this to the people with missing information, who are mostly immigration cases: 66% of people with missing demographic data had immigration offenses, compared with 31% of people with complete data. Treat the firearms result as unreliable."),
                kind = "caution")
  )

mod_sr_robust <-
  tagList(
    p("The reports re-ran the model two more ways to see whether missing information changes the results. One kept people with missing values by treating missing as its own category. The other, multiple imputation, fills in missing values several times using the other information on each person and combines the results."),
    mod_chart("Odds ratios under three approaches to missing data", "mod_sr_robust_chart",
              "Odds ratios for the same comparisons under three ways of handling missing data: complete cases only (the main model), missing as its own category, and multiple imputation. Markers that stay close together show a result that holds. Markers that spread out, or land on the other side of 1, show a result that depends on the method. The horizontal axis is logarithmic.",
              height = 620, min_width = 520),
    ab_table(mod_robust_sr),
    p(class = "ov-small", "Odds ratios are shown. Result compares the direction and significance across the three approaches."),
    tags$ul(
      tags$li("Citizenship, ethnicity, gender and the FY2025 drop held under all three approaches. FY2025 stayed well below FY2024 in each."),
      tags$li("The offense results kept their direction, but their size moved. For example, immigration offenses had an odds ratio of 0.153 in the main model and 0.433 under multiple imputation."),
      tags$li(paste0("Missing education and race are concentrated among non-citizens. In the supervised release data, ",
                     sprintf("%.1f%%", mod_miss_edu$pct[mod_miss_edu$outcome == "Supervised release" & mod_miss_edu$group == "Non-U.S. citizen"]),
                     " of non-citizens had no education recorded, compared with ",
                     sprintf("%.1f%%", mod_miss_edu$pct[mod_miss_edu$outcome == "Supervised release" & mod_miss_edu$group == "U.S. citizen"]),
                     " of U.S. citizens. Because non-citizens rarely receive supervised release, dropping rows with missing values can distort comparisons. This is why the education and firearms results change.")),
      tags$li(paste0("The geography results held up. The district-level share of variation was ", pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1_mice")),
                     " after multiple imputation, compared with ", pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1")), " in the main yearly-trend model."))
    )
  )

mod_sr_tab <-
  tagList(
    h2("Supervised release"),
    p(class = "ab-lead", "Supervised release is a period of supervision by a probation officer after a prison term. This model compares the share of people sentenced to prison who also received it."),
    div(class = "mod-short",
        h3("In short"),
        tags$ul(
          tags$li("Citizenship and offense type are linked to the largest differences between people. Non-citizens and people sentenced for immigration, fraud or violent offenses had lower odds of supervised release than comparable people. People sentenced for sexual offenses had higher odds."),
          tags$li(paste0("Where someone is sentenced matters a great deal: about ", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")), " of the leftover variation is tied to state and district, mostly the district.")),
          tags$li("The odds rose from FY2014 to FY2024 and fell in FY2025."),
          tags$li("The education and firearms results are not reliable. They change when people with missing information are kept in the analysis.")
        )),
    
    h3("What the model found"),
    mod_chart("Adjusted odds ratios for supervised release", "mod_sr_forest", mod_forest_caption("Supervised release"), height = 600, min_width = 520),
    mod_sr_findings,
    
    h3("The raw rates behind the odds ratios"),
    p(paste0("For context, these are the unadjusted rates in the dashboard data (", fy_lbl(ov_fy_first), " to ", fy_lbl(ov_fy_latest),
             "). They do not hold other factors constant, which is why they can look different from the odds ratios above.")),
    div(class = "mod-chart-pair",
        mod_chart("Supervised release rate by offense type", "mod_sr_obs",
                  "Share of people sentenced to prison who received supervised release, national total. The dotted line is the rate across all offenses.",
                  height = 340, min_width = 380),
        mod_chart("Citizenship and race together", "mod_sr_racecit",
                  paste0("Among U.S. citizens the rates are almost the same. Among non-citizens the rate is lower for Hispanic than for White defendants. ",
                         "Hispanic defendants are far more likely to be non-citizens (", pct0(mod_noncit_share("Hispanic")), " versus ", pct0(mod_noncit_share("White")), "), which is why the overall gap looks large."),
                  height = 340, min_width = 380)),
    
    h3("Less certain results"),
    mod_sr_uncertain,
    
    h3("Where the variation sits"),
    p("How much of the variation in supervised release, after accounting for people's characteristics, is tied to place? Trends come from a version of the model that lets each district follow its own year-to-year path (an AR1 model)."),
    mod_chart("Where the variation in supervised release sits", "mod_sr_icc", mod_icc_caption("Supervised release"), height = 330, min_width = 480),
    ab_table(mod_icc_table("Supervised release")),
    
    h3("How well did the findings hold up?"),
    mod_sr_robust,
    
    tags$details(class = "mod-details",
                 tags$summary("All odds ratios from the main model"),
                 ab_table(mod_or_table("Supervised release")),
                 p(class = "ov-small", "Odds ratios with 95% confidence intervals from the complete-case model. Education and firearms results depend on how missing data are handled.")),
    
    h3("Full report"),
    div(class = "mod-links", ab_link(ov_report_sr_url, "Supervised Release Analysis Report (PDF)"))
  )


##Tab 3: Probation
mod_pb_findings <-
  tagList(
    mod_finding(fmt_or(pb_or("Fraud")), "Fraud vs. drug trafficking",
                "Offense type shows the largest differences.",
                p(paste0("Compared with drug trafficking, fraud had ", mod_times(pb_or("Fraud")), " the odds of probation, other offenses ", mod_times(pb_or("Other offense")), ", immigration ",
                         mod_times(pb_or("Immigration")), " and firearms ", mod_times(pb_or("Firearms")), ". Violent offenses had ", mod_lower(pb_or("Violent offense")),
                         " lower odds and sexual offenses ", mod_lower(pb_or("Sexual offense")), " lower odds.")),
                p("For fraud, immigration and other offenses the pattern is the reverse of supervised release, where they had lower odds. For sexual offenses it is also the reverse: lowest odds of probation and highest odds of supervised release."),
                kind = "pb"),
    
    mod_finding(fmt_or(pb_or("Female")), "Female vs. male",
                "Women had much higher odds of probation.",
                p(paste0("Women had ", mod_times(pb_or("Female")), " the odds of probation compared with men. For supervised release the gap runs the other way (women had ",
                         mod_lower(sr_or("Female")), " lower odds).")),
                kind = "pb"),
    
    mod_finding(fmt_or(pb_or("College graduate")), "College graduate vs. less than H.S.",
                "More education is linked to higher odds of probation.",
                p(paste0("Compared with people without a high school diploma, high school graduates had ", mod_higher(pb_or("H.S. graduate")), " higher odds, people with some college ",
                         mod_higher(pb_or("Some college")), " higher and college graduates ", mod_higher(pb_or("College graduate")), " higher. ",
                         "The step-up held under multiple imputation (OR 1.91 for college graduates), unlike the education pattern for supervised release.")),
                kind = "pb"),
    
    mod_finding(fmt_or(pb_or("Non-U.S. citizen")), "Non-U.S. citizen vs. U.S. citizen",
                "Non-citizens had lower odds of probation.",
                p(paste0("Non-U.S. citizens had ", mod_lower(pb_or("Non-U.S. citizen")), " lower odds of probation than U.S. citizens who were otherwise similar. ",
                         "The gap is smaller than for supervised release (", mod_lower(sr_or("Non-U.S. citizen")), " lower). The report links the citizenship pattern to deportation.")),
                kind = "pb"),
    
    mod_finding(fmt_or(pb_or("Black")), "Black vs. White",
                "Black and Hispanic defendants had lower odds of probation.",
                p(paste0("After accounting for the other factors in the model, Black defendants had ", mod_lower(pb_or("Black")), " lower odds of probation than White defendants and Hispanic defendants ",
                         mod_lower(pb_or("Hispanic")), " lower. Defendants in the Other group had ", mod_higher(pb_or("Other race")), " higher odds. ",
                         "For supervised release, Black and White defendants showed no clear difference.")),
                kind = "pb"),
    
    mod_finding(fmt_or(pb_or("Age (per year)")), "per additional year of age",
                "Age makes little difference.",
                p(paste0("Each additional year of age was linked to ", sprintf("%.1f", 100 * (pb_or("Age (per year)") - 1)), "% higher odds of probation in the main model. The effect disappears under multiple imputation (OR 1.00).")),
                kind = "pb"),
    
    mod_finding(fmt_or(mod_fy_val("Probation", 2025)), paste0(fy_lbl(2025), " vs. ", fy_lbl(mod_model_fy[1])),
                "Probation became less common after FY2016.",
                p(paste0("In the main model, the odds of probation did not differ from ", fy_lbl(mod_model_fy[1]), " in FY2015 or FY2016. They then fell, reaching a low in FY2023 (OR ", fmt_or(mod_fy_val("Probation", 2023)),
                         "), and were ", mod_lower(mod_fy_val("Probation", 2024)), " lower in FY2024 and ", mod_lower(mod_fy_val("Probation", 2025)), " lower in FY2025 than in ", fy_lbl(mod_model_fy[1]), ".")),
                p("Over most of this period supervised release became more common while probation became less common. The decline held under both missing-data checks."),
                kind = "pb"),
    
    mod_finding(pct0(mod_icc_val("Probation", mod_icc_comb, "primary")), "of leftover variation tied to state and district",
                "Place matters much less than for supervised release.",
                p(paste0("After accounting for people's characteristics, ", pct0(mod_icc_val("Probation", mod_icc_comb, "primary")), " of the remaining variation in probation is tied to the state and district (",
                         fmt_pct(mod_icc_val("Probation", mod_icc_state, "primary")), " state, ", fmt_pct(mod_icc_val("Probation", mod_icc_dist, "primary")),
                         " district). For supervised release the figure is ", pct0(mod_icc_val("Supervised release", mod_icc_comb, "primary")), ".")),
                p(paste0("When each district can follow its own trend over time, the district-level share rises to ", pct0(mod_icc_val("Probation", mod_icc_dall, "ar1")), " (",
                         pct0(mod_icc_val("Probation", mod_icc_dall, "ar1_mice")), " after multiple imputation), still far below the ", pct0(mod_icc_val("Supervised release", mod_icc_dall, "ar1")),
                         " for supervised release. The reports read this as probation depending more on case characteristics than on place. The model cannot see every case detail, so the rest may include unmeasured features of the case.")),
                kind = "pb")
  )

mod_pb_robust <-
  tagList(
    p("Probation results were more stable than supervised release results. The table compares the main model with the multiple imputation check."),
    mod_chart("Odds ratios from the main model and after multiple imputation", "mod_pb_robust_chart",
              "Odds ratios for the same comparisons from the main model (complete cases) and after multiple imputation. Markers that stay close together show a result that holds. The horizontal axis is logarithmic.",
              height = 680, min_width = 520),
    ab_table(mod_robust_pb),
    p(class = "ov-small", "Odds ratios are shown. Result compares the direction and significance of the two approaches."),
    tags$ul(
      tags$li("Under multiple imputation every result kept its direction, including the education pattern. The violent offense gap shrank, and the small age effect disappeared."),
      tags$li("When missing values were treated as their own category, the citizenship gap grew (OR 0.189). People with missing education had far higher odds of probation (OR 3.965), the opposite of the supervised release data, where they had far lower odds. This suggests the cases with missing education differ between the two datasets."),
      tags$li(paste0("The geography results held up. The district-level share of variation was ", pct0(mod_icc_val("Probation", mod_icc_dall, "ar1_mice")),
                     " after multiple imputation, compared with ", pct0(mod_icc_val("Probation", mod_icc_dall, "ar1")), " in the main yearly-trend model."))
    )
  )

mod_pb_tab <-
  tagList(
    h2("Probation"),
    p(class = "ab-lead", "Probation is a sentence served in the community instead of prison. This model compares the share of everyone sentenced who received it."),
    div(class = "mod-short",
        h3("In short"),
        tags$ul(
          tags$li("Offense type is linked to the largest differences. Fraud, immigration and other offenses had much higher odds of probation than drug trafficking. Violent and sexual offenses had lower odds."),
          tags$li("Women and people with more education had higher odds of probation. Non-citizens, Black defendants and Hispanic defendants had lower odds."),
          tags$li(paste0("Place matters much less than for supervised release: about ", pct0(mod_icc_val("Probation", mod_icc_comb, "primary")), " of the leftover variation is tied to state and district.")),
          tags$li("The odds of probation fell after FY2016 and were lower in FY2025 than in FY2014."),
          tags$li("The results held up well when missing information was handled differently.")
        )),
    
    h3("What the model found"),
    mod_chart("Adjusted odds ratios for probation", "mod_pb_forest", mod_forest_caption("Probation"), height = 600, min_width = 520),
    mod_pb_findings,
    
    h3("The raw rates behind the odds ratios"),
    p(paste0("For context, this is the unadjusted rate in the dashboard data (", fy_lbl(ov_fy_first), " to ", fy_lbl(ov_fy_latest),
             "). It does not hold other factors constant, which is why it can look different from the odds ratios above.")),
    mod_chart("Probation rate by offense type", "mod_pb_obs",
              "Share of everyone sentenced who received probation, national total. The dotted line is the rate across all offenses.",
              height = 340, min_width = 420),
    
    h3("Where the variation sits"),
    p("How much of the variation in probation, after accounting for people's characteristics, is tied to place? Trends come from a version of the model that lets each district follow its own year-to-year path (an AR1 model)."),
    mod_chart("Where the variation in probation sits", "mod_pb_icc", mod_icc_caption("Probation"), height = 330, min_width = 480),
    ab_table(mod_icc_table("Probation")),
    
    h3("How well did the findings hold up?"),
    mod_pb_robust,
    
    tags$details(class = "mod-details",
                 tags$summary("All odds ratios from the main model"),
                 ab_table(mod_or_table("Probation")),
                 p(class = "ov-small", "Odds ratios with 95% confidence intervals from the complete-case model.")),
    
    h3("Full report"),
    div(class = "mod-links", ab_link(ov_report_pb_url, "Probation Analysis Report (PDF)"))
  )


##Tab 4: Methods and limits
mod_ref_table <-
  tribble(
    ~Characteristic,                                          ~`Compared with (reference group)`,
    "Age",                                                    "Each additional year",
    "Race and ethnicity: Black, Hispanic, Other",             "White",
    "Gender: Female",                                         "Male",
    "Citizenship: Non-U.S. citizen",                          "U.S. citizen",
    "Education: H.S. graduate, Some college, College graduate", "Less than H.S. graduate",
    "Offense: Firearms, Fraud, Immigration, Sexual, Violent, Other", "Drug trafficking",
    "Fiscal year",                                            fy_lbl(mod_model_fy[1])
  )

mod_methods <-
  tagList(
    h2("How the analysis was done"),
    
    h3("Data and sample"),
    tags$ul(
      tags$li(HTML(paste0("<strong>Data.</strong> The aggregated sentencing counts behind this dashboard, fiscal years ", mod_model_fy[1], " to ", mod_model_fy[2],
                          ". Each row is a group of people who share the same characteristics and place. In the probation data, 87% of rows describe a single person."))),
      tags$li(HTML("<strong>Supervised release.</strong> Only people sentenced to prison, because only they can receive it (about 488,000 rows).")),
      tags$li(HTML("<strong>Probation.</strong> Everyone sentenced (about 514,000 rows).")),
      tags$li(HTML("<strong>Exclusions.</strong> The Northern Mariana Islands were left out because there were too few cases. The main models also used only rows with no missing values on the variables in the model: about 460,000 of 488,000 rows (94%) for supervised release and about 477,000 of 514,000 rows (93%) for probation. Education was missing most often (5.5% of rows for supervised release and about 7% for probation), followed by race and ethnicity, citizenship and gender.")),
      tags$li(HTML("<strong>Not missing.</strong> Offense category, fiscal year and district had no missing values."))
    ),
    div(class = "mod-chart-pair",
        mod_chart("How much was missing", "mod_miss_field_chart",
                  "Share of rows with a missing value for each field, from the two reports. The supervised release data cover people sentenced to prison. The probation data cover everyone sentenced.",
                  height = 310, min_width = 380),
        mod_chart("Who is missing education", "mod_miss_edu_chart",
                  "Share of rows with no education recorded, by group. Non-citizens and Hispanic defendants are missing education most often in both datasets. Because these groups also differ in their outcomes, dropping rows with missing values can distort comparisons.",
                  height = 390, min_width = 380)),
    
    h3("The model"),
    p(class = "ab-lead", "Each outcome was modeled with a binomial mixed-effects (multilevel) logistic regression, fitted with the glmmTMB package in R. In plain terms:"),
    tags$ul(
      tags$li("For each row, the model uses the number of people who received the outcome and the number who did not."),
      tags$li("It estimates how the odds of the outcome differ between groups of people, holding the other characteristics in the model constant."),
      tags$li("It gives each state, and each district within a state, its own baseline level, so places can differ even when the people sentenced are alike."),
      tags$li("Results are reported as odds ratios with 95% confidence intervals (Wald intervals). Every odds ratio compares a group with its reference group, listed below.")
    ),
    ab_table(mod_ref_table),
    
    h3("Checks on the results"),
    tags$ul(
      tags$li(HTML("<strong>Missing data.</strong> The reports re-ran the models with missing values treated as their own category, and with multiple imputation (five filled-in datasets, using random forests through the miceRanger package, combined with Rubin's rules). Missing education and race are concentrated among non-citizens, a pattern the reports describe as consistent with data that are missing at random, depending on observed variables such as citizenship. Imputation is the check designed for that situation.")),
      tags$li(HTML("<strong>Yearly trends.</strong> The reports replaced the fiscal-year terms with an autoregressive (AR1) structure, which lets each district follow its own year-to-year path. It fit the data much better (by about 5,300 AIC points for supervised release and about 1,600 for probation) and left the results for individual characteristics essentially unchanged. The district-level shares in the tables come from this model.")),
      tags$li(HTML("<strong>Overlapping predictors.</strong> No predictors were strongly overlapping (all variance inflation factors were below 1.4).")),
      tags$li(HTML("<strong>Fiscal year as a fixed or random effect.</strong> For supervised release, treating fiscal year as a fixed effect fit better than a random effect."))
    ),
    
    h3("Reading an odds ratio"),
    div(class = "ab-tip",
        HTML(paste0("<strong>Odds are not percentages.</strong> Odds compare the chance something happens with the chance it does not. If 50 in 100 people receive an outcome, the odds are 1. An odds ratio of 2 doubles the odds to 2, which is about 67 in 100 people, not 100 in 100. ",
                    "As an illustration, if 98 in 100 U.S. citizens received supervised release (odds of 49), an odds ratio of ", fmt_or(sr_or("Non-U.S. citizen")),
                    " would give non-citizens odds of about 2.4, or about 71 in 100. When an outcome is very common or very rare, a large odds ratio can be a small change in percentage points."))),
    
    h3("What this analysis cannot tell you"),
    tags$ul(
      tags$li("It describes patterns in recorded outcomes. It does not show why a difference exists, or whether a difference reflects unequal treatment."),
      tags$li("The model leaves out things that influence sentences, such as criminal history, the guideline range, plea agreements and the facts of the case, because the dashboard data do not include them."),
      tags$li("Odds ratios are not the same as differences in percentages."),
      tags$li("Education and firearms results for supervised release depend on how missing data are handled, so they are labeled as less certain and left off the chart."),
      tags$li("The analysis code also includes models that test whether gaps differ between groups (for example, race within citizenship). The reports do not summarize those, so they are not shown here."),
      tags$li("The results are estimates from one set of data and one set of modeling choices. The reports recommend profile-likelihood confidence intervals for final results, rather than the Wald intervals shown here.")
    ),
    
    h3("Open questions the reports flag"),
    tags$ul(
      tags$li("Which districts, if any, show the largest racial and ethnic gaps. The reports suggest letting the race effect vary by district."),
      tags$li("Why Hispanic non-citizens had lower supervised release rates than White non-citizens."),
      tags$li("What explains the drop in supervised release in FY2025. The data alone cannot show why."),
      tags$li("Whether the education pattern for supervised release differs by offense type.")
    ),
    
    h3("Full reports"),
    p("Each report adds the full odds-ratio tables, every sensitivity check and the technical details of the models."),
    mod_report_links,
    p(class = "ov-small", "Terms such as odds ratio, ICC, AR1 and multiple imputation are defined under Key terms on the About tab.")
  )


##Page
modeling_ui <-
  div(class = "ab-page mod-page",
      div(class = "ab-hero",
          h1("Modeling Supervision Rates"),
          p(class = "ab-lede",
            "The dashboards show how often supervised release and probation are used. This page asks what is linked to the differences: which people, offenses and places are more likely to receive each outcome, and how that has changed over time."),
          p(class = "ab-lede",
            "It summarizes two analysis reports from REFORM Alliance, which fitted statistical models to the same data as the dashboards. ",
            "The reports aim to identify what is linked to higher use of each outcome, to inform efforts to reduce unnecessary use of federal supervision."),
          div(class = "ab-caution",
              HTML("<strong>Read this first:</strong> These results describe patterns in recorded sentences. They do not show why a difference exists or whether it reflects unequal treatment. The models leave out factors such as criminal history, the guideline range and the facts of each case.")),
          mod_report_links,
          p(class = "ov-small", mod_currency_note)),
      tabsetPanel(
        id = "mod_tabs",
        type = "pills",
        tabPanel("At a glance", div(class = "ab-section", mod_glance)),
        tabPanel("Supervised release", div(class = "ab-section", mod_sr_tab)),
        tabPanel("Probation", div(class = "ab-section", mod_pb_tab)),
        tabPanel("Methods and limits", div(class = "ab-section", mod_methods))
      )
  )



##About Page
##Ways to use the tool
ab_use_cards <-
  div(class = "ab-card-grid",
      
      ab_card("See where supervised release is most or least common", "Main Dashboard &rsaquo; sidebar and map",
              c("Set <strong>Outcome Metric</strong> to Supervised Release Rate and <strong>Geography Level</strong> to State, District or PO Office.",
                "Use <strong>Year Selection Mode</strong> to look at one fiscal year or a range. The map and the ranked bar chart update together.",
                "Hover over the map or a bar for the exact rate. For rates, the National Total bar is outlined in gold."),
              tip = "The Supervised Release Rate divides by people sentenced to prison. Choose Unfiltered Supervised Release Rate to divide by everyone sentenced instead.",
              caution = "Small districts and offices can show extreme rates. Check the counts in the table below the map before drawing conclusions."),
      
      ab_card("Compare one state with the nation", "State Dashboard",
              c("Choose a <strong>State</strong>, an <strong>Outcome Metric</strong> (Supervised Release Rate, Unfiltered Supervised Release Rate or Probation Rate) and a fiscal year or range.",
                "The bar charts compare the state with the national total by race and ethnicity, by gender and by offense category.",
                "In range mode, the line chart at the bottom follows the state and the national rate year by year."),
              tip = "Bars are sorted by the selected state's rate, so the biggest gaps between a group's rate and the nation's are easy to spot."),
      
      ab_card("Follow national trends", "National Dashboard",
              c("Use the <strong>Fiscal Year</strong> slider to choose the period.",
                "The upper chart plots the national Supervised Release Rate and Probation Rate for each year.",
                "The lower chart compares both rates across offense categories, combined over the years you chose."),
              tip = "The Overview page shows the same national rates. The Modeling tab looks at what is linked to the differences."),
      
      ab_card("Focus on one group of people", "Main Dashboard or Custom Visualization &rsaquo; sidebar filters",
              c("Narrow by <strong>Age Range</strong>, <strong>Race</strong>, <strong>Education</strong>, <strong>Gender</strong>, <strong>Offense Category</strong>, <strong>Sentence Type</strong> or <strong>Citizenship Status</strong>.",
                "Pick one or several values in a filter. Leave it on All to include everyone.",
                "Rates and counts are recalculated for the people who match all of your filters."),
              tip = "To explore a pattern from the modeling results, such as the gap between U.S. citizens and non-citizens, run the same view twice with different Citizenship Status filters.",
              caution = "Filtering to a small group can leave only a handful of people in some places. Rates for such places are not stable."),
      
      ab_card("Pull numbers for a report", "Main Dashboard &rsaquo; table",
              c("Set the filters and Geography Level you need. The table lists every state, district or office with its counts and rates.",
                "Click a column header to sort, and use the boxes at the top of each column to search.",
                "Use <strong>Copy</strong>, <strong>CSV</strong> or <strong>Excel</strong> to export the table."),
              tip = "The table shows counts beside the rates, so you can judge how many people sit behind each rate."),
      
      ab_card("Build your own chart", "Custom Visualization",
              c("Choose a <strong>Chart Type</strong> (bar, line, boxplot or violin plot) and one or more <strong>Outcome Metrics</strong>.",
                "Pick what to <strong>Group By</strong>, and optionally up to two <strong>Facet By</strong> variables. Set the filters on the left.",
                "Click <strong>Generate Plot</strong>. Use <strong>Download Plot</strong> or <strong>Download Data</strong> to save the result."),
              tip = "For boxplots and violin plots, <strong>Aggregate By</strong> sets the units whose spread is shown, for example districts within each offense category.",
              caution = "Changing any setting clears the chart. Click Generate Plot again to redraw it. A line chart with several outcomes groups by fiscal year automatically.")
  )

##Key terms
ab_glossary <-
  tagList(
    div(class = "ab-glossary-search",
        tags$input(type = "search", id = "glossary-filter", class = "form-control",
                   placeholder = "Filter terms (for example: probation, odds ratio, ICC)", `aria-label` = "Filter key terms")),
    
    ab_term_group("Sentences", list(
      list("Supervised release", "A period of supervision by a U.S. probation officer after a person leaves prison. Federal law replaced parole with supervised release. The judge sets it at sentencing, as a term separate from the prison sentence, and some offenses require it by statute."),
      list("Probation", "A sentence served in the community, under a probation officer, instead of a prison term. It can come with conditions such as home confinement or community confinement."),
      list("Prison sentence", "A sentence of imprisonment. In this dashboard, people sentenced to prison are the group that can receive supervised release. It includes prison only and prison combined with confinement conditions, such as some split sentences."),
      list("Sentence type", "The Sentencing Commission's classification of the sentence imposed: Prison Only, Prison plus Confinement Conditions (including Zone C split sentences), Probation Only, Probation plus Confinement Conditions, or No Prison/Probation (fine only)."),
      list("U.S. probation officer", "Court employees in each district's probation office who supervise people on probation and supervised release, check compliance with court-ordered conditions, and help people return to the community.")
    )),
    
    ab_term_group("Measures", list(
      list("Supervised Release Count", "The number of people sentenced to prison who also received a term of supervised release. Labeled Supervised Release in the Outcome Metric list."),
      list("Probation Sentence Count", "The number of people given a term of probation. This counts people sentenced to probation only or probation with confinement conditions, and people with a probation term in a sentence that was not prison only. Labeled Probationers in the Outcome Metric list."),
      list("Total Sentenced", "Everyone in the data: all people sentenced in the selected years, places and groups."),
      list("Total Sentenced Eligible for FSR", "People sentenced to prison, who are the people eligible for a term of supervised release. This is the denominator of the Supervised Release Rate. The dashboard labels this group Eligible for FSR."),
      list("Supervised Release Rate", "Supervised Release Count divided by Total Sentenced Eligible for FSR. The share of people sentenced to prison who also received supervised release."),
      list("Unfiltered Supervised Release Rate", "Supervised Release Count divided by Total Sentenced, so it also counts people who were not sentenced to prison in the denominator. It is lower than the Supervised Release Rate."),
      list("Probation Rate", "Probation Sentence Count divided by Total Sentenced. Shown as Probation Sentence Rate in the table and map legend."),
      list("National Total", "The figure for the whole country, shown as a row in tables and a highlighted bar on rate charts.")
    )),
    
    ab_term_group("People and places", list(
      list("Race", "The dashboard uses the Sentencing Commission's combined race and ethnicity grouping: White, Black, Hispanic and Other. Hispanic is its own group, so people of Hispanic origin are not also counted under White or Black. Blank means the information was not available."),
      list("Citizenship Status", "Whether the person is a U.S. citizen, and for non-citizens the type of status: Resident or Legal Alien, Illegal Alien, Extradited Alien, or Not a U.S. Citizen with Alien Status Unknown."),
      list("Offense Category", "A grouping of the primary offense: Drug Trafficking, Firearms, Fraud, Immigration, Sexual Offense, Violent Offense, or Other. See the Data dictionary for how the groups are built."),
      list("Judicial district", paste0("A federal trial-court jurisdiction. The data include ", ov_n_districts_all, " districts. Each state has between one and four, and the District of Columbia and the territories have one each.")),
      list("PO Office", paste0("A U.S. probation office. The data include ", ov_n_offices, " offices with a known location, which the map shows as points.")),
      list("State", paste0("The State level includes the 50 states, the District of Columbia, Guam, Puerto Rico, the Virgin Islands and the Northern Mariana Islands (", ov_n_states, " in all).")),
      list("Fiscal year (FY)", "A federal fiscal year runs from October 1 to September 30 and is named for the year in which it ends. For example, FY2025 covers October 1, 2024 to September 30, 2025.")
    )),
    
    ab_term_group("Modeling terms", list(
      list("Reference group", "The group an odds ratio is measured against. For example, Hispanic defendants are compared with White defendants, so White is the reference group. The reference groups in the modeling results are U.S. citizen, male, White, less than H.S. graduate, drug trafficking and FY2014."),
      list("Odds and odds ratio (OR)", "Odds compare the chance an outcome happens with the chance it does not (for example, 3 to 1). An odds ratio compares the odds of two groups. An OR of 1 means no difference, 0.5 means half the odds, and 2 means twice the odds. Odds are not the same as probabilities, but they move in the same direction."),
      list("Mixed-effects (multilevel) logistic regression", "A statistical model for yes or no outcomes that estimates the link between each characteristic and the outcome while holding the others constant. It also lets each district and each state have its own baseline, so places can differ even when the people sentenced are alike."),
      list("Random intercept", "A separate baseline level for each state and each district within a state. Differences between these baselines show how much local practice varies after accounting for the characteristics in the model."),
      list("ICC (intraclass correlation)", "The share of the leftover variation in an outcome, after accounting for individual characteristics, that sits at a group level such as the district or state. A higher ICC means outcomes depend more on where someone is sentenced. The reports calculate it on the model's logit scale."),
      list("AR1 (autoregressive) term", "A way to let each district's rate drift from year to year with each year related to the one before. It captures district trends that a single national trend would miss."),
      list("Complete-case analysis", "The main model, which uses only people with no missing values on the variables it includes."),
      list("Missing as its own category", "A check that keeps people with missing values by treating missing as another category, so they are not dropped."),
      list("Multiple imputation (MICE)", "A check that fills in missing values several times using the other information on each person, fits the model to each filled-in dataset, and combines the results. The reports used five datasets."),
      list("Confidence interval (CI)", "The range of values that is consistent with the data. The charts show 95% intervals. If an odds ratio's interval does not include 1, the difference is statistically distinguishable from no difference.")
    ))
  )

##Data dictionary
ab_dict_measures <-
  tribble(
    ~`Dashboard measure`, ~`How it is built`, ~`Denominator`,
    "Supervised Release Count", "Sentence type is prison only or prison plus confinement conditions, and a supervised release term was imposed.", "Not applicable (a count)",
    "Probation Sentence Count", "Sentence type is probation only or probation plus confinement conditions, or a probation term was imposed in a sentence that was not prison only.", "Not applicable (a count)",
    "Total Sentenced", "Every person in the data (one row per person before grouping).", "Not applicable (a count)",
    "Total Sentenced Eligible for FSR", "Sentence type is prison only or prison plus confinement conditions.", "Not applicable (a count)",
    "Supervised Release Rate", "Supervised Release Count divided by Total Sentenced Eligible for FSR.", "People sentenced to prison",
    "Unfiltered Supervised Release Rate", "Supervised Release Count divided by Total Sentenced.", "Everyone sentenced",
    "Probation Rate", "Probation Sentence Count divided by Total Sentenced.", "Everyone sentenced"
  )

ab_dict_fields <-
  tribble(
    ~Field, ~Meaning, ~`Values in the data`,
    "fiscal_year", "Federal fiscal year of the sentencing (October to September).", paste0(ov_fy_first, " to ", ov_fy_latest),
    "age", "Age of the person at sentencing, in years.", paste0(ov_age_range[1], " to ", ov_age_range[2]),
    "geo_level_type", "Which geography the row is summarized at.", "state_name, state_district, po_office",
    "geo_level", "The place itself: a state or territory, a judicial district, or a probation office. A National Total row is included for each group.", "For example Texas, Texas Northern",
    "newrace_description", "Combined race and ethnicity grouping.", "White, Black, Hispanic, Other, blank",
    "neweduc_description", "Highest education level.", "Less Than H.S. Graduate, H.S. Graduate, Some College, College Graduate, blank",
    "monsex_description", "Gender as coded by the Sentencing Commission.", "Male, Female, blank",
    "citizen_description", "Citizenship status in detail.", "U.S. Citizen, Resident/Legal Alien, Illegal Alien, Extradited Alien, Not a U.S. Citizen/Alien Status Unknown, blank",
    "newcit_description", "Citizenship status, grouped into two categories.", "U.S. Citizen, Non-U.S. Citizen, blank",
    "sentence_type_description", "Type of sentence imposed.", "Prison Only; Prison + Confinement Conditions; Probation Only; Probation + Confinement Conditions; No Prison/Probation (Fine Only); blank",
    "offguide_description", "Primary offense guideline, the detailed offense that drives the sentence.", "Many values, for example Drug Trafficking, Murder",
    "offense_category", "Grouping of the primary offense guideline (see below).", "Drug Trafficking, Firearms, Fraud, Immigration, Sexual Offense, Violent Offense, Other",
    "supervised_release_flag", "Number of people in the row who received supervised release.", "Count",
    "probation_sentence_flag", "Number of people in the row who received probation.", "Count",
    "total_sentenced", "Number of people in the row.", "Count",
    "total_sentenced_fsr", "Number of people in the row sentenced to prison (eligible for supervised release).", "Count"
  )

ab_dict_offense <-
  tribble(
    ~`Offense Category`, ~`Primary offense guidelines included`,
    "Drug Trafficking", "Drug Trafficking and Drug Possession",
    "Firearms", "Firearms",
    "Fraud", "Fraud/Theft/Embezzlement",
    "Immigration", "Immigration",
    "Sexual Offense", "Child Pornography and Sex Abuse",
    "Violent Offense", "Assault, Kidnapping, Manslaughter, Murder, Robbery and Stalking/Harassing",
    "Other", "All other guidelines"
  )

ab_dict_table <-
  tribble(
    ~Column, ~Meaning,
    "State, District or PO Office", "The place. Which column appears depends on the Geography Level you choose.",
    "Supervised Release Count", "People sentenced to prison who also received supervised release.",
    "Probation Sentence Count", "People given probation.",
    "Total Sentenced", "Everyone sentenced.",
    "Total Sentenced Eligible for FSR", "People sentenced to prison.",
    "Supervised Release Rate", "Supervised Release Count divided by Total Sentenced Eligible for FSR.",
    "Unfiltered Supervised Release Rate", "Supervised Release Count divided by Total Sentenced.",
    "Probation Sentence Rate", "Probation Sentence Count divided by Total Sentenced."
  )

ab_dictionary <-
  tagList(
    h3("What the dashboards measure"),
    p(class = "ab-lead", "Every chart and table is built from the three counts and the rates calculated from them."),
    ab_table(ab_dict_measures),
    
    h3("Fields in the dashboard data"),
    p(class = "ab-lead", paste0("The data behind the dashboards are counts of people. Each row is a group of people who share the same age, fiscal year, offense, education, race, citizenship, gender, sentence type and place. ",
                                "The files are in data-raw/aggregated-ussc-sentencing-data, one per geography and fiscal year.")),
    ab_table(ab_dict_fields, code_cols = "Field"),
    
    h3("Columns in the downloadable table"),
    p(class = "ab-lead", "These are the columns you can copy or export from the table on the Main Dashboard."),
    ab_table(ab_dict_table),
    
    h3("How offense categories are built"),
    p(class = "ab-lead", "Offense Category groups the primary offense guideline recorded by the Sentencing Commission. Every guideline not listed below falls under Other. The mapping is in R/parse-federal-supervised-release-data-clean.R."),
    ab_table(ab_dict_offense)
  )

##Sources and caveats
ab_sources <-
  tagList(
    h3("Where the data come from"),
    tags$ul(
      tags$li(HTML("<strong>Sentencing data:</strong> the U.S. Sentencing Commission's "),
              ab_link("https://www.ussc.gov/research/datafiles/commission-datafiles", "individual offender datafiles"),
              " for fiscal years 2014 to 2025. The Commission receives sentencing documents from the federal courts and codes them. Supplemental files and criminal history files are also stored in the project database."),
      tags$li(HTML("<strong>Place information:</strong> the district and probation office for each case come from the Commission's records. Map boundaries come from a U.S. District Courts shapefile for districts and from the Census Bureau (through the tigris package) for states. Office locations come from a locations file in the project repository.")),
      tags$li(HTML(paste0("<strong>Code and data preparation:</strong> ", ab_link_html(ov_code_url, "the project's GitHub repository"), "."))),
      tags$li(HTML(paste0("<strong>Modeling results:</strong> summarized on the Modeling tab, with two full reports: ", ab_link_html(ov_report_sr_url, "Supervised Release Analysis Report"), " and ", ab_link_html(ov_report_pb_url, "Probation Analysis Report"), " (PDFs).")))
    ),
    
    h3("Things to keep in mind"),
    tags$ol(class = "ab-caveats",
            tags$li(HTML("<strong>Rates use different denominators.</strong> The Supervised Release Rate counts only people sentenced to prison. The Unfiltered Supervised Release Rate and the Probation Rate count everyone sentenced.")),
            tags$li(HTML("<strong>Supervised release and probation are not opposites.</strong> A person with a prison sentence that also includes a probation term can be counted in both. The two rates should not be added together.")),
            tags$li(HTML("<strong>Blank values.</strong> When race, education, gender, citizenship or sentence type is blank in the source, the row is kept. It is included when a filter is set to All and excluded when you pick specific values. The State Dashboard labels these Unknown/Unreported and the Custom Visualization labels them Missing/Not Applicable.")),
            tags$li(HTML("<strong>Small places can swing.</strong> Rates for small districts, probation offices and small groups can change sharply with a few cases. Territories are included in the State level.")),
            tags$li(HTML(paste0("<strong>Static findings.</strong> The odds ratios and ICCs on the Modeling tab come from the 2026 reports and are fixed values (", fy_lbl(mod_model_fy[1]), " to ", fy_lbl(mod_model_fy[2]),
                                "). They do not change when you apply filters, and they need to be re-estimated when new fiscal years are added."))),
            tags$li(HTML("<strong>These are counts of people, not measures of crime or fairness.</strong> Differences between places and groups reflect many factors, including the mix of federal cases, prosecution priorities, local practices and the data's coverage."))
    )
  )

about_ui <-
  div(class = "ab-page",
      div(class = "ab-hero",
          h1("About this dashboard"),
          p(class = "ab-lede",
            "Explore federal sentencing data from the U.S. Sentencing Commission, with a focus on supervised release and probation. Use the sections below to find the right view for your question, ",
            "look up unfamiliar terms, and see exactly what each number measures. The modeling results are on their own tab.")),
      tabsetPanel(
        id = "about_tabs",
        type = "pills",
        tabPanel("Ways to use it", div(class = "ab-section", ab_use_cards)),
        tabPanel("Key terms", div(class = "ab-section", ab_glossary)),
        tabPanel("Data dictionary", div(class = "ab-section", ab_dictionary)),
        tabPanel("Sources and caveats", div(class = "ab-section", ab_sources))
      )
  )


##Styles and Scripts for the Overview and About Pages
overview_about_css <- "
      /* --- OVERVIEW & ABOUT PAGES --- */
      .ov-page, .ab-page {
        max-width: 1180px;
        margin: 0 auto;
        padding: 24px 20px 64px;
        color: #2C2C2C;
      }
      .ov-page a:not(.ov-card-link), .ab-page a:not(.nav-link) {
        color: #00785A;
        text-decoration: underline;
      }
      .ov-hero h1, .ab-hero h1 {
        font-size: 2rem;
        font-weight: 700;
        line-height: 1.2;
        margin: 0 0 14px;
      }
      .ov-lede, .ab-lede {
        font-size: 1.15rem;
        line-height: 1.55;
        max-width: 860px;
        margin-bottom: 12px;
      }
      .ov-body {
        font-size: 1rem;
        line-height: 1.6;
        max-width: 860px;
        color: #3b3b3b;
        margin-bottom: 12px;
      }
      .ov-asof {
        display: inline-block;
        font-size: 0.85rem;
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 999px;
        padding: 4px 12px;
      }

      /* Headline numbers */
      .ov-kpis {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 16px;
        margin: 28px 0 8px;
      }
      .ov-kpi {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-top: 4px solid #009E73;
        border-radius: 8px;
        padding: 14px 16px;
      }
      .ov-kpi.pb {
        border-top-color: #0072B2;
      }
      .ov-kpi.pb .ov-kpi-value {
        color: #005A8D;
      }
      .ov-kpi-label {
        font-size: 0.78rem;
        font-weight: 500;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: #5c5c5c;
      }
      .ov-kpi-value {
        font-size: 2.1rem;
        font-weight: 700;
        line-height: 1.15;
        color: #00785A;
        margin: 2px 0;
      }
      .ov-kpi-note {
        font-size: 0.85rem;
        line-height: 1.4;
        color: #555;
      }

      /* Trend blocks */
      .ov-section-title {
        font-size: 1.5rem;
        font-weight: 700;
        margin: 40px 0 6px;
      }
      .ov-section-sub {
        color: #555;
        margin-bottom: 16px;
      }
      .ov-trend {
        display: grid;
        grid-template-columns: minmax(0, 5fr) minmax(0, 7fr);
        gap: 28px;
        align-items: start;
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 22px;
        margin-bottom: 20px;
      }
      .ov-trend h3 {
        font-size: 1.2rem;
        font-weight: 700;
        line-height: 1.3;
        margin: 0 0 8px;
      }
      .ov-trend h3:not(:first-child) {
        margin-top: 22px;
      }
      .ov-trend p {
        line-height: 1.6;
        margin-bottom: 10px;
      }
      .ov-trend-chart {
        min-width: 0;
      }
      .ov-plot-scroll {
        overflow-x: auto;
      }
      .ov-scroll-hint {
        display: none;
        font-size: 0.78rem;
        color: #666;
        margin: 0 0 6px;
      }
      .ov-caption {
        font-size: 0.78rem;
        line-height: 1.45;
        color: #666;
        margin-top: 6px;
      }

      /* Links to the dashboards */
      .ov-explore {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 16px;
        margin-top: 12px;
      }
      .ov-card-link {
        display: block;
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 8px;
        padding: 16px;
        text-decoration: none !important;
        color: #2C2C2C !important;
        transition: border-color 0.15s, box-shadow 0.15s;
      }
      .ov-card-link:hover, .ov-card-link:focus {
        border-color: #009E73;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.08);
      }
      .ov-card-link .t {
        display: block;
        font-weight: 700;
        color: #00785A;
        margin-bottom: 4px;
      }
      .ov-card-link .d {
        display: block;
        font-size: 0.9rem;
        line-height: 1.45;
      }
      .ov-notes {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 16px 22px;
        margin-top: 32px;
      }
      .ov-notes h3 {
        font-size: 1.05rem;
        font-weight: 700;
        margin: 0 0 8px;
      }
      .ov-notes ul {
        padding-left: 1.2rem;
        margin-bottom: 8px;
      }
      .ov-notes li {
        line-height: 1.55;
        margin-bottom: 4px;
      }

      /* About page */
      .ab-page .nav-pills {
        margin-bottom: 8px;
      }
      .ab-page .nav-pills .nav-link {
        border-radius: 999px;
        font-weight: 500;
        color: #00785A;
      }
      .ab-page .nav-pills .nav-link.active {
        background-color: #00785A;
        color: #fff;
      }
      .ab-section {
        padding-top: 14px;
      }
      .ab-section h3 {
        font-size: 1.15rem;
        font-weight: 700;
        margin: 28px 0 8px;
      }
      .ab-section h3:first-child {
        margin-top: 0;
      }
      .ab-lead {
        color: #555;
        max-width: 860px;
      }
      .ab-card-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
        gap: 18px;
      }
      .ab-card {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 18px 18px 14px;
      }
      .ab-card h3 {
        font-size: 1.1rem;
        margin: 0 0 8px;
      }
      .ab-where {
        display: inline-block;
        font-size: 0.8rem;
        font-weight: 500;
        background: #E6F4EF;
        color: #00583F;
        border-radius: 999px;
        padding: 3px 10px;
        margin-bottom: 10px;
      }
      .ab-card ol {
        padding-left: 1.2rem;
        margin-bottom: 10px;
      }
      .ab-card li {
        line-height: 1.5;
        margin-bottom: 6px;
      }
      .ab-tip, .ab-caution {
        font-size: 0.9rem;
        line-height: 1.5;
        padding: 8px 10px;
        border-radius: 6px;
        margin-top: 8px;
      }
      .ab-tip {
        background: #F1F8F5;
        border-left: 4px solid #009E73;
      }
      .ab-caution {
        background: #FFF3DF;
        border-left: 4px solid #D55E00;
      }
      .ab-glossary-search {
        max-width: 520px;
        margin-bottom: 8px;
      }
      .ab-term-group h3 {
        padding-bottom: 6px;
        border-bottom: 2px solid #E6DCC3;
      }
      .ab-term {
        margin: 0;
        padding: 10px 0;
        border-bottom: 1px solid #EFE6CF;
      }
      .ab-term dt {
        font-weight: 700;
      }
      .ab-term dd {
        margin: 2px 0 0;
        line-height: 1.55;
        max-width: 860px;
      }
      .ab-table-wrap {
        overflow-x: auto;
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 8px;
        margin-bottom: 8px;
      }
      .ab-table {
        margin: 0;
        font-size: 0.9rem;
      }
      .ab-table th {
        background: #F6EED8;
        font-weight: 700;
        white-space: nowrap;
      }
      .ab-table td {
        vertical-align: top;
      }
      .ab-table code {
        color: #8A3B00;
        background: transparent;
        padding: 0;
      }
      .ab-caveats li, .ab-section ul li {
        line-height: 1.6;
        margin-bottom: 8px;
        max-width: 860px;
      }

      /* Notes that tie the charts to the 2026 analysis */
      .ov-model-note {
        font-size: 0.92rem;
        line-height: 1.55;
        background: #F1F8F5;
        border-left: 4px solid #009E73;
        border-radius: 6px;
        padding: 10px 12px;
        margin: 14px 0 0;
      }
      .ov-badge {
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: #00583F;
        background: #D6EDE4;
        border-radius: 999px;
        padding: 1px 8px;
        margin-right: 2px;
      }
      .ov-or-list {
        padding-left: 1.1rem;
        margin: 0 0 6px;
      }
      .ov-or-list li {
        line-height: 1.55;
        margin-bottom: 8px;
      }
      .ov-work {
        background: #F6EED8;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 18px 22px;
        margin-bottom: 8px;
      }
      .ov-work h3 {
        font-size: 1.2rem;
        font-weight: 700;
        margin: 0 0 8px;
      }
      .ov-work p {
        line-height: 1.55;
        max-width: 860px;
      }
      .ov-small {
        font-size: 0.85rem;
        color: #555;
        margin-bottom: 0;
      }

      .ov-page a.ov-btn {
        display: inline-block;
        background: #00785A;
        color: #fff !important;
        font-weight: 700;
        border-radius: 999px;
        padding: 8px 18px;
        text-decoration: none !important;
      }
      .ov-page a.ov-btn:hover, .ov-page a.ov-btn:focus {
        background: #00583F;
      }

      /* --- MODELING SUPERVISION RATES PAGE --- */
      .mod-page .ab-section h2 {
        font-size: 1.45rem;
        font-weight: 700;
        margin: 0 0 6px;
      }
      .mod-page .ab-section h2.mod-gap {
        margin-top: 38px;
      }
      .mod-page .ab-hero .ab-caution {
        max-width: 860px;
        margin: 12px 0 4px;
      }
      .mod-links {
        display: flex;
        flex-wrap: wrap;
        gap: 8px 22px;
        margin: 12px 0 6px;
        font-weight: 500;
      }
      .mod-card {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 20px 22px 12px;
      }
      .mod-card h3 {
        margin-top: 0;
      }
      .mod-card p {
        line-height: 1.6;
        max-width: 860px;
      }
      .mod-chart {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 10px;
        padding: 16px 18px 12px;
        margin: 14px 0 18px;
        min-width: 0;
      }
      .mod-chart-title {
        font-size: 1rem;
        font-weight: 700;
        margin: 0 0 6px;
      }
      .mod-chart-pair {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 20px;
        margin: 14px 0 18px;
        align-items: start;
      }
      .mod-chart-pair .mod-chart {
        margin: 0;
      }
      .mod-tiles {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 16px;
        margin: 14px 0;
        max-width: 900px;
      }
      .mod-short {
        background: #F1F8F5;
        border: 1px solid #CFE8DE;
        border-radius: 10px;
        padding: 14px 18px 4px;
        margin: 14px 0 6px;
        max-width: 900px;
      }
      .mod-short h3 {
        font-size: 1.05rem;
        margin: 0 0 6px;
      }
      .mod-short ul {
        padding-left: 1.2rem;
      }
      .mod-finding {
        display: grid;
        grid-template-columns: 175px minmax(0, 1fr);
        gap: 20px;
        background: #fff;
        border: 1px solid #E6DCC3;
        border-left: 5px solid #009E73;
        border-radius: 8px;
        padding: 14px 18px 8px;
        margin-bottom: 12px;
      }
      .mod-finding.pb {
        border-left-color: #0072B2;
      }
      .mod-finding.caution {
        border-left-color: #D55E00;
        background: #FFFBF3;
      }
      .mod-finding-num .n {
        display: block;
        font-size: 1.9rem;
        font-weight: 700;
        line-height: 1.1;
        color: #00785A;
      }
      .mod-finding.pb .mod-finding-num .n {
        color: #005A8D;
      }
      .mod-finding.caution .mod-finding-num .n {
        color: #A04400;
      }
      .mod-finding-num .c {
        display: block;
        font-size: 0.78rem;
        line-height: 1.35;
        color: #555;
        margin-top: 4px;
      }
      .mod-finding-body h4 {
        font-size: 1.05rem;
        font-weight: 700;
        line-height: 1.35;
        margin: 0 0 6px;
      }
      .mod-finding-body p {
        line-height: 1.55;
        max-width: 780px;
        margin-bottom: 8px;
      }
      .mod-flag {
        display: inline-block;
        font-size: 0.78rem;
        font-weight: 700;
        border-radius: 999px;
        padding: 1px 9px;
        white-space: nowrap;
      }
      .mod-flag-ok   { background: #D6EDE4; color: #00583F; }
      .mod-flag-note { background: #E1EDF7; color: #0B4F7A; }
      .mod-flag-warn { background: #FFEBC9; color: #7A4300; }
      .mod-flag-bad  { background: #FADBC8; color: #7A2A00; }
      .mod-details {
        background: #fff;
        border: 1px solid #E6DCC3;
        border-radius: 8px;
        padding: 10px 14px;
        margin: 22px 0 8px;
      }
      .mod-details summary {
        font-weight: 700;
        cursor: pointer;
      }
      .mod-details[open] summary {
        margin-bottom: 10px;
      }

      @media (max-width: 860px) {
        .ov-trend { grid-template-columns: 1fr; }
        .mod-chart-pair { grid-template-columns: 1fr; }
      }
      @media (max-width: 576px) {
        .ov-scroll-hint { display: block; }
        .ov-hero h1, .ab-hero h1 { font-size: 1.6rem; }
        .ab-card-grid { grid-template-columns: 1fr; }
        .mod-tiles { grid-template-columns: 1fr; }
        .mod-finding { grid-template-columns: 1fr; gap: 8px; }
      }
"

overview_about_js <- "
  // Jump back to the top when switching between the main tabs
  $(document).on('shown.bs.tab', '.navbar a[data-toggle=tab]', function(){
    window.scrollTo(0, 0);
  });
  // Filter the key terms list as the user types
  $(document).on('input', '#glossary-filter', function(){
    var q = $(this).val().toLowerCase().trim();
    $('.ab-term-group').each(function(){
      var anyShown = false;
      $(this).find('.ab-term').each(function(){
        var show = (q === '' || $(this).text().toLowerCase().indexOf(q) !== -1);
        $(this).toggle(show);
        if(show) anyShown = true;
      });
      $(this).toggle(anyShown);
    });
  });
"


####UI####
ui <- navbarPage(
  "USSC Federal Sentencing Dashboard",
  id = "main_nav",
  selected = "Overview",
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
      
      .dataTables_wrapper {
        width: 100% !important;
      }
      
      .dataTables_scroll {
        width: 100% !important;
      }
      
      .dataTables_scrollBody {
        width: 100% !important;
      }
  ")),
              tags$style(HTML(overview_about_css)),
              tags$script(HTML(overview_about_js))),
  
  # ---- Overview (landing page) ----
  tabPanel("Overview", overview_ui),
  
  # ---- About ----
  tabPanel("About", about_ui),
  
  # ---- Modeling Supervision Rates ----
  tabPanel("Modeling Supervision Rates", modeling_ui),
  
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
                                           "Supervised Release Rate"   = "supervised_release_rate",
                                           "Unfiltered Supervised Release Rate" = "unfiltered_supervised_release_rate"),
                               selected = "supervised_release_rate"),
                   selectInput("geo_level", "Geography Level",
                               choices = c("State"    = "state_name",
                                           "District" = "state_district",
                                           "PO Office"= "po_office")),
                   sliderInput("age", "Age Range",
                               min = min(supervision_df$age, na.rm = TRUE),
                               max = max(supervision_df$age, na.rm = TRUE),
                               value = range(supervision_df$age, na.rm = TRUE)),
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
                               choices = c("All", sort(unique(supervision_df$offense_category))),
                               multiple = TRUE, selected = "All"),
                   selectInput("sentence", "Sentence Type",
                               choices = c("All", sort(unique(supervision_df$sentence_type_description))),
                               multiple = TRUE, selected = "All"),
                   selectInput("citizen", "Citizenship Status", 
                               choices = c("All", sort(unique(supervision_df$citizen_description))),
                               multiple = TRUE, selected = "All"),
                   div(class = "reporting-note", "Note: Rates shown as share of total sentenced.")
                 ),
                 mainPanel(
                   width = 10,
                   fluidRow(
                     box(width = 7, leafletOutput("map", height = 500)),
                     box(width = 5, 
                         align = "left",
                         div(style = "width:95%; padding:0; margin:0; text-align:left;",
                             plotlyOutput("barplot", height = 500, width = "100%")
                         )
                     )
                   ),
                   fluidRow(box(width = 12, 
                                align = "left",
                                div(style = "width: 100%; padding: 0; margin: 0;",
                                    DTOutput("table"))
                   )
                   )
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
                               choices = states_list,
                               selected = "Alabama"),
                   selectInput("state_outcome", "Outcome Metric",
                               choices = c("Supervised Release Rate" = "supervised_release_rate",
                                           "Unfiltered Supervised Release Rate" = "unfiltered_supervised_release_rate",
                                           "Probation Rate"          = "probation_rate"),
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
                                       # "Scatterplot (needs 2 outcomes)" = "scatter",
                                       "Violin Plot"                    = "violinplot",
                                       "Boxplot"                        = "boxplot")),
               selectInput("custom_outcome", "Outcome Metric(s)",
                           choices = c("Probationers"           = "probation_sentence_flag",
                                       "Supervised Release"      = "supervised_release_flag",
                                       "Total Sentenced"         = "total_sentenced",
                                       "Probation Rate"          = "probation_rate",
                                       "Supervised Release Rate" = "supervised_release_rate", 
                                       "Unfiltered Supervised Release Rate" = "unfiltered_supervised_release_rate"),
                           selected = "probation_sentence_flag", multiple = TRUE),
               uiOutput("custom_group_ui"),
               uiOutput("custom_variance_ui"),
               selectInput("custom_facet", "Facet By (max 2)",
                           choices = c("State"            = "state_name",
                                       "District"         = "state_district",
                                       "PO Office"        = "po_office",
                                       "Race"             = "newrace_description",
                                       "Education"        = "neweduc_description",
                                       "Gender"           = "monsex_description",
                                       "Offense Category" = "offense_category",
                                       "Fiscal Year"      = "fiscal_year", 
                                       "Citizenship Status" = "citizen_description"),
                           multiple = TRUE, selectize = TRUE),
               sliderInput("custom_age", "Age Range",
                           min = min(supervision_df$age, na.rm = TRUE),
                           max = max(supervision_df$age, na.rm = TRUE),
                           value = range(supervision_df$age, na.rm = TRUE)),
               sliderInput("custom_year", "Fiscal Year",
                           min = min(supervision_df$fiscal_year, na.rm = TRUE),
                           max = max(supervision_df$fiscal_year, na.rm = TRUE),
                           value = range(supervision_df$fiscal_year, na.rm = TRUE), sep = "", step = 1),
               selectInput("custom_state", "State",
                           choices = c("All", states_list),
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
                           choices = c("All", sort(unique(supervision_df$offense_category))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_sentence", "Sentence Type",
                           choices = c("All", sort(unique(supervision_df$sentence_type_description))),
                           multiple = TRUE, selected = "All"),
               selectInput("custom_citizen", "Citizenship Status",
                           choices = c("All", sort(unique(supervision_df$citizen_description))),
                           multiple = TRUE, selected = "All"),
               actionButton("generate_plot", "Generate Plot",
                            class = "btn-success", style = "width:100%; margin-top:8px;"),
               downloadButton("download_custom_plot", "Download Plot"),
               downloadButton("download_custom_data", "Download Data")
             ),
             mainPanel(
               width = 10,
               fluidRow(box(width = 12, plotlyOutput("custom_plot", height = 700))),
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
  
  # plot_reset <- reactiveVal(0)
  
  # ---- Shared filter helper ----
  apply_filters <- function(df, age_inp, year_inp, race_inp, educ_inp, gender_inp, 
                            offense_inp, sentence_inp, geo_level_inp, citizen_inp) {
    df %>%
      filter(
        between(age, age_inp[1], age_inp[2]),
        between(fiscal_year, year_inp[1], year_inp[2]),
        ("All" %in% race_inp    | newrace_description    %in% race_inp),
        ("All" %in% educ_inp    | neweduc_description    %in% educ_inp),
        ("All" %in% gender_inp  | monsex_description     %in% gender_inp),
        ("All" %in% offense_inp | offense_category   %in% offense_inp),
        ("All" %in% sentence_inp| sentence_type_description %in% sentence_inp), 
        ("All" %in% citizen_inp | citizen_description %in% citizen_inp),
        geo_level_type == geo_level_inp
      )
  }
  
  apply_filters_custom <- function(df, age_inp, year_inp, race_inp, educ_inp,
                                   gender_inp, offense_inp, sentence_inp, citizen_inp) {
    df %>%
      filter(
        between(age, age_inp[1], age_inp[2]),
        between(fiscal_year, year_inp[1], year_inp[2]),
        ("All" %in% race_inp    | newrace_description    %in% race_inp),
        ("All" %in% educ_inp    | neweduc_description    %in% educ_inp),
        ("All" %in% gender_inp  | monsex_description     %in% gender_inp),
        ("All" %in% offense_inp | offense_category   %in% offense_inp),
        ("All" %in% sentence_inp| sentence_type_description %in% sentence_inp),
        ("All" %in% citizen_inp | citizen_description %in% citizen_inp)
      )
  }
  
  # ---- Main Dashboard reactives ----
  filtered <- reactive({
    yr <- main_year_range()
    apply_filters(supervision_df, input$age, yr,
                  input$race, input$educ, input$gender,
                  input$offense, input$sentence, 
                  input$geo_level, input$citizen)
  })
  
  agg_data <- reactive({
    filtered() %>% 
      group_by(geo_level, geo_level_type) %>% 
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr,
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced)
  })
  
  # ---- Shared map builder ----
  build_map <- function(map_data, geo_col, geo_label_col, value_col,
                        is_rate, outcome_label, point_mode = FALSE,
                        lng_col = NULL, lat_col = NULL) {
    map_data <- 
      map_data %>%
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
    
    legend_fmt <- 
      if(is_rate){
        labelFormat(transform = function(x) x * 100, suffix = "%")
      }else{
        labelFormat()
      }
    
    m <- 
      map_data %>%
      leaflet() %>%
      add_basemap()
    
    if(point_mode){
      m <- 
        m %>%
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
    }else{
      m <- 
        m %>%
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
    if(input$year_mode_main == "single"){
      selectInput("single_year_main", "Fiscal Year",
                  choices = available_years_ussc,
                  selected = max(available_years_ussc))
    }else{
      sliderInput("fiscal_year", "Fiscal Year Range",
                  min   = min(supervision_df$fiscal_year, na.rm = TRUE),
                  max   = max(supervision_df$fiscal_year, na.rm = TRUE),
                  value = range(supervision_df$fiscal_year, na.rm = TRUE),
                  sep = "", step = 1)
    }
  })
  
  # State dashboard year UI
  output$year_selector_state <- renderUI({
    if(input$year_mode_state == "single"){
      selectInput("single_year_state", "Fiscal Year",
                  choices = available_years_ussc,
                  selected = max(available_years_ussc))
    }else{
      sliderInput("state_year", "Fiscal Year Range",
                  min   = min(supervision_df$fiscal_year, na.rm = TRUE),
                  max   = max(supervision_df$fiscal_year, na.rm = TRUE),
                  value = range(supervision_df$fiscal_year, na.rm = TRUE),
                  sep = "", step = 1)
    }
  })
  
  # Year range helpers
  main_year_range <- reactive({
    if(input$year_mode_main == "single"){
      req(input$single_year_main)
      rep(as.numeric(input$single_year_main), 2)
    }else{
      yr <- input$fiscal_year
      if (is.null(yr)) range(supervision_df$fiscal_year, na.rm = TRUE) else yr
    }
  })
  
  state_year_range <- reactive({
    if(input$year_mode_state == "single"){
      req(input$single_year_state)
      rep(as.numeric(input$single_year_state), 2)
    }else{
      yr <- input$state_year
      ifelse(is.null(yr) == TRUE, range(supervision_df$fiscal_year, na.rm = TRUE), yr)
    }
  })
  
  output$map <- renderLeaflet({
    df  <- agg_data()
    lbl <- clean_outcome_label(input$outcome)
    geo <- input$geo_level
    is_rate <- is_rate_outcome(input$outcome)
    value <- input$outcome
    
    if(geo == "state_name"){
      map_data <- 
        states_sf %>%
        left_join(df, by = c("state_name" = "geo_level"))
      
      build_map(map_data, "state_name", "state_name", value, is_rate, lbl)
    }else if(geo == "state_district"){
      map_data <- 
        judicial_sf %>% 
        left_join(df, by = c("state_district" = "geo_level"))
      
      build_map(map_data, "state_district", "state_district", value, is_rate, lbl)
    }else if(geo == "po_office"){
      map_data <- 
        po_office_df %>% 
        dplyr::select(po_office, lat, long) %>% 
        distinct() %>% 
        left_join(df, by = c("po_office" = "geo_level"))
      
      build_map(map_data, "po_office", "po_office", value, is_rate, lbl,
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
    
    df <- 
      df %>%
      mutate(across(outcome, 
                    ~.x %>% 
                      as.numeric()),
             hover_text = 
               ifelse(is_rate, 
                      paste0(.data[[geo_col]], ": ", scales::percent(.data[[outcome]], accuracy = 0.01)),
                      paste0(.data[[geo_col]], ": ", scales::comma(.data[[outcome]]))))
    
    p <- 
      df %>%
      mutate(geo_reorder = reorder(.data[[geo_col]], .data[[outcome]])) %>%
      ggplot(aes(x = .data[[outcome]], y = geo_reorder, fill = .data[[outcome]], text = hover_text)) +
      geom_col() +
      scale_fill_gradientn(
        colors = pal, 
        name = "",
        labels = ifelse(is_rate, percent_format(accuracy = 0.1), comma_format())
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
    
    if(is_rate){
      p <- 
        p + 
        scale_x_continuous(labels = percent_format(accuracy = 0.1))
    }else{
      p <- 
        p + 
        scale_x_continuous(labels = comma_format())
    }         
    
    plotly_obj <- 
      p %>%
      ggplotly(tooltip = "text") %>%
      layout(
        height     = height,
        margin     = list(l = 50, r = 10, t = 30, b = 50),
        autosize   = TRUE,
        showlegend = FALSE
      )
    
    # Only hide labels for PO offices (too many), show them for state_district
    if(geo_col == "po_office"){
      plotly_obj <- 
        plotly_obj %>%
        layout(yaxis = list(showticklabels = FALSE, showline = TRUE, showgrid = FALSE))
    }
    plotly_obj
  }
  
  output$barplot <- renderPlotly({
    
    is_rate <- is_rate_outcome(input$outcome)
    x_lab   <- clean_outcome_label(input$outcome)
    y_lab <- clean_geo_label(input$geo_level)
    
    if(is_rate == TRUE){
      df <- 
        agg_data() %>%
        mutate(across(c(input$outcome),
                      ~.x %>% 
                        as.numeric()))
    }else{
      df <- 
        agg_data() %>%
        mutate(across(c(input$outcome),
                      ~.x %>% 
                        as.numeric())) %>% 
        filter(geo_level != "National Total")
    }
    
    # Scale font size down as number of bars increases
    n_bars <- nrow(df)
    y_size <- 
      case_when(
        n_bars > 80 ~ 4,
        n_bars > 50 ~ 5,
        n_bars > 20 ~ 6,
        TRUE        ~ 8
      )
    
    ggplot_barplot_main <-
      df %>%
      mutate(geo_reorder = reorder(geo_level, .data[[input$outcome]]),
             is_rate_flag = is_rate, 
             hover_text =
               ifelse(is_rate_flag == TRUE,
                      paste0(geo_level, ": ", scales::percent(.data[[input$outcome]], accuracy = 0.01)),
                      paste0(geo_level, ": ", scales::comma(.data[[input$outcome]]))), 
             line_size = ifelse(geo_level == "National Total", "1", "0"), 
             color = ifelse(geo_level == "National Total", "1", "0")) %>%
      ggplot(aes(
        x    = .data[[input$outcome]],
        y    = geo_reorder,
        fill = .data[[input$outcome]], 
        text = hover_text, 
        color = color, 
        size = line_size
      )) +
      geom_col() +
      scale_color_manual(values = c("1" = "#FFBF00")) +
      scale_size_manual(values = c("1" = 1, "0" = 0)) +
      scale_fill_distiller(
        palette = "YlGn", 
        direction = 1, 
        name = NULL,
        labels = ifelse(is_rate, 
                        percent_format(accuracy = 0.1), 
                        comma_format())
      ) +
      labs(x = x_lab, y = y_lab) +
      theme_minimal() +
      theme(
        axis.text.y  = element_text(size = y_size, margin = margin(r = 0)),
        plot.margin  = margin(t = 0, r = 0, b = 0, l = 0),
        axis.title.y = element_text(margin = margin(r = 5)),
        axis.title.x = element_text(margin = margin(t = 5))
      )
    
    ifelse(is_rate, 
           ggplot_barplot_main <- 
             ggplot_barplot_main +
             scale_x_continuous(labels = percent_format(accuracy = 0.1)),
           ggplot_barplot_main <- 
             ggplot_barplot_main +
             scale_x_continuous(labels = comma_format()))
    
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
    df <-
      agg_data() %>%
      group_by(geo_level_type, geo_level) %>%
      mutate(group_id = cur_group_id()) %>%
      ungroup() %>%
      pivot_wider(names_from = "geo_level_type",
                  values_from = "geo_level") %>%
      dplyr::select(-group_id) %>%
      mutate(
        across(ends_with("rate"),
               ~as.numeric(.x) %>% round(4)),
        across(c(ends_with("_flag"), starts_with("total_sentenced")),
               ~as.numeric(.x))
      ) %>%
      relocate(any_of(c("state_name", "po_office", "state_district")), .before = 1) %>%
      arrange(desc(.data[[input$outcome]])) %>%
      rename_with(~.x %>%
                    clean_outcome_label() %>%
                    str_replace_all("Po Office", "PO Office") %>%
                    str_replace_all("State Name", "State") %>%
                    str_replace_all("State District", "District"))
    
    # Detect renamed column types for formatting
    rate_cols  <- names(df)[str_detect(names(df), "Rate")]
    count_cols <- names(df)[sapply(df, is.numeric) & !names(df) %in% rate_cols]
    
    datatable(
      df,
      extensions = "Buttons",
      class      = "compact stripe hover",
      filter     = "top",
      options    = list(
        dom      = "Bfrtpi",
        buttons  = c("copy", "csv", "excel"),
        paging   = FALSE,
        scrollY  = "400px",
        scrollX  = TRUE
      ),
      style    = "bootstrap",
      rownames = FALSE,
      width    = "100%"
    ) %>%
      formatPercentage(rate_cols,  digits = 1) %>%   
      formatRound(count_cols, digits = 0, mark = ",")
  })
  
  output$download_table <- downloadHandler(
    filename = function() paste0("USSC_table_", Sys.Date(), ".csv"),
    content  = function(file) {
      df <- filtered() %>%
        group_by(.data[[input$geo_level]]) %>%
        dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                                ~.x %>% 
                                  sum(na.rm = TRUE))) %>% 
        ungroup() %>% 
        mutate(supervised_release_rate = 
                 supervised_release_flag/total_sentenced_fsr,
               unfiltered_supervised_release_rate = 
                 supervised_release_flag/total_sentenced,
               probation_rate = 
                 probation_sentence_flag/total_sentenced,
               across(ends_with("rate"), ~round(.x, 3)))
      
      write_csv(df, file)
    }
  )
  
  # ---- State Dashboard ----
  state_data <- reactive({
    yr <- state_year_range()
    supervision_df %>%
      filter(geo_level_type == "state_name", 
             geo_level %in% c(input$state_sel, "National Total"),
             between(fiscal_year, yr[1], yr[2]))
  })
  
  # ---- State Dashboard Demographic Charts ----
  # ---- State Dashboard Demographic and Offense Charts ----
  output$state_demo_race <- renderPlotly({
    df <- 
      state_data() %>%
      group_by(newrace_description, geo_level) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr, 
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced, 
             newrace_description = 
               newrace_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>% 
      pivot_longer(cols = ends_with("_rate"), 
                   names_to = "rate_type", 
                   values_to = "rate") %>% 
      filter(rate_type == input$state_outcome) %>% 
      {
        df2 <- .
        df2 %>% 
          mutate(race_reorder = 
                   newrace_description %>% 
                   factor(levels = 
                            df2 %>% 
                            filter(geo_level == input$state_sel) %>% 
                            arrange(rate) %>% 
                            pull(newrace_description)))
      } %>% 
      mutate(hover_text = paste0(newrace_description, "\n", 
                                 geo_level, "\n", 
                                 clean_outcome_label(input$state_outcome), ": ", 
                                 scales::percent(rate, accuracy = 0.01)))
    
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      df %>% 
        ggplot(aes(x = rate, y = race_reorder, fill = geo_level, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = percent_format(accuracy = 0.1)) +
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
    df <- 
      state_data() %>%
      group_by(monsex_description, geo_level) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr, 
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced, 
             monsex_description = 
               monsex_description %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>% 
      pivot_longer(cols = ends_with("_rate"), 
                   names_to = "rate_type", 
                   values_to = "rate") %>% 
      filter(rate_type == input$state_outcome) %>% 
      {
        df2 <- .
        df2 %>% 
          mutate(monsex_reorder = 
                   monsex_description %>% 
                   factor(levels = 
                            df2 %>% 
                            filter(geo_level == input$state_sel) %>% 
                            arrange(rate) %>% 
                            pull(monsex_description)))
      } %>% 
      mutate(hover_text = paste0(monsex_description, "\n", 
                                 geo_level, "\n", 
                                 clean_outcome_label(input$state_outcome), ": ", 
                                 scales::percent(rate, accuracy = 0.01)))
    
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      df %>% 
        ggplot(aes(x = rate, y = monsex_reorder, fill = geo_level, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = percent_format(accuracy = 0.1)) +
        labs(x = y_label, y = "Gender", fill = "") +
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
  
  output$state_offense_comparison <- renderPlotly({
    df <- 
      state_data() %>%
      group_by(offense_category, geo_level) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr, 
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced, 
             offense_category = 
               offense_category %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported")) %>% 
      pivot_longer(cols = ends_with("_rate"), 
                   names_to = "rate_type", 
                   values_to = "rate") %>% 
      filter(rate_type == input$state_outcome) %>% 
      {
        df2 <- .
        df2 %>% 
          mutate(offense_category_reorder = 
                   offense_category %>% 
                   factor(levels = 
                            df2 %>% 
                            filter(geo_level == input$state_sel) %>% 
                            arrange(rate) %>% 
                            pull(offense_category)))
      } %>% 
      mutate(hover_text = paste0(offense_category, "\n", 
                                 geo_level, "\n", 
                                 clean_outcome_label(input$state_outcome), ": ", 
                                 scales::percent(rate, accuracy = 0.01)))
    
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      df %>% 
        ggplot(aes(x = rate, y = offense_category_reorder, fill = geo_level, text = hover_text)) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("#0072B2", "#E69F00"), name = "") +
        scale_x_continuous(labels = percent_format(accuracy = 0.1)) +
        labs(x = y_label, y = "Offense Category", fill = "") +
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
  
  output$state_vs_national <- renderPlotly({
    df <- 
      state_data() %>%
      group_by(geo_level, fiscal_year) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr, 
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             probation_rate = 
               probation_sentence_flag/total_sentenced) %>% 
      pivot_longer(cols = ends_with("_rate"), 
                   names_to = "rate_type", 
                   values_to = "rate") %>% 
      filter(rate_type == input$state_outcome) %>% 
      mutate(hover_text = paste0(geo_level, "\n", 
                                 fiscal_year, "\n", 
                                 clean_outcome_label(input$state_outcome), ": ", 
                                 scales::percent(rate, accuracy = 0.01)))
    
    y_label <- clean_outcome_label(input$state_outcome)
    
    {
      df %>% 
        ggplot(aes(x = fiscal_year, y = rate, color = geo_level, group = geo_level, text = hover_text)) +
        geom_line(linewidth = 1) +
        geom_point(size = 2) +
        scale_color_manual(values = c("#0072B2", "#E69F00"), name = "") +  
        labs(x = "Fiscal Year", y = y_label,
             title = paste0(y_label, ": ", input$state_sel, " vs. National Total")) +
        theme_minimal() +
        theme(strip.text   = element_text(size = 12, face = "bold"),
              legend.position = "bottom",
              legend.title = element_blank(), 
              axis.title.x = element_text(margin = margin(t = 15)),
              axis.title.y = element_text(margin = margin(r = 20)),
              plot.title   = element_text(size = 13, face = "bold", hjust = 0.5)) + 
        scale_y_continuous(labels = percent_format(accuracy = 0.1))
    } %>%
      ggplotly(tooltip = "text") %>%
      layout(
        legend = list(
          orientation = "h", 
          x = 0.5, 
          xanchor = "center",
          y = -0.15, 
          yanchor = "top",
          title = list(text = "")  
        ),
        margin = list(l = 80, r = 50, t = 50, b = 55),
        yaxis = list(
          tickformat = ".1%"
        )
      )
  })
  
  # ---- National Dashboard ----
  nat_filtered <- reactive({
    supervision_df %>%
      filter(between(fiscal_year, input$nat_year[1], input$nat_year[2]))
  })
  
  output$nat_trends <- renderPlotly({
    df <- 
      nat_filtered() %>%
      group_by(fiscal_year) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(probation_rate = 
               probation_sentence_flag/total_sentenced, 
             supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr) %>% 
      rename_with(~.x %>% 
                    str_replace_all("probation_rate", "Probation Rate") %>% 
                    str_replace_all("supervised_release_rate", "Supervised Release Rate")) %>% 
      pivot_longer(cols = ends_with("Rate"), 
                   names_to = "outcome", 
                   values_to = "value") %>%
      mutate(hover_text = paste0("Year: ", fiscal_year, "\n",
                                 "Outcome: ", outcome, "\n",
                                 "Rate: ", scales::percent(value, accuracy = 0.01)))
    
    {
      df %>% 
        ggplot(aes(x = fiscal_year, y = value, color = outcome,
                   group = outcome, text = hover_text)) +
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
    df <- 
      nat_filtered() %>%
      group_by(offense_category) %>%
      dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")), 
                              ~.x %>% 
                                sum(na.rm = TRUE))) %>% 
      ungroup() %>% 
      mutate(probation_rate = 
               probation_sentence_flag/total_sentenced, 
             supervised_release_rate = 
               supervised_release_flag/total_sentenced_fsr,
             unfiltered_supervised_release_rate = 
               supervised_release_flag/total_sentenced,
             offense_category = 
               offense_category %>% 
               na_if("") %>% 
               replace_na("Unknown/Unreported"),
             max_rate = pmax(probation_rate, supervised_release_rate)) %>% 
      pivot_longer(cols = c(probation_rate, supervised_release_rate), 
                   names_to = "outcome", 
                   values_to = "value") %>% 
      mutate(outcome = 
               outcome %>% 
               clean_outcome_label(), 
             geo_reorder = 
               reorder(offense_category, max_rate), 
             hover_text  = paste0(offense_category, "\n",
                                  outcome, ": ", scales::percent(value, accuracy = 0.01)))
    
    {
      df %>% 
        ggplot(aes(x = value, y = geo_reorder, fill = outcome, text = hover_text)) +
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
  custom_inputs <- eventReactive(input$generate_plot, {
    list(
      chart_type = input$custom_chart_type,
      outcome    = input$custom_outcome,
      group      = if (input$custom_chart_type == "line" && length(input$custom_outcome) > 1) {
        "fiscal_year"
      } else {
        input$custom_group
      },
      facet      = input$custom_facet,
      age        = input$custom_age,
      year       = input$custom_year,
      state      = input$custom_state,
      race       = input$custom_race,
      educ       = input$custom_educ,
      gender     = input$custom_gender,
      offense    = input$custom_offense,
      sentence   = input$custom_sentence,
      citizen    = input$custom_citizen, 
      variance_by = 
        if(!is.null(input$custom_variance_by)){
          input$custom_variance_by
        }else{
          character(0)
        } 
    )
  })
  
  custom_filtered <- reactive({
    req(custom_inputs())
    inp <- custom_inputs()
    
    supervision_df %>% 
      filter(geo_level != "National Total") %>% 
      left_join(
        geo_crosswalk %>% 
          pivot_longer(cols = c(state_district, po_office),
                       names_to = "geo_level_type",
                       values_to = "geo_level") %>% 
          distinct(),
        by = c("geo_level_type", "geo_level")
      ) %>%
      mutate(state_name = ifelse(is.na(state_name) & geo_level_type == "state_name",
                                 geo_level, state_name)) %>%
      apply_filters_custom(
        age_inp      = inp$age,
        year_inp     = inp$year,
        race_inp     = inp$race,
        educ_inp     = inp$educ,
        gender_inp   = inp$gender,
        offense_inp  = inp$offense,
        sentence_inp = inp$sentence,
        citizen_inp  = inp$citizen
      ) %>%
      filter("All" %in% inp$state | state_name %in% inp$state)
  })
  
  custom_agg <- eventReactive(input$generate_plot, {
    req(custom_inputs())
    inp <- custom_inputs()
    
    df <- custom_filtered()
    
    group_cols <- c(inp$group, inp$facet)
    group_cols <- group_cols[group_cols != ""]
    
    if(inp$chart_type == "line"){
      if(length(inp$outcome) == 1){
        group_cols <- unique(c(group_cols, "fiscal_year"))
      }else{
        group_cols <- unique(c("fiscal_year", inp$facet[inp$facet != ""]))
      }
    }else if(inp$chart_type == "boxplot"){
      group_cols <- unique(c(group_cols, inp$variance_by))
    }
    
    group_by_geo <- any(c("state_name", "state_district", "po_office") %in% group_cols)
    
    # -----------------------------
    # CASE 1: GEO INCLUDED
    # -----------------------------
    if(group_by_geo == TRUE){
      group_cols_full <- unique(c(group_cols, "geo_level_type", "geo_level"))
      
      df_geo <-
        df %>%
        filter(geo_level_type %in% group_cols) %>%
        group_by(across(c(any_of(group_cols_full)))) %>%
        dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")),
                                ~.x %>%
                                  sum(na.rm = TRUE))) %>%
        ungroup() %>%
        mutate(probation_rate =
                 probation_sentence_flag/total_sentenced,
               supervised_release_rate =
                 supervised_release_flag/total_sentenced_fsr,
               unfiltered_supervised_release_rate = 
                 supervised_release_flag/total_sentenced) %>%
        dplyr::select(any_of(c(group_cols_full, input$custom_outcome))) %>%
        dplyr::select(-any_of(c("state_name"))) %>%
        group_by(geo_level_type, geo_level) %>%
        mutate(group_id = cur_group_id()) %>%
        ungroup() %>%
        pivot_wider(names_from = "geo_level_type",
                    values_from = "geo_level") %>%
        dplyr::select(-group_id)
      
    }else{
      group_cols_full = group_cols
      
      df_geo <-
        df %>%
        filter(geo_level_type == "state_name") %>%
        group_by(across(c(all_of(group_cols_full)))) %>%
        dplyr::summarize(across(c(ends_with("_flag"), starts_with("total_sentenced")),
                                ~.x %>%
                                  sum(na.rm = TRUE))) %>%
        ungroup() %>%
        mutate(probation_rate =
                 probation_sentence_flag/total_sentenced,
               unfiltered_supervised_release_rate = 
                 supervised_release_flag/total_sentenced,
               supervised_release_rate =
                 supervised_release_flag/total_sentenced_fsr) %>%
        dplyr::select(all_of(c(group_cols_full, input$custom_outcome)))
    }
    df_geo %>%
      mutate(across(where(is.character),
                    ~.x %>%
                      na_if("") %>%
                      replace_na("Missing/Not Applicable")))
  })
  
  last_generate <- reactiveVal(0)
  
  observeEvent(input$generate_plot, {
    last_generate(input$generate_plot)
  })
  
  observeEvent({
    input$custom_chart_type
    input$custom_outcome
    input$custom_group
    input$custom_facet
    input$custom_age
    input$custom_year
    input$custom_state
    input$custom_race
    input$custom_educ
    input$custom_gender
    input$custom_offense
    input$custom_sentence
    input$custom_citizen
    input$custom_variance_by
  }, {
    last_generate(0)
  })
  
  output$custom_group_ui <- renderUI({
    if(input$custom_chart_type == "line" && length(input$custom_outcome) > 1){
      tags$div(
        class = "reporting-note",
        "Line charts with multiple outcomes automatically group by Fiscal Year."
      )
    }else{
      selectInput("custom_group", "Group By",
                  choices = c("State"            = "state_name",
                              "District"         = "state_district",
                              "PO Office"        = "po_office",
                              "Race"             = "newrace_description",
                              "Education"        = "neweduc_description",
                              "Gender"           = "monsex_description",
                              "Offense Category" = "offense_category",
                              "Fiscal Year"      = "fiscal_year", 
                              "Citizenship Status" = "citizen_description"),
                  selected = "state_name")
    }
  })
  
  output$custom_variance_ui <- renderUI({
    req(input$custom_chart_type %in% c("boxplot", "violinplot"))
    
    already_used <- c(input$custom_group, input$custom_facet)
    
    all_choices <- c(
      "State"              = "state_name",
      "District"           = "state_district",
      "PO Office"          = "po_office",
      "Fiscal Year"        = "fiscal_year",
      "Race"               = "newrace_description",
      "Education"          = "neweduc_description",
      "Gender"             = "monsex_description",
      "Offense Category"   = "offense_category",
      "Citizenship Status" = "citizen_description"
    )
    
    available_choices <- all_choices[!all_choices %in% already_used]
    
    selectInput("custom_variance_by", "Aggregate By",
                choices  = available_choices,
                multiple = TRUE,
                selected = available_choices[1])
  })
  
  output$custom_plot <- renderPlotly({
    empty_state <- plotly::plotly_empty(type = "scatter", mode = "markers") %>%
      layout(
        annotations = list(
          text = "Click 'Generate Plot' to display.",
          x = 0.5, y = 0.5,
          showarrow = FALSE,
          xref = "paper",
          yref = "paper",
          font = list(size = 16, color = "darkgreen")
        ),
        xaxis = list(visible = FALSE),
        yaxis = list(visible = FALSE)
      )
    
    # if (is.null(input$generate_plot) || input$generate_plot == 0) {
    #   return(empty_state)
    # }
    # 
    # if (plot_reset() > input$generate_plot) {
    #   return(empty_state)
    # }
    
    if (is.null(input$generate_plot) || last_generate() == 0) {
      return(empty_state)
    }
    
    tryCatch({
      req(input$custom_group)
      
      df <- tryCatch(custom_agg(), error = function(e) NULL)
      
      if (is.null(df) || nrow(df) == 0) {
        plot.new()
        text(0.5, 0.5, "No data. Adjust filters and click 'Generate Plot'.", cex = 1.2)
        return()
      }
      
      # ---------------------------
      # INPUTS
      # ---------------------------
      req(input$custom_group)
      x_var <- input$custom_group
      
      outcome_vars <- input$custom_outcome
      
      facet_vars <- input$custom_facet
      facet_vars <- facet_vars[!is.null(facet_vars) & facet_vars != ""]
      
      is_rate <- any(str_detect(outcome_vars, "rate"))
      
      fmt_value <- function(x) {
        if (is_rate) scales::percent(x, accuracy = 0.01)
        else scales::comma(x)
      }
      
      # ---------------------------
      # LONG FORMAT
      # ---------------------------
      df_long <- 
        df %>%
        pivot_longer(cols = all_of(outcome_vars),
                     names_to = "outcome",
                     values_to = "value") %>%
        mutate(outcome = clean_outcome_label(outcome)) %>%
        {
          blah <- .
          if(input$custom_chart_type == "line"){
            complete_vars <- 
              if(length(outcome_vars) > 1){
                c("fiscal_year", "outcome", facet_vars)
              }else{
                c("fiscal_year", x_var, "outcome", facet_vars)
              }
            
            complete_vars <- complete_vars[complete_vars != ""]
            
            blah %>% 
              complete(!!!syms(complete_vars), fill = list(value = 0))
          }else if(input$custom_chart_type == "boxplot"){
            complete_vars <- c(x_var, "outcome", input$custom_variance_by, facet_vars)
            complete_vars <- complete_vars[complete_vars != ""]
            
            blah %>% 
              complete(!!!syms(complete_vars), fill = list(value = 0))
          }else{
            .
          }
        } %>%
        mutate(group_label = 
                 if(input$custom_chart_type == "line" && length(outcome_vars) > 1){
                   fiscal_year
                 }else{
                   .data[[x_var]]
                 }, 
               value = 
                 value %>% 
                 replace_na(0))
      
      n_groups <- n_distinct(df_long[[x_var]])
      y_size <- case_when(
        n_groups > 80 ~ 4,
        n_groups > 50 ~ 5,
        n_groups > 20 ~ 6,
        TRUE          ~ 8
      )
      
      # ---------------------------
      # FACET TEXT (SAFE)
      # ---------------------------
      facet_text <- 
        if(length(facet_vars) > 0){
          clean_facet_names <- 
            facet_vars %>%
            clean_geo_label() %>%
            clean_outcome_label() %>%
            str_replace_all("Newrace Description",   "Race:") %>%
            str_replace_all("newrace_description",   "Race:") %>%
            str_replace_all("neweduc_description",   "Education:") %>%
            str_replace_all("Neweduc Description",   "Education:") %>%
            str_replace_all("monsex_description",    "Gender:") %>%
            str_replace_all("Monsex Description",    "Gender:") %>%
            str_replace_all("offense_category",      "Offense Category:") %>%
            str_replace_all("Offense Category",      "Offense Category:") %>%
            str_replace_all("citizen_description",   "Citizenship Status:") %>%
            str_replace_all("Citizen Description",   "Citizenship Status:") %>%
            str_replace_all("fiscal_year",           "Fiscal Year:")
          
          apply(df_long[, facet_vars, drop = FALSE], 1, function(row) {
            paste(clean_facet_names, row, collapse = "\n")
          })
        }else{
          rep("", nrow(df_long))
        }
      
      # ---------------------------
      # HOVER TEXT (FIXED)
      # ---------------------------
      df_long <- 
        df_long %>%
        mutate(hover_text = 
                 if(input$custom_chart_type == "line" && length(outcome_vars) > 1){
                   paste0("Fiscal Year: ", fiscal_year,
                          ifelse(facet_text != "", paste0("\n", facet_text), ""), 
                          "\n",
                          outcome, ": ", fmt_value(value))
                 }else{
                   paste0(x_var %>% 
                            clean_geo_label() %>% 
                            clean_outcome_label() %>% 
                            str_replace_all("Newrace Description",   "Race") %>%
                            str_replace_all("newrace_description",   "Race") %>%
                            str_replace_all("neweduc_description",   "Education") %>%
                            str_replace_all("Neweduc Description",   "Education") %>%
                            str_replace_all("monsex_description",    "Gender") %>%
                            str_replace_all("Monsex Description",    "Gender") %>%
                            str_replace_all("offense_category",      "Offense Category") %>%
                            str_replace_all("Offense Category",      "Offense Category") %>%
                            str_replace_all("citizen_description",   "Citizenship Status") %>%
                            str_replace_all("Citizen Description",   "Citizenship Status") %>%
                            str_replace_all("fiscal_year",           "Fiscal Year"), ": ", group_label, "\n",
                          outcome, ": ", fmt_value(value),
                          ifelse(facet_text != "", paste0("\n", facet_text), ""))
                 })
      
      # ---------------------------
      # LABELS
      # ---------------------------
      x_lab <- 
        x_var %>% 
        clean_geo_label() %>% 
        clean_outcome_label() %>% 
        str_replace_all("Newrace Description",   "Race") %>%
        str_replace_all("newrace_description",   "Race") %>%
        str_replace_all("neweduc_description",   "Education") %>%
        str_replace_all("Neweduc Description",   "Education") %>%
        str_replace_all("monsex_description",    "Gender") %>%
        str_replace_all("Monsex Description",    "Gender") %>%
        str_replace_all("offense_category",      "Offense Category") %>%
        str_replace_all("Offense Category",      "Offense Category") %>%
        str_replace_all("citizen_description",   "Citizenship Status") %>%
        str_replace_all("Citizen Description",   "Citizenship Status") %>%
        str_replace_all("fiscal_year",           "Fiscal Year")
      
      y_lab <- paste(clean_outcome_label(outcome_vars), collapse = " / ")
      
      # ---------------------------
      # PLOT
      # ---------------------------
      p <- NULL
      
      # ===========================
      # BAR CHART
      # ===========================
      if(input$custom_chart_type == "bar"){
        
        p <- ggplot(df_long,
                    aes(
                      x = reorder(.data[[x_var]], value),
                      y = value,
                      fill = outcome,
                      text = hover_text
                    )) +
          geom_col(position = "dodge") +
          coord_flip() +
          scale_fill_brewer(palette = "Set2") +
          labs(x = x_lab, y = y_lab, fill = "Outcome") +
          theme_minimal() +
          theme(axis.text.y = element_text(size = y_size))  # <- dynamic size here
        
        if (is_rate) {
          p <- p + scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
        } else {
          p <- p + scale_y_continuous(labels = scales::comma_format())
        }
      }
      
      # ===========================
      # LINE CHART
      # ===========================
      if(input$custom_chart_type == "line"){
        
        multi_outcome <- length(outcome_vars) > 1
        
        if(multi_outcome){
          p <- ggplot(df_long,
                      aes(
                        x     = fiscal_year,
                        y     = value,
                        color = outcome,
                        group = outcome,
                        text  = hover_text
                      )) +
            geom_line(linewidth = 1.2) +
            geom_point() +
            scale_color_brewer(palette = "Set2") +
            labs(x = "Fiscal Year", y = y_lab, color = "Outcome") +
            theme_minimal()
        }else{
          p <- ggplot(df_long,
                      aes(
                        x     = fiscal_year,
                        y     = value,
                        color = .data[[x_var]],
                        group = .data[[x_var]],
                        text  = hover_text
                      )) +
            geom_line(linewidth = 1.2) +
            geom_point() +
            scale_color_brewer(palette = "Set2") +
            labs(x = "Fiscal Year", y = y_lab, color = x_lab) +
            theme_minimal()
        }
        
        if(is_rate){
          p <- p + scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
        }else{
          p <- p + scale_y_continuous(labels = scales::comma_format())
        }
      }
      
      # ===========================
      # BOXPLOT
      # ===========================
      # ===========================
      # OUTCOME BOXPLOT
      # ===========================
      if(input$custom_chart_type == "boxplot"){
        p <- 
          df_long %>% 
          ggplot(aes(x = .data[[x_var]],
                     y = value,
                     fill = outcome, 
                     text = paste0(
                       x_lab, ": ", .data[[x_var]], "\n",
                       "Outcome: ", outcome, "\n",
                       "Distribution across: ", paste(input$custom_variance_by, collapse = " & ")
                     ))) +
          geom_boxplot() +
          coord_flip() +
          scale_fill_brewer(palette = "Set2") +
          labs(x = x_lab, y = y_lab, fill = "Outcome") +
          theme_minimal() +
          theme(axis.text.y = element_text(size = y_size))
        
        if(is_rate){
          p <- p + scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
        }else{
          p <- p + scale_y_continuous(labels = scales::comma_format())
        }
      }
      
      # ===========================
      # OUTCOME VIOLIN PLOT
      # ===========================
      if(input$custom_chart_type == "violinplot"){
        p <- 
          df_long %>% 
          group_by(.data[[x_var]], outcome) %>%
          filter(n() >= 3) %>%
          ungroup() %>%
          ggplot(aes(x = .data[[x_var]],
                     y = value,
                     fill = outcome, 
                     text = paste0(
                       x_lab, ": ", .data[[x_var]], "\n",
                       "Outcome: ", outcome, "\n",
                       "Distribution across: ", paste(input$custom_variance_by, collapse = " & ")
                     ))) +
          geom_violin(width = 1.4) +
          geom_boxplot(width = 0.1, color="grey", alpha=0.2) +
          coord_flip() +
          scale_fill_brewer(palette = "Set2") +
          labs(x = x_lab, y = y_lab, fill = "Outcome") +
          theme_minimal() +
          theme(axis.text.y = element_text(size = y_size))
        
        if(is_rate){
          p <- p + scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
        }else{
          p <- p + scale_y_continuous(labels = scales::comma_format())
        }
      }
      
      # ===========================
      # SCATTER
      # ===========================
      if (input$custom_chart_type == "scatter") {
        
        req(length(outcome_vars) == 2)
        
        df <- df %>%
          mutate(group_label = .data[[x_var]])
        
        df$hover_text <- paste0(
          x_var, ": ", df$group_label
        )
        
        p <- ggplot(df,
                    aes(
                      x = .data[[outcome_vars[1]]],
                      y = .data[[outcome_vars[2]]],
                      text = hover_text
                    )) +
          geom_point(size = 3, alpha = 0.7, color = "#009E73") +
          labs(
            x = clean_outcome_label(outcome_vars[1]),
            y = clean_outcome_label(outcome_vars[2])
          ) +
          theme_minimal()
        
        if (is_rate) {
          p <- p +
            scale_x_continuous(labels = scales::percent_format(accuracy = 0.1)) +
            scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
        } else {
          p <- p +
            scale_x_continuous(labels = scales::comma_format()) +
            scale_y_continuous(labels = scales::comma_format())
        }
      }
      
      # ---------------------------
      # FACETING
      # ---------------------------
      if (length(facet_vars) == 1) {
        p <- p + facet_wrap(vars(.data[[facet_vars[1]]]))
      } else if (length(facet_vars) == 2) {
        p <- p + facet_grid(
          rows = vars(.data[[facet_vars[1]]]),
          cols = vars(.data[[facet_vars[2]]])
        )
      }
      
      # ---------------------------
      # FINAL OUTPUT
      # ---------------------------
      if(input$custom_chart_type %in% c("boxplot", "violinplot")){
        ggplotly(p, tooltip = "text") %>%
          layout(
            boxmode = "group",
            legend = list(
              orientation = "h",
              x = 0.5,
              xanchor = "center",
              y = -0.15,
              title = list(text = "")
            ),
            margin = list(l = 0, r = 10, t = 20, b = 60)
          )
      }else{
        ggplotly(p, tooltip = "text") %>%
          layout(
            legend = list(
              orientation = "h",
              x = 0.5,
              xanchor = "center",
              y = -0.15,
              title = list(text = "")
            ),
            margin = list(l = 0, r = 10, t = 20, b = 60)
          )
      }
      
    }, error = function(e) {
      
      # -----------------------------------
      # SAFE FALLBACK (NO CRASH EVER)
      # -----------------------------------
      plotly::plotly_empty(type = "scatter", mode = "markers") %>%
        layout(
          annotations = list(
            text = "Click 'Generate Plot' to display.",
            # text = paste("Error:", conditionMessage(e)),
            x = 0.5, y = 0.5,
            showarrow = FALSE,
            xref = "paper",
            yref = "paper",
            font = list(size = 16, color = "darkgreen")
          ),
          xaxis = list(visible = FALSE),
          yaxis = list(visible = FALSE)
        )
    })
  })
  
  output$custom_table <- renderDT({
    if (is.null(input$generate_plot) || input$generate_plot == 0)
      return(datatable(data.frame(Message = "Click 'Generate Plot' to view table")))
    
    df <- custom_agg()
    if (!all(input$custom_outcome %in% names(df)) || nrow(df) == 0)
      return(datatable(data.frame(Message = "No data available.")))
    
    df_renamed <- 
      df %>%
      mutate(across(any_of("fiscal_year"), as.character)) %>% 
      relocate(any_of(input$custom_outcome), .after = last_col()) %>%  
      rename_with(~.x %>% 
                    clean_outcome_label() %>%
                    clean_geo_label() %>%
                    str_replace_all("Newrace Description", "Race") %>%
                    str_replace_all("Neweduc Description", "Education") %>%
                    str_replace_all("Monsex Description",  "Gender") %>%
                    str_replace_all("Offguide Description", "Offense Category") %>% 
                    str_replace_all("Citizen Description", "Citizenship Status"))
    
    rate_cols  <- names(df_renamed)[str_detect(names(df_renamed), "Rate")]
    count_cols <- names(df_renamed)[sapply(df_renamed, is.numeric) & !names(df_renamed) %in% rate_cols]
    
    datatable(
      df_renamed,
      extensions = "Buttons",
      class      = "compact stripe hover",
      filter     = "top",
      options    = list(
        paging  = FALSE,
        scrollY = "400px",
        scrollX = TRUE
      ),
      style    = "bootstrap",
      rownames = FALSE
    ) %>%
      formatPercentage(rate_cols,  digits = 1) %>%
      formatRound(count_cols, digits = 0, mark = ",")
  })
  
  output$download_custom_data <- downloadHandler(
    filename = function() paste0("custom_USSC_table_", Sys.Date(), ".csv"),
    content  = function(file) write_csv(custom_agg(), file)
  )
  
  output$download_custom_plot <- downloadHandler(
    filename = function() paste0("custom_plot_", Sys.Date(), ".png"),
    content  = function(file) {
      ggsave(file, width = 10, height = 7)
    }
  )
  
  # ---- Overview page ----
  output$ov_trend_chart    <- renderPlotly({ ov_plot_trend() })
  output$ov_district_chart <- renderPlotly({ ov_plot_district() })
  
  # ---- Modeling Supervision Rates page ----
  output$mod_or_chart <- renderPlotly({ mod_plot_or() })
  output$mod_fy_chart <- renderPlotly({ mod_plot_fy() })
  output$mod_sr_forest <- renderPlotly({ mod_plot_forest("Supervised release") })
  output$mod_pb_forest <- renderPlotly({ mod_plot_forest("Probation") })
  output$mod_sr_obs <- renderPlotly({ mod_plot_obs("Supervised release", mod_obs_sr_offense) })
  output$mod_pb_obs <- renderPlotly({ mod_plot_obs("Probation", mod_obs_pb_offense) })
  output$mod_sr_racecit <- renderPlotly({ mod_plot_race_cit() })
  output$mod_sr_icc <- renderPlotly({ mod_plot_icc("Supervised release") })
  output$mod_pb_icc <- renderPlotly({ mod_plot_icc("Probation") })
  output$mod_sr_robust_chart <- renderPlotly({ mod_plot_robust("Supervised release") })
  output$mod_pb_robust_chart <- renderPlotly({ mod_plot_robust("Probation") })
  output$mod_miss_field_chart <- renderPlotly({ mod_plot_miss_field() })
  output$mod_miss_edu_chart <- renderPlotly({ mod_plot_miss_edu() })
  
  # Links from the Overview page to the other tabs
  observeEvent(input$ov_go_main,     { updateNavbarPage(session, "main_nav", selected = "Main Dashboard") })
  observeEvent(input$ov_go_state,    { updateNavbarPage(session, "main_nav", selected = "State Dashboard") })
  observeEvent(input$ov_go_national, { updateNavbarPage(session, "main_nav", selected = "National Dashboard") })
  observeEvent(input$ov_go_custom,   { updateNavbarPage(session, "main_nav", selected = "Custom Visualization") })
  observeEvent(input$ov_go_about,    { updateNavbarPage(session, "main_nav", selected = "About") })
  observeEvent(input$ov_go_model,    { updateNavbarPage(session, "main_nav", selected = "Modeling") })
  
  # Link from the modeling page to the glossary on the About tab
  observeEvent(input$mod_go_terms, {
    updateNavbarPage(session, "main_nav", selected = "About")
    updateTabsetPanel(session, "about_tabs", selected = "Key terms")
  })
}

####Run App####
shinyApp(ui, server)