####Helpers shared by the two report files####
# Used by supervised-release-report.Rmd and probation-report.Rmd (both call source("report-helpers.R")).
# It loads the model results from data/modeling-results/, formats numbers for the text, and draws the figures.
# The results come from R/fit-supervision-models.R. The dashboard reads the same files, so the app and reports agree.

suppressPackageStartupMessages({
  library(tidyverse)
  library(patchwork)
  library(svglite)
  library(here)
})

####Constants####
SR <- "Supervised release"
PB <- "Probation"
COLORS <- c(setNames("#009E73", SR), setNames("#0072B2", PB))   # same colors as the dashboard
DARK   <- c(setNames("#00785A", SR), setNames("#005A8C", PB))   # darker shade for text and rules on white paper
INK <- "#2C2C2C"; MUTED <- "#6B6B6B"; GRID <- "#E6DCC3"; GREY <- "#8C8470"; LINK <- "#CDBF9C"
FONT <- "Inter"

# Cover date: the month the reports were last rebuilt
MONTH_YEAR <- format(Sys.Date(), "%B %Y")

####Read the results####
results_dir <- Sys.getenv("MODELING_RESULTS_DIR", here("data", "modeling-results"))
read_results <- function(){
  rd <- function(f, ...) readr::read_csv(file.path(results_dir, f), show_col_types = FALSE, ...)
  list(
    or         = rd("odds_ratios.csv", col_types = readr::cols(p_value = readr::col_character(), ref = readr::col_character(), .default = readr::col_guess())),
    fy         = rd("fiscal_year_odds_ratios.csv", col_types = readr::cols(p_value = readr::col_character(), .default = readr::col_guess())),
    icc        = rd("icc.csv"),
    rob        = rd("robustness.csv", col_types = readr::cols(display = readr::col_character(), note = readr::col_character(), .default = readr::col_guess())),
    miss_field = rd("missing_by_field.csv"),
    miss_edu   = rd("missing_education_by_group.csv"),
    obs_year   = rd("observed_national_by_year.csv"),
    obs_group  = rd("observed_by_group.csv"),
    details    = rd("model_details.csv"),
    miss_cat   = rd("missing_category_odds_ratios.csv")
  )
}
R <- read_results()
FY_FIRST <- min(R$obs_year$fiscal_year)
FY_LAST  <- max(R$obs_year$fiscal_year)

# Look-ups into the results
odds     <- function(oc, label) R$or$or[R$or$outcome == oc & R$or$label == label]
or_row   <- function(oc, label) R$or[R$or$outcome == oc & R$or$label == label, ]
fy_or    <- function(oc, year)  R$fy$or[R$fy$outcome == oc & R$fy$fiscal_year == year]
icc_val  <- function(oc, component, col = "primary") R$icc[[col]][R$icc$outcome == oc & R$icc$component == component]
detail   <- function(oc, metric, col = "complete") R$details[[col]][R$details$outcome == oc & R$details$metric == metric]
# Odds ratio of a coefficient in the model that keeps missing demographics as their own category
miss_cat_or <- function(oc, term) R$miss_cat$or[R$miss_cat$outcome == oc & R$miss_cat$term == term]
# Raw (unadjusted) rate for one group
group_rate <- function(dimension, group, oc){
  r <- R$obs_group[R$obs_group$dimension == dimension & R$obs_group$group == group, ]
  if(oc == SR) r$sr_events / r$sr_denom else r$pb_events / r$pb_denom
}
year_rate <- function(oc){
  d <- R$obs_year
  tibble(fiscal_year = d$fiscal_year, rate = if(oc == SR) d$sr_events / d$sr_denom else d$pb_events / d$pb_denom)
}
year_rate_at <- function(oc, year) year_rate(oc)$rate[year_rate(oc)$fiscal_year == year]


####Formatting for the text####
# Percent used in plain sentences: one decimal below 10%, whole numbers above
mod_pct <- function(p) ifelse(p < 10, sub("\\.0$", "", sprintf("%.1f", p)), as.character(floor(p + 0.5 + 1e-9)))
lower   <- function(x) paste0(mod_pct(100 * (1 - x)), "%")
higher  <- function(x) paste0(mod_pct(100 * (x - 1)), "%")
times   <- function(x) paste0(sprintf("%.1f", x), " times")
fmt_or  <- function(x) sprintf("%.3f", x)
pct     <- function(x, digits = 1) paste0(formatC(100 * x, format = "f", digits = digits), "%")
num     <- function(x, digits = 0) formatC(x, format = "f", digits = digits, big.mark = ",")

plain <- function(or, lo, hi){
  ifelse(lo <= 1 & hi >= 1, "No clear difference",
         ifelse(or < 1, paste0(lower(or), " lower odds"),
                ifelse(or >= 2, paste0(times(or), " the odds"), paste0(higher(or), " higher odds"))))
}
esc <- function(x) htmltools::htmlEscape(as.character(x))
# Print HTML into the knitted document untouched (pandoc does not parse it as markdown)
show <- function(x) cat("\n```{=html}\n", paste(x, collapse = "\n"), "\n```\n\n", sep = "")


####HTML pieces####
html_table <- function(headers, rows, num_cols = integer(), cls = ""){
  cell <- function(tag, x, i) sprintf('<%s class="%s">%s</%s>', tag, if(i %in% num_cols) "num" else "", x, tag)
  head <- paste0(mapply(function(h, i) cell("th", h, i), headers, seq_along(headers)), collapse = "")
  body <- paste0(vapply(rows, function(r) paste0("<tr>", paste0(mapply(function(x, i) cell("td", x, i), r, seq_along(r)), collapse = ""), "</tr>"), ""), collapse = "")
  sprintf('<table class="%s"><thead><tr>%s</tr></thead><tbody>%s</tbody></table>', cls, head, body)
}
flag     <- function(label, kind) sprintf('<span class="flag %s">%s</span>', kind, esc(label))
kpis     <- function(values, labels) paste0('<div class="kpis">', paste0(sprintf('<div class="kpi"><div class="v">%s</div><div class="l">%s</div></div>', values, labels), collapse = ""), "</div>")
page_break <- function() '<div class="pb"></div>'
toc <- function(items) paste0('<div class="toc">', paste0(sprintf('<div><span class="n">%d</span>%s</div>', seq_along(items), items), collapse = ""), "</div>")
# Running footer text of the printed report (page numbers are added by report.css)
running_footer <- function(name) show(sprintf('<style>@page { @bottom-left { content: "REFORM Alliance \\00a0\\00b7\\00a0 %s Analysis Report"; } }</style>', name))
REPORT_NAME <- c(setNames("Supervised Release", SR), setNames("Probation", PB))
cover <- function(oc, title, sub, companion){
  sprintf('<style>:root{--c:%s;--dark:%s;}</style>
<div class="cover"><div class="kicker">REFORM Alliance &middot; Federal sentencing data analysis</div>
<h1>%s</h1><div class="sub">%s</div>
<div class="meta">%s &nbsp;&middot;&nbsp; %s Analysis Report &nbsp;&middot;&nbsp; Companion to the %s Analysis Report</div></div>',
          COLORS[[oc]], DARK[[oc]], title, sub, MONTH_YEAR, REPORT_NAME[[oc]], companion)
}

# Figures are drawn as inline SVG and numbered in the order they appear
.fig_env <- new.env(); .fig_env$n <- 0
next_figure <- function() .fig_env$n + 1
svg_string <- function(plot, width, height){
  s <- svglite::svgstring(width = width, height = height, standalone = FALSE)
  print(plot)
  grDevices::dev.off()
  as.character(s())
}
figure <- function(plot, width, height, title, caption, alt){
  .fig_env$n <- .fig_env$n + 1
  show(sprintf('<figure role="img" aria-label="%s">%s<figcaption><b>Figure %d. %s.</b> %s</figcaption></figure>',
               esc(alt), svg_string(plot, width, height), .fig_env$n, esc(title), caption))
}


####Tables for the appendix####
tbl_odds <- function(oc){
  t <- R$or %>% filter(outcome == oc)
  rows <- lapply(seq_len(nrow(t)), function(i){
    r <- t[i, ]
    comp <- if(is.na(r$ref)) r$label else paste0(r$label, " vs. ", r$ref)
    words <- if(r$group == "Age") paste0(sprintf("%.1f", 100 * (r$or - 1)), "% higher odds with each year of age") else plain(r$or, r$lo, r$hi)
    c(esc(r$group), esc(comp), fmt_or(r$or), paste0(fmt_or(r$lo), " to ", fmt_or(r$hi)), esc(r$p_value), esc(words))
  })
  html_table(c("Group", "Comparison", "Odds ratio", "95% CI", "p-value", "In plain words"), rows, num_cols = 3:5)
}

tbl_fy <- function(oc){
  t <- R$fy %>% filter(outcome == oc) %>% arrange(fiscal_year)
  rows <- lapply(seq_len(nrow(t)), function(i){
    r <- t[i, ]
    p <- plain(r$or, r$lo, r$hi)
    words <- if(grepl("odds", p)) sub("odds", "odds than in FY2014", p) else "No clear difference from FY2014"
    c(paste0("FY", r$fiscal_year, " vs. FY", FY_FIRST), fmt_or(r$or), paste0(fmt_or(r$lo), " to ", fmt_or(r$hi)), esc(r$p_value), esc(words))
  })
  html_table(c("Fiscal year", "Odds ratio", "95% CI", "p-value", "In plain words"), rows, num_cols = 2:4)
}

tbl_icc <- function(oc){
  t <- R$icc %>% filter(outcome == oc)
  cell <- function(x) ifelse(is.na(x), "&ndash;", pct(x, 1))
  rows <- lapply(seq_len(nrow(t)), function(i) c(esc(t$component[i]), cell(t$primary[i]), cell(t$ar1[i]), cell(t$ar1_mice[i])))
  html_table(c("Source of variation", "Main model", "Model with yearly trends", "Yearly-trend model, multiple imputation"), rows, num_cols = 2:4)
}

# Standard deviations and AR1 correlations of the yearly-trend model, complete cases and imputed
tbl_icc_detail <- function(oc){
  spec <- tribble(~label, ~sd_metric, ~rho_metric,
                  "State baseline", "ar1_sd_state", NA,
                  "District baseline", "ar1_sd_district", NA,
                  "National trend", "ar1_sd_national", "ar1_rho_national",
                  "District trend", "ar1_sd_district_trend", "ar1_rho_district")
  f3 <- function(x) ifelse(is.na(x), "&ndash;", sprintf("%.3f", x))
  rows <- lapply(seq_len(nrow(spec)), function(i){
    s <- spec[i, ]
    rho <- function(col) if(is.na(s$rho_metric)) NA_real_ else detail(oc, s$rho_metric, col)
    c(s$label, f3(detail(oc, s$sd_metric, "complete")), f3(detail(oc, s$sd_metric, "imputed")), f3(rho("complete")), f3(rho("imputed")))
  })
  html_table(c("Component", "Std. dev. (complete cases)", "Std. dev. (imputed)", "AR1 correlation (complete cases)", "AR1 correlation (imputed)"), rows, num_cols = 2:5, cls = "keep nowrap1")
}

tbl_robust <- function(oc){
  t <- R$rob %>% filter(outcome == oc)
  aps <- intersect(c("Complete cases", "Missing as its own category", "Multiple imputation"), unique(t$approach))
  rows <- lapply(sort(unique(t$order)), function(k){
    g <- t %>% filter(order == k)
    cells <- vapply(aps, function(a){
      r <- g %>% filter(approach == a)
      paste0(esc(r$display), if(!is.na(r$note)) sprintf(' <span class="small">(%s)</span>', esc(r$note)) else "")
    }, "")
    c(esc(g$comparison[1]), unname(cells), flag(g$result_label[1], g$result_kind[1]))
  })
  html_table(c("Comparison", aps, "Result"), rows, num_cols = seq_along(aps) + 1, cls = "keep")
}

# Side-by-side of the two outcomes for the main factors
compare_table <- function(oc){
  other <- if(oc == SR) PB else SR
  items <- tribble(~label, ~ref,
                   "Non-U.S. citizen", "vs. U.S. citizen", "Female", "vs. male", "Black", "vs. White", "Hispanic", "vs. White",
                   "College graduate", "vs. less than H.S.", "Sexual offense", "vs. drug trafficking", "Fraud", "vs. drug trafficking",
                   "Immigration", "vs. drug trafficking")
  cell <- function(o, lbl){ r <- or_row(o, lbl); sprintf('%s <span class="small">(%s)</span>', fmt_or(r$or), tolower(plain(r$or, r$lo, r$hi))) }
  rows <- lapply(seq_len(nrow(items)), function(i) c(paste(items$label[i], items$ref[i]), cell(SR, items$label[i]), cell(PB, items$label[i])))
  paste0('<h4>How supervised release and probation compare</h4>',
         html_table(c("Comparison", "Supervised release", "Probation"), rows, num_cols = 2:3),
         '<p class="small">Adjusted odds ratios from the two reports. The same groups often have opposite results: factors linked to more probation tend to be linked to less supervised release.</p>')
}

ref_table <- function(){
  html_table(c("Characteristic", "Compared with (reference group)"), list(
    c("Age", "Each additional year"),
    c("Race and ethnicity: Black, Hispanic, Other", "White"),
    c("Gender: Female", "Male"),
    c("Citizenship: Non-U.S. citizen", "U.S. citizen"),
    c("Education: H.S. graduate, Some college, College graduate", "Less than H.S. graduate"),
    c("Offense: Firearms, Fraud, Immigration, Sexual, Violent, Other", "Drug trafficking"),
    c("Fiscal year", "FY2014")), cls = "kv")
}

sample_table <- function(oc, who){
  mf <- R$miss_field %>% filter(outcome == oc) %>% { setNames(.$pct, .$field) }
  html_table(c("Sample", ""), list(
    c(paste("Rows for", who), num(detail(oc, "n_eligible"))),
    c("Rows used in the main analysis (no missing values on the variables in the model)", sprintf("%s (%.1f%%)", num(detail(oc, "n_complete")), detail(oc, "complete_pct"))),
    c("Most often missing", sprintf("Education (%.1f%% of rows), race and ethnicity (%.1f%%), citizenship (%.1f%%), gender (%.1f%%)",
                                     mf[["Education"]], mf[["Race and ethnicity"]], mf[["Citizenship"]], mf[["Gender"]])),
    c("Never missing", "Offense category, fiscal year, judicial district")), cls = "kv")
}


####Figures####
theme_report <- function(){
  theme_minimal(base_size = 8.2, base_family = FONT) +
    theme(text = element_text(colour = INK),
          panel.grid.major.y = element_blank(), panel.grid.minor = element_blank(),
          panel.grid.major.x = element_line(colour = GRID, linewidth = 0.3),
          axis.text = element_text(colour = INK, size = 8.2), axis.text.x = element_text(colour = MUTED, size = 7.8),
          axis.title = element_text(colour = INK, size = 8.2), axis.title.y = element_blank(),
          axis.line.x = element_line(colour = "#BDB59E", linewidth = 0.3), axis.ticks.x = element_line(colour = "#BDB59E", linewidth = 0.3),
          axis.ticks.length.x = unit(2, "pt"),
          plot.title = element_text(face = "bold", size = 9, hjust = 0, margin = margin(b = 4)), plot.title.position = "plot",
          legend.position = "bottom", legend.title = element_blank(), legend.text = element_text(size = 8, colour = INK),
          legend.key.height = unit(8, "pt"), legend.margin = margin(0, 0, 0, 0), legend.box.margin = margin(-2, 0, 0, 0),
          plot.margin = margin(4, 10, 4, 4))
}
pt <- function(x) x / ggplot2::.pt          # text size in points -> geom_text size
pct_axis <- scales::label_percent(accuracy = 1)
log_ticks <- function(rng, ticks = c(0.03, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)) ticks[ticks >= rng[1] & ticks <= rng[2]]
fmt_pct_label <- function(v, digits = NULL) if(is.null(digits)) ifelse(is.na(v), "", ifelse(v < 0.1, pct(v, 1), pct(v, 0))) else pct(v, digits)
dashed <- "22"

# Comparisons whose result changes with the way missing data are handled (marked with a dagger)
less_certain <- function(oc){
  R$rob %>% filter(outcome == oc, result_kind %in% c("warn", "bad")) %>% pull(comparison) %>% str_remove(" vs\\..*$") %>% unique() %>% sort()
}

fig_trend <- function(oc){
  col <- COLORS[[oc]]
  s <- year_rate(oc) %>% mutate(pct = rate * 100)
  marks <- list()
  marks[[as.character(s$fiscal_year[1])]] <- "above"
  marks[[as.character(s$fiscal_year[nrow(s)])]] <- "below"
  marks[[as.character(s$fiscal_year[which.max(s$rate)])]] <- "above"
  marks[[as.character(s$fiscal_year[which.min(s$rate)])]] <- "below"
  lab <- tibble(fiscal_year = as.integer(names(marks)), pos = unlist(marks)) %>% inner_join(s, by = "fiscal_year")
  pad <- (max(s$pct) - min(s$pct)) * 0.35
  ggplot(s, aes(fiscal_year, pct)) +
    geom_line(colour = col, linewidth = 0.75) +
    geom_point(shape = 21, size = 2.3, fill = col, colour = "white", stroke = 0.4) +
    geom_text(data = lab, aes(label = sprintf("%.1f%%", pct), vjust = ifelse(pos == "above", -0.9, 1.9)),
              size = pt(8), fontface = "bold", colour = INK, family = FONT) +
    scale_x_continuous(breaks = s$fiscal_year, labels = sprintf("FY%02d", s$fiscal_year %% 100), expand = expansion(add = 0.5)) +
    scale_y_continuous(limits = c(min(s$pct) - pad, max(s$pct) + pad), labels = function(v) sprintf("%.0f%%", v)) +
    labs(x = NULL) +
    theme_report() +
    theme(panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = GRID, linewidth = 0.3),
          axis.line.x = element_blank(), axis.ticks.x = element_blank(), axis.ticks.y = element_blank())
}

forest_groups <- list(
  list(group = "Citizenship",    title = "Citizenship (vs. U.S. citizen)"),
  list(group = "Gender",         title = "Gender (vs. male)"),
  list(group = "Race/ethnicity", title = "Race and ethnicity (vs. White)"),
  list(group = "Education",      title = "Education (vs. less than H.S.)"),
  list(group = "Offense",        title = "Offense type (vs. drug trafficking)"))

forest_rows <- function(oc){
  unc <- less_certain(oc)
  t <- R$or %>% filter(outcome == oc, group != "Age")
  rows <- bind_rows(lapply(forest_groups, function(g){
    items <- t %>% filter(group == g$group) %>% arrange(desc(or))
    bind_rows(tibble(kind = "h", text = g$title, label = NA_character_),
              tibble(kind = "r", text = paste0(items$label, ifelse(items$label %in% unc, " \u2020", "")), label = items$label))
  }))
  rows %>% mutate(ypos = rev(seq_len(n())))
}

fig_forest <- function(oc){
  col <- COLORS[[oc]]
  rows <- forest_rows(oc)
  ord <- order(rows$ypos)      # axis labels are given bottom to top, so the bold headings line up
  d <- R$or %>% filter(outcome == oc, group != "Age") %>% inner_join(rows %>% filter(kind == "r") %>% select(label, ypos), by = "label") %>%
    mutate(clear = factor(ifelse(lo <= 1 & hi >= 1, "No clear difference", "Clear difference (95% interval excludes 1)"),
                          levels = c("Clear difference (95% interval excludes 1)", "No clear difference")))
  rng <- c(0.035, 8.5)
  ggplot(d, aes(y = ypos)) +
    geom_vline(xintercept = 1, linetype = dashed, colour = INK, linewidth = 0.4) +
    geom_segment(aes(x = lo, xend = hi, yend = ypos), colour = col, linewidth = 0.7) +
    geom_point(aes(x = or, fill = clear), shape = 21, size = 2.5, colour = col, stroke = 0.6) +
    scale_fill_manual(values = c("Clear difference (95% interval excludes 1)" = col, "No clear difference" = "white"), drop = TRUE) +
    scale_x_log10(limits = rng, breaks = log_ticks(rng), labels = log_ticks(rng), expand = expansion(0)) +
    scale_y_continuous(breaks = rows$ypos[ord], labels = rows$text[ord], limits = c(0.3, nrow(rows) + 0.7), expand = expansion(0)) +
    labs(x = "Odds ratio (log scale)   \u2190 lower odds        higher odds \u2192") +
    theme_report() +
    theme(axis.text.y = element_text(face = ifelse(rows$kind[ord] == "h", "bold", "plain")))
}

fig_fy <- function(oc){
  col <- COLORS[[oc]]
  t <- R$fy %>% filter(outcome == oc) %>% arrange(fiscal_year)
  d <- bind_rows(tibble(fiscal_year = FY_FIRST, or = 1, lo = 1, hi = 1), t %>% select(fiscal_year, or, lo, hi))
  pts <- list(SR = c(`2020` = "above", `2024` = "above", `2025` = "below"), PB = c(`2019` = "below", `2023` = "below", `2025` = "above"))[[if(oc == SR) "SR" else "PB"]]
  lab <- tibble(fiscal_year = as.integer(names(pts)), pos = unname(pts)) %>% inner_join(d, by = "fiscal_year")
  ticks <- c(0.7, 0.8, 0.9, 1, 1.25, 1.5)
  ggplot(d, aes(fiscal_year, or)) +
    geom_hline(yintercept = 1, linetype = dashed, colour = INK, linewidth = 0.4) +
    geom_ribbon(aes(ymin = lo, ymax = hi), fill = col, alpha = 0.16) +
    geom_line(colour = col, linewidth = 0.75) +
    geom_point(shape = 21, size = 2.3, fill = col, colour = "white", stroke = 0.4) +
    geom_text(data = lab, aes(label = sprintf("%.2f", or), vjust = ifelse(pos == "above", -0.9, 1.9)), size = pt(8), fontface = "bold", family = FONT, colour = INK) +
    annotate("text", x = -Inf, y = Inf, label = "Above 1: higher odds than in FY2014", hjust = -0.02, vjust = 1.6, size = pt(7.5), colour = MUTED, family = FONT) +
    annotate("text", x = -Inf, y = 0.62, label = "Below 1: lower odds than in FY2014", hjust = -0.02, vjust = -0.8, size = pt(7.5), colour = MUTED, family = FONT) +
    scale_x_continuous(breaks = d$fiscal_year, labels = sprintf("FY%02d", d$fiscal_year %% 100), expand = expansion(add = 0.4)) +
    scale_y_log10(limits = c(0.62, 1.85), breaks = ticks, labels = ticks, expand = expansion(0)) +
    labs(x = NULL, y = "Odds ratio vs. FY2014 (log scale)") +
    theme_report() +
    theme(panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = GRID, linewidth = 0.3),
          axis.line.x = element_blank(), axis.ticks.x = element_blank(), axis.title.y = element_text(size = 8.2, angle = 90))
}

# Horizontal bars with a value label and an optional dotted reference line
bar_panel <- function(d, col, xmax, ref = NULL, ref_label = NULL, title = NULL){
  p <- ggplot(d, aes(rate, factor(group, levels = group))) +
    geom_col(width = 0.58, fill = col)
  if(!is.null(ref)) p <- p +
    geom_vline(xintercept = ref, linetype = dashed, colour = INK, linewidth = 0.4) +
    annotate("text", x = ref, y = Inf, label = ref_label, vjust = -0.5, size = pt(7.4), colour = MUTED, family = FONT)
  p +
    geom_label(aes(x = rate + xmax * 0.012, label = pct(rate)), hjust = 0, size = pt(8), colour = INK, family = FONT,
               fill = "white", label.size = 0, label.padding = unit(0.6, "pt"), label.r = unit(0, "pt")) +
    scale_x_continuous(limits = c(0, xmax), breaks = scales::breaks_pretty(4), labels = pct_axis, expand = expansion(0)) +
    coord_cartesian(clip = "off") +
    labs(x = NULL, title = title) +
    theme_report() +
    theme(plot.margin = margin(14, 10, 4, 4), plot.title = element_text(face = "bold", size = 9, margin = margin(b = 12)))
}

group_rates <- function(dim, oc, order = NULL, rename = NULL){
  t <- R$obs_group %>% filter(dimension == dim) %>%
    mutate(rate = if(oc == SR) sr_events / sr_denom else pb_events / pb_denom, group = if(is.null(rename)) group else coalesce(unname(rename[group]), group))
  if(is.null(order)) arrange(t, rate) else t %>% slice(match(order, group))
}
sentence_case <- function(g){ g <- paste0(toupper(substr(g, 1, 1)), tolower(substring(g, 2))); ifelse(g == "Other", "Other offense", g) }

fig_observed <- function(oc){
  col <- COLORS[[oc]]
  allr <- group_rate("all", "All", oc)
  t <- group_rates("offense", oc) %>% mutate(group = sentence_case(group))
  p1 <- bar_panel(t, col, if(oc == SR) 1.17 else max(t$rate) * 1.3, ref = allr, ref_label = paste("All offenses", pct(allr)), title = "By offense type")
  if(oc == SR){
    p1 <- p1 + scale_x_continuous(limits = c(0, 1.17), breaks = c(0, 0.5, 1), labels = pct_axis, expand = expansion(0))
    t2 <- R$obs_group %>% filter(dimension == "race_by_citizenship") %>%
      mutate(rate = sr_events / sr_denom, race = str_remove(group, " \\|.*"), cit = str_remove(group, ".*\\| "),
             y = ifelse(cit == "U.S. Citizen", 1, 0) + ifelse(race == "White", 0.13, -0.13))
    seg <- t2 %>% group_by(cit) %>% summarize(lo = min(rate), hi = max(rate), y = ifelse(first(cit) == "U.S. Citizen", 1, 0), .groups = "drop")
    p2 <- ggplot() +
      geom_segment(data = seg, aes(x = lo, xend = hi, y = y, yend = y), colour = LINK, linewidth = 0.9) +
      geom_point(data = t2, aes(rate, y, shape = race), fill = col, colour = "white", size = 2.9, stroke = 0.4) +
      geom_text(data = t2, aes(rate + 0.018, y, label = pct(rate, 1)), hjust = 0, size = pt(8), colour = INK, family = FONT) +
      scale_shape_manual(values = c(White = 21, Hispanic = 23), breaks = c("White", "Hispanic")) +
      scale_y_continuous(breaks = c(1, 0), labels = c("U.S. citizens", "Non-U.S. citizens"), limits = c(-0.6, 1.6), expand = expansion(0)) +
      scale_x_continuous(limits = c(0.5, 1.08), breaks = c(0.6, 0.8, 1), labels = pct_axis, expand = expansion(0)) +
      labs(x = NULL, title = "By citizenship and race") +
      theme_report() + theme(plot.margin = margin(14, 10, 4, 4))
  } else {
    order2 <- c("Less than H.S.", "H.S. graduate", "Some college", "College graduate")
    t2 <- group_rates("education", oc, order = order2,
                      rename = c("Less Than H.S. Graduate" = "Less than H.S.", "H.S. Graduate" = "H.S. graduate", "Some College" = "Some college", "College Graduate" = "College graduate")) %>%
      slice(rev(seq_len(n())))
    p2 <- bar_panel(t2, col, max(t2$rate) * 1.3, ref = allr, ref_label = paste("All", pct(allr)), title = "By education")
  }
  p1 + p2 + plot_layout(widths = c(1.05, 1), guides = "collect") & theme(legend.position = "bottom")
}

icc_labels <- c("State baseline" = "State baseline", "District baseline (within state)" = "District baseline",
                "District trend over time" = "District trend over time", "National trend over time" = "National trend over time",
                "Individual-level variation (remainder)" = "Everything else")

fig_icc <- function(oc){
  col <- COLORS[[oc]]
  t <- R$icc %>% filter(outcome == oc, kind != "combined") %>% mutate(lab = icc_labels[component], rem = kind == "remainder")
  a <- t %>% filter(!is.na(primary)) %>% arrange(primary) %>% mutate(lab = factor(lab, levels = lab))
  pa <- ggplot(a, aes(primary, lab, fill = rem)) + geom_col(width = 0.5) +
    geom_text(aes(label = fmt_pct_label(primary), x = primary + 0.02), hjust = 0, size = pt(8), colour = INK, family = FONT) +
    scale_fill_manual(values = c(`TRUE` = GREY, `FALSE` = col), guide = "none") +
    scale_x_continuous(limits = c(0, 1.12), breaks = c(0, 0.25, 0.5, 0.75, 1), labels = pct_axis, expand = expansion(0)) +
    labs(x = NULL, title = "A. Main model") + theme_report()
  b <- t %>% arrange(ar1) %>% mutate(lab = factor(lab, levels = lab),
                                     bar = ifelse(rem, "Everything else (individual cases)", "Share of variation"),
                                     txt = paste0(fmt_pct_label(ar1), " (", fmt_pct_label(ar1_mice), ")"))
  pb <- ggplot(b, aes(y = lab)) +
    geom_col(aes(ar1, fill = bar), width = 0.5) +
    geom_point(aes(ar1_mice, shape = "After multiple imputation (in brackets)"), fill = "white", colour = INK, size = 2.6, stroke = 0.6) +
    geom_text(aes(x = pmax(ar1, ar1_mice) + 0.045, label = txt), hjust = 0, size = pt(8), colour = INK, family = FONT) +
    scale_fill_manual(values = c("Share of variation" = col, "Everything else (individual cases)" = GREY), breaks = "Share of variation") +
    scale_shape_manual(values = 23) +
    scale_x_continuous(limits = c(0, 1.12), breaks = c(0, 0.25, 0.5, 0.75, 1), labels = pct_axis, expand = expansion(0)) +
    labs(x = NULL, title = "B. Model with yearly trends (AR1)") + theme_report() +
    guides(fill = guide_legend(order = 1), shape = guide_legend(order = 2))
  (pa + theme(legend.position = "none")) + pb + plot_layout(widths = c(0.8, 1.35), guides = "collect") & theme(legend.position = "bottom")
}

fig_robust <- function(oc){
  col <- COLORS[[oc]]
  t <- R$rob %>% filter(outcome == oc)
  rows <- t %>% distinct(order, comparison) %>% arrange(order) %>% mutate(ypos = rev(seq_len(n())))
  aps <- intersect(c("Complete cases", "Missing as its own category", "Multiple imputation"), unique(t$approach))
  off <- if(length(aps) == 3) c(0.27, 0, -0.27) else c(0.17, -0.17)
  d <- t %>% inner_join(rows, by = c("order", "comparison")) %>% mutate(y = ypos + off[match(approach, aps)], approach = factor(approach, levels = aps))
  rng <- c(min(t$or) / 1.3, max(t$or) * 1.3)
  seg <- d %>% group_by(ypos) %>% summarize(lo = min(or), hi = max(or), .groups = "drop")
  ggplot() +
    geom_vline(xintercept = 1, linetype = dashed, colour = INK, linewidth = 0.4) +
    geom_segment(data = seg, aes(x = lo, xend = hi, y = ypos, yend = ypos), colour = LINK, linewidth = 0.55) +
    geom_point(data = d, aes(or, y, shape = approach), fill = col, colour = "white", size = 2.4, stroke = 0.3) +
    scale_shape_manual(values = c("Complete cases" = 21, "Missing as its own category" = 22, "Multiple imputation" = 23)[aps]) +
    scale_x_log10(limits = rng, breaks = log_ticks(rng), labels = log_ticks(rng), expand = expansion(0)) +
    scale_y_continuous(breaks = rows$ypos, labels = rows$comparison, limits = c(0.3, nrow(rows) + 0.7), expand = expansion(0)) +
    labs(x = "Odds ratio (log scale)") + theme_report()
}

fig_missing <- function(oc){
  col <- COLORS[[oc]]
  f <- R$miss_field %>% filter(outcome == oc) %>% arrange(pct) %>% mutate(field = factor(field, levels = field))
  pa <- ggplot(f, aes(pct / 100, field)) + geom_col(width = 0.5, fill = col) +
    geom_text(aes(x = pct / 100 + 0.002, label = sprintf("%.1f%%", pct)), hjust = 0, size = pt(8), colour = INK, family = FONT) +
    scale_x_continuous(limits = c(0, 0.09), breaks = scales::breaks_pretty(4), labels = pct_axis, expand = expansion(0)) +
    labs(x = NULL, title = "A. Rows with a value missing") + theme_report()
  e <- R$miss_edu %>% filter(outcome == oc)
  order <- tribble(~group, ~y, "U.S. citizen", 7, "Non-U.S. citizen", 6, "White", 4, "Black", 3, "Hispanic", 2, "Other", 1)
  d <- order %>% left_join(e %>% select(group, pct), by = "group") %>% mutate(v = pct / 100)
  axis <- tribble(~y, ~lab, 8, "By citizenship", 7, "U.S. citizen", 6, "Non-U.S. citizen", 5, "By race and ethnicity", 4, "White", 3, "Black", 2, "Hispanic", 1, "Other") %>% arrange(y)
  pb <- ggplot(d, aes(y = y)) +
    geom_segment(aes(x = 0, xend = v, yend = y), colour = LINK, linewidth = 0.55) +
    geom_point(aes(v), shape = 21, size = 2.8, fill = col, colour = "white", stroke = 0.3) +
    geom_text(aes(x = v + 0.006, label = sprintf("%.1f%%", pct)), hjust = 0, size = pt(8), colour = INK, family = FONT) +
    scale_y_continuous(breaks = axis$y, labels = axis$lab, limits = c(0.4, 8.6), expand = expansion(0)) +
    scale_x_continuous(limits = c(0, 0.2), breaks = scales::breaks_pretty(4), labels = pct_axis, expand = expansion(0)) +
    labs(x = NULL, title = "B. Rows with no education recorded") + theme_report() +
    theme(axis.text.y = element_text(face = ifelse(grepl("^By", axis$lab), "bold", "plain")))
  pa + pb + plot_layout(widths = c(1, 1.1))
}

# One-line worked example of odds versus percentages (supervised release report)
odds_example <- function(){
  o <- odds(SR, "Non-U.S. citizen")
  sprintf("Odds compare the chance that something happens with the chance that it does not. If 50 in 100 people receive an outcome, the odds are 1, and an odds ratio of 2 doubles them to 2, which is about 67 in 100 people, not 100 in 100. As an illustration, if 98 in 100 U.S. citizens received supervised release (odds of 49), an odds ratio of %s would give non-citizens odds of about %.1f, or about %.0f in 100. When an outcome is very common or very rare, a large odds ratio can be a small change in percentage points.",
          fmt_or(o), 49 * o, 100 * (49 * o) / (1 + 49 * o))
}
