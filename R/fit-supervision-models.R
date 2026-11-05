####Fit the supervised release and probation models####
# Fits the mixed-effects models behind the Modeling tab and the two PDF reports, and writes the results to
# data/modeling-results/*.csv. The dashboard (app.R) and the report files (reports/*.Rmd) read those CSV files.
#
# Run it from the project root (open the .Rproj), either by sourcing it or with:  Rscript R/fit-supervision-models.R
# It reads only files that are already in the repo, so it needs no database connection:
#   data-raw/aggregated-ussc-sentencing-data/aggregated_ussc_sentencing_data_po_office_<year>.csv   (counts of people by group and probation office)
#   data-raw/geo_crosswalk.csv                                                                      (probation office -> judicial district -> state)
# The aggregated rows are the same ones the models were first fitted on (supervised release: 487,798 rows, 459,570 of them
# with complete demographics and outside the Northern Mariana Islands; probation: 554,676 rows, 514,157 used).
#
# What it fits, for each outcome (supervised release among people eligible for it, probation among everyone sentenced):
#   1. Primary model: binomial mixed-effects model (glmmTMB) on the counts, complete cases, judicial district nested in state
#   2. The same model with fiscal year as a random effect instead of a fixed effect (only to compare AIC)
#   3. Yearly-trend (AR1) model: replaces the fiscal-year effect with a national and a district-level AR1 trend over time
#   4. Sensitivity model: missing demographics kept as their own category
#   5. Multiple imputation (miceRanger, 5 imputations): the primary and AR1 models re-fitted on each imputed data set and pooled
#
# Steps 3 and 5 are the slow ones (each AR1 model takes about 4 minutes per outcome, and multiple imputation fits the models five
# more times on each outcome and has not been timed), so both can be switched on or off below. If a step is switched off, the results from it that are already in data/modeling-results/ are kept.
#
# Needs: tidyverse, data.table, here, glmmTMB, and miceRanger (only when RUN_MICE is TRUE).
# Models are fitted with the "cond" part of glmmTMB. Odds ratios use Wald 95% intervals, as in the reports.

suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(here)
  library(glmmTMB)
})

####Settings####
# Each setting can also be given as an environment variable, e.g.  RUN_MICE=TRUE Rscript R/fit-supervision-models.R
RUN_AR1  <- as.logical(Sys.getenv("RUN_AR1", "TRUE"))     # yearly-trend (AR1) models, complete cases
RUN_MICE <- as.logical(Sys.getenv("RUN_MICE", "FALSE"))   # multiple imputation + pooled models; needs miceRanger and takes much longer
MICE_M   <- 5       # number of imputed data sets
MICE_SEED <- 123

out_dir  <- Sys.getenv("MODELING_RESULTS_DIR", here("data", "modeling-results"))   # where the results are read from and written to
raw_dir  <- here("data-raw", "aggregated-ussc-sentencing-data")

# What each outcome is modeled from: the count of events and the count of people who could have had the event
outcomes <- list(
  "Supervised release" = list(events = "supervised_release_flag", denom = "total_sentenced_fsr"),
  "Probation"          = list(events = "probation_sentence_flag", denom = "total_sentenced")
)
# For testing, FIT_OUTCOMES="Probation" fits one outcome only (the CSV files written then hold only that outcome)
fit_outcomes <- if(nzchar(Sys.getenv("FIT_OUTCOMES"))) strsplit(Sys.getenv("FIT_OUTCOMES"), ",")[[1]] else names(outcomes)

# Random-effects structure shared by the models
re_nested <- "(1 | state_name/state_district)"
re_ar1    <- "ar1(0 + year_f | dummy) + ar1(0 + year_f | state_district)"
covariates <- "age + newrace_description + monsex_description + newcit_description + neweduc_description + offense_category"
glmm_control <- glmmTMBControl(optCtrl = list(iter.max = 1000, eval.max = 1000))


####Read in the data####
read_supervision_data <- function(){
  files <- list.files(raw_dir, pattern = "po_office_\\d{4}\\.csv$", full.names = TRUE)
  stopifnot(length(files) > 0)

  crosswalk <-
    fread(here("data-raw", "geo_crosswalk.csv")) %>%
    distinct(po_office, state_name, state_district)

  df <-
    rbindlist(lapply(files, fread, na.strings = c("", "NA"))) %>%
    as_tibble() %>%
    filter(geo_level != "National Total") %>%                       # the file also holds national total rows
    rename(po_office = geo_level) %>%
    left_join(crosswalk, by = "po_office")

  if(anyNA(df$state_district)) stop("Some probation offices are not in data-raw/geo_crosswalk.csv: ",
                                    paste(unique(df$po_office[is.na(df$state_district)]), collapse = "; "))

  df %>%
    mutate(across(c(ends_with("_flag"), starts_with("total_sentenced")), ~ replace_na(as.numeric(.x), 0)),
           age = as.numeric(age),
           fiscal_year = factor(fiscal_year),                        # first level (FY2014) is the reference
           offense_category = factor(offense_category),              # first level (Drug Trafficking) is the reference
           newcit_description  = relevel(factor(newcit_description), ref = "U.S. Citizen"),
           monsex_description  = relevel(factor(monsex_description), ref = "Male"),
           neweduc_description = factor(neweduc_description,
                                        levels = c("Less Than H.S. Graduate", "H.S. Graduate", "Some College", "College Graduate")),
           newrace_description = factor(newrace_description, levels = c("White", "Black", "Hispanic", "Other")),
           state_name = factor(state_name),
           state_district = factor(state_district))
}

# One data set per outcome: only the rows where the outcome applies to someone
make_outcome_data <- function(df, oc){
  o <- outcomes[[oc]]
  df %>%
    filter(.data[[o$denom]] > 0) %>%
    select(age, newrace_description, monsex_description, newcit_description, neweduc_description, offense_category,
           fiscal_year, state_name, state_district, all_of(c(o$events, o$denom)))
}

demographics <- c("newrace_description", "monsex_description", "newcit_description", "neweduc_description")

complete_cases <- function(d){
  d %>%
    filter(!if_any(all_of(demographics), is.na)) %>%
    filter(state_district != "North Mariana Islands") %>%           # too few cases to estimate
    mutate(state_district = droplevels(state_district))
}

# Missing demographics kept as their own category (sensitivity analysis)
missing_as_category <- function(d){
  d %>% mutate(across(all_of(demographics), ~ fct_na_value_to_level(.x, level = "Unknown/Missing")))
}

add_ar1_terms <- function(d){
  d %>% mutate(year_f = factor(fiscal_year, levels = sort(unique(as.character(fiscal_year)))), dummy = factor(1))
}


####Fit the models####
model_formula <- function(oc, fixed_year = TRUE, ar1 = FALSE){
  o <- outcomes[[oc]]
  rhs <- c(covariates,
           if(fixed_year && !ar1) "fiscal_year",
           re_nested,
           if(ar1) re_ar1,
           if(!fixed_year && !ar1) "(1 | fiscal_year)")
  as.formula(paste0("cbind(", o$events, ", ", o$denom, " - ", o$events, ") ~ ", paste(rhs, collapse = " + ")))
}

fit_model <- function(oc, data, ..., control = glmmTMBControl()){
  cat("  fitting", oc, "model on", format(nrow(data), big.mark = ","), "rows -", format(Sys.time(), "%H:%M:%S"), "\n")
  glmmTMB(model_formula(oc, ...), data = data, family = binomial(link = "logit"), control = control)
}

# Fixed effects as odds ratios with Wald 95% intervals
tidy_fixed <- function(model){
  cf <- summary(model)$coefficients$cond
  tibble(term = rownames(cf), estimate = cf[, "Estimate"], se = cf[, "Std. Error"], p_value = cf[, "Pr(>|z|)"]) %>%
    mutate(or = exp(estimate), lo = exp(estimate - qnorm(0.975) * se), hi = exp(estimate + qnorm(0.975) * se))
}

# Pool fixed effects over imputed data sets (Rubin's rules)
pool_fixed <- function(models){
  bind_rows(lapply(models, function(m){ tidy_fixed(m) %>% select(term, estimate, se) }), .id = "imputation") %>%
    group_by(term) %>%
    summarize(estimate_pooled = mean(estimate),
              within_var = mean(se^2),
              between_var = var(estimate),
              se = sqrt(within_var + (1 + 1 / n()) * between_var),
              .groups = "drop") %>%
    mutate(estimate = estimate_pooled, p_value = 2 * pnorm(abs(estimate / se), lower.tail = FALSE),
           or = exp(estimate), lo = exp(estimate - qnorm(0.975) * se), hi = exp(estimate + qnorm(0.975) * se)) %>%
    select(term, estimate, se, p_value, or, lo, hi)
}

# Standard deviations (and AR1 correlations) of the random effects
random_effect_summary <- function(model){
  vc <- VarCorr(model)$cond
  sd1  <- function(g) attr(vc[[g]], "stddev")[[1]]
  cor1 <- function(g) attr(vc[[g]], "correlation")[1, 2]
  out <- c(sd_district_nested = sd1("state_district:state_name"), sd_state = sd1("state_name"))
  if("dummy" %in% names(vc))
    out <- c(out, sd_national = sd1("dummy"), sd_district_ar1 = sd1("state_district"),
             cor_national = cor1("dummy"), cor_district = cor1("state_district"))
  out
}

# Share of the total variation (on the log-odds scale) tied to each random effect.
# Non-AR terms contribute SD^2, AR1 terms SD^2 / (1 - rho^2), and the binomial residual pi^2 / 3.
icc_from_components <- function(re){
  comps <- c("State baseline" = unname(re["sd_state"]^2),
             "District baseline (within state)" = unname(re["sd_district_nested"]^2))
  if("sd_national" %in% names(re)){
    comps <- c(comps,
               "District trend over time" = unname(re["sd_district_ar1"]^2 / (1 - re["cor_district"]^2)),
               "National trend over time" = unname(re["sd_national"]^2 / (1 - re["cor_national"]^2)))
  }
  total <- sum(comps) + pi^2 / 3
  share <- comps / total
  share <- c(share, "State and district baselines combined" = unname(share["State baseline"] + share["District baseline (within state)"]))
  if("sd_national" %in% names(re))
    share <- c(share, "District baseline plus district trend" = unname(share["District baseline (within state)"] + share["District trend over time"]))
  share
}


####Turn model output into the results tables####
# Labels for the comparisons shown in the app and the reports
term_labels <-
  tribble(
    ~term,                                      ~group,            ~label,              ~ref,
    "age",                                      "Age",             "Age (per year)",    NA,
    "newcit_descriptionNon-U.S. Citizen",       "Citizenship",     "Non-U.S. citizen",  "U.S. citizen",
    "monsex_descriptionFemale",                 "Gender",          "Female",            "male",
    "newrace_descriptionBlack",                 "Race/ethnicity",  "Black",             "White",
    "newrace_descriptionHispanic",              "Race/ethnicity",  "Hispanic",          "White",
    "newrace_descriptionOther",                 "Race/ethnicity",  "Other race",        "White",
    "neweduc_descriptionH.S. Graduate",         "Education",       "H.S. graduate",     "less than H.S.",
    "neweduc_descriptionSome College",          "Education",       "Some college",      "less than H.S.",
    "neweduc_descriptionCollege Graduate",      "Education",       "College graduate",  "less than H.S.",
    "offense_categoryFirearms",                 "Offense",         "Firearms",          "drug trafficking",
    "offense_categoryFraud",                    "Offense",         "Fraud",             "drug trafficking",
    "offense_categoryImmigration",              "Offense",         "Immigration",       "drug trafficking",
    "offense_categoryOther",                    "Offense",         "Other offense",     "drug trafficking",
    "offense_categorySexual Offense",           "Offense",         "Sexual offense",    "drug trafficking",
    "offense_categoryViolent Offense",          "Offense",         "Violent offense",   "drug trafficking"
  )

fmt_p <- function(p) ifelse(p < 0.001, "<0.001", sprintf("%.3f", p))

odds_ratio_table <- function(fixed, oc){
  term_labels %>%
    inner_join(fixed, by = "term") %>%
    transmute(outcome = oc, group, label, ref, or = round(or, 3), lo = round(lo, 3), hi = round(hi, 3), p_value = fmt_p(p_value))
}

fiscal_year_table <- function(fixed, oc){
  fixed %>%
    filter(str_starts(term, "fiscal_year")) %>%
    transmute(outcome = oc, fiscal_year = as.integer(str_remove(term, "fiscal_year")),
              or = round(or, 3), lo = round(lo, 3), hi = round(hi, 3), p_value = fmt_p(p_value))
}

# Comparison text used in the robustness table, e.g. "Hispanic vs. White" or "FY2024 vs. FY2014"
comparison_terms <- function(fy_terms){
  bind_rows(
    term_labels %>% transmute(term, comparison = ifelse(label == "Age (per year)", label, paste0(label, " vs. ", ref))),
    tibble(term = paste0("fiscal_year", fy_terms), comparison = paste0("FY", fy_terms, " vs. FY2014"))
  )
}

# Percent of rows with a missing value for each field
missing_tables <- function(d, oc){
  fields <- c("Education" = "neweduc_description", "Race and ethnicity" = "newrace_description",
              "Citizenship" = "newcit_description", "Gender" = "monsex_description")
  by_field <- tibble(outcome = oc, field = names(fields),
                     pct = sapply(fields, function(f) round(100 * mean(is.na(d[[f]])), 1)))
  pct_edu <- function(var, dimension, lab){
    d %>% filter(!is.na(.data[[var]])) %>%
      group_by(group = as.character(.data[[var]])) %>%
      summarize(pct = round(100 * mean(is.na(neweduc_description)), 1), .groups = "drop") %>%
      mutate(outcome = oc, dimension = dimension, group = lab[group])
  }
  by_group <- bind_rows(
    pct_edu("newcit_description", "Citizenship", c("U.S. Citizen" = "U.S. citizen", "Non-U.S. Citizen" = "Non-U.S. citizen")),
    pct_edu("newrace_description", "Race and ethnicity", c(White = "White", Black = "Black", Hispanic = "Hispanic", Other = "Other"))
  ) %>% select(outcome, dimension, group, pct)
  list(by_field = by_field, by_group = by_group)
}

# Unadjusted national rates, from the "National Total" rows of the state files (used for the descriptive charts in the reports)
observed_tables <- function(){
  files <- list.files(raw_dir, pattern = "state_name_\\d{4}\\.csv$", full.names = TRUE)
  cols <- c("fiscal_year", "offense_category", "newrace_description", "newcit_description", "monsex_description", "neweduc_description",
            "geo_level", "supervised_release_flag", "probation_sentence_flag", "total_sentenced", "total_sentenced_fsr")
  df <- rbindlist(lapply(files, fread, select = cols, na.strings = c("", "NA"))) %>% as_tibble() %>% filter(geo_level == "National Total")
  sums <- function(x) x %>% summarize(sr_events = sum(supervised_release_flag), sr_denom = sum(total_sentenced_fsr),
                                      pb_events = sum(probation_sentence_flag), pb_denom = sum(total_sentenced), .groups = "drop")
  by_year <- df %>% group_by(fiscal_year) %>% sums()
  one_dim <- function(dimension, col) df %>% filter(!is.na(.data[[col]])) %>% group_by(group = .data[[col]]) %>% sums() %>% mutate(dimension = dimension, .before = 1)
  race_cit <- df %>%
    filter(newrace_description %in% c("White", "Hispanic"), newcit_description %in% c("U.S. Citizen", "Non-U.S. Citizen")) %>%
    group_by(group = paste(newrace_description, newcit_description, sep = " | ")) %>% sums() %>% mutate(dimension = "race_by_citizenship", .before = 1)
  by_group <- bind_rows(one_dim("offense", "offense_category"), one_dim("education", "neweduc_description"),
                        one_dim("gender", "monsex_description"), one_dim("race", "newrace_description"),
                        one_dim("citizenship", "newcit_description"), race_cit,
                        df %>% sums() %>% mutate(dimension = "all", group = "All", .before = 1))
  list(by_year = by_year, by_group = by_group)
}


####Multiple imputation####
# Imputes missing race, gender, citizenship and education with random forests (miceRanger) and returns a list of completed data sets.
impute_missing <- function(d, m = MICE_M, seed = MICE_SEED){
  if(!requireNamespace("miceRanger", quietly = TRUE)) stop("Install the miceRanger package to run multiple imputation.")
  d <- d %>% filter(state_district != "North Mariana Islands") %>% mutate(state_district = droplevels(state_district))
  set.seed(seed)
  obj <- miceRanger::miceRanger(data = as.data.frame(d), m = m, maxiter = 5, parallel = FALSE, verbose = TRUE,
                                num.trees = 50, vars = demographics)
  miceRanger::completeData(obj)
}

fit_imputed <- function(oc, imputed, ar1 = FALSE){
  lapply(seq_along(imputed), function(i){
    cat("  imputed data set", i, "of", length(imputed), "-", format(Sys.time(), "%H:%M:%S"), "\n")
    dat <- if(ar1) add_ar1_terms(imputed[[i]]) else imputed[[i]]
    tryCatch(fit_model(oc, dat, ar1 = ar1, control = glmm_control), error = function(e){ message("  model failed: ", conditionMessage(e)); NULL })
  }) %>% purrr::compact()
}

# Average the random-effect summaries over the imputed data sets (shares of variation are then worked out from the averages)
pooled_random_effects <- function(models){
  unlist(bind_rows(lapply(models, function(m) as.list(random_effect_summary(m)))) %>% summarize(across(everything(), mean)))
}


####Put it all together####
fit_everything <- function(){
  cat("Reading data\n")
  df <- read_supervision_data()
  res <- list(odds = list(), fy = list(), icc = list(), robust = list(), miss_field = list(), miss_group = list(), miss_cat = list(), details = list())

  for(oc in fit_outcomes){
    cat("\n====", oc, "====\n")
    d_all  <- make_outcome_data(df, oc)
    d_comp <- complete_cases(d_all)
    cat("  rows:", format(nrow(d_all), big.mark = ","), "| complete cases (district-level, without the Northern Mariana Islands):", format(nrow(d_comp), big.mark = ","), "\n")

    mt <- missing_tables(d_all, oc)
    res$miss_field[[oc]] <- mt$by_field
    res$miss_group[[oc]] <- mt$by_group

    ## 1. Primary model
    m_primary <- fit_model(oc, d_comp)
    fixed <- tidy_fixed(m_primary)
    re1 <- random_effect_summary(m_primary)
    res$odds[[oc]] <- odds_ratio_table(fixed, oc)
    res$fy[[oc]]   <- fiscal_year_table(fixed, oc)
    icc_primary <- icc_from_components(re1)
    cat("  AIC:", round(AIC(m_primary), 1), "| SD state", round(re1["sd_state"], 3), "| SD district", round(re1["sd_district_nested"], 3), "\n")

    ## 2. Fiscal year as a random effect (AIC comparison only)
    m_random_year <- fit_model(oc, d_comp, fixed_year = FALSE)
    aic_random <- AIC(m_random_year)

    ## 3. Yearly-trend (AR1) model
    icc_ar1 <- NULL; aic_ar1 <- NA_real_; re_ar1_sum <- NULL
    if(RUN_AR1){
      m_ar1 <- fit_model(oc, add_ar1_terms(d_comp), ar1 = TRUE, control = glmm_control)
      re_ar1_sum <- random_effect_summary(m_ar1)
      icc_ar1 <- icc_from_components(re_ar1_sum)
      aic_ar1 <- AIC(m_ar1)
    }

    ## 4. Missing demographics as their own category
    fixed_cat <- tidy_fixed(fit_model(oc, missing_as_category(d_all)))
    res$miss_cat[[oc]] <- fixed_cat %>% transmute(outcome = oc, term, or = round(or, 3), lo = round(lo, 3), hi = round(hi, 3), p_value = fmt_p(p_value))

    ## 5. Multiple imputation
    fixed_mi <- NULL; icc_mi <- NULL; re_mi <- NULL
    if(RUN_MICE){
      imputed <- impute_missing(d_all)
      fixed_mi <- pool_fixed(fit_imputed(oc, imputed))
      re_mi    <- pooled_random_effects(fit_imputed(oc, imputed, ar1 = TRUE))
      icc_mi   <- icc_from_components(re_mi)
    }

    ## ICC table (long)
    components <- c("State baseline", "District baseline (within state)", "State and district baselines combined",
                    "District trend over time", "National trend over time", "District baseline plus district trend")
    pick <- function(x, k) if(is.null(x) || !k %in% names(x)) NA_real_ else unname(x[k])
    icc_tbl <- tibble(outcome = oc, component = components,
                      primary  = sapply(components, function(k) pick(icc_primary, k)),
                      ar1      = sapply(components, function(k) pick(icc_ar1, k)),
                      ar1_mice = sapply(components, function(k) pick(icc_mi, k)),
                      kind = ifelse(str_detect(components, "combined|plus"), "combined", "component"))
    # the remainder is what is left over once the separate components are taken out
    rem <- function(col){
      v <- icc_tbl[[col]][icc_tbl$kind == "component"]
      if(all(is.na(v))) NA_real_ else 1 - sum(v, na.rm = TRUE)
    }
    res$icc[[oc]] <- bind_rows(icc_tbl, tibble(outcome = oc, component = "Individual-level variation (remainder)",
                                               primary = rem("primary"), ar1 = rem("ar1"), ar1_mice = rem("ar1_mice"), kind = "remainder")) %>%
      mutate(across(c(primary, ar1, ar1_mice), ~ round(.x, 3)))

    ## Robustness table (numbers only; the judgement labels come from robustness_labels.csv)
    fy_terms <- str_remove(grep("^fiscal_year", fixed$term, value = TRUE), "fiscal_year")
    ct <- comparison_terms(fy_terms)
    pull_or <- function(f, approach) if(is.null(f)) NULL else
      ct %>% inner_join(f, by = "term") %>% transmute(outcome = oc, comparison, approach = approach, or = or, p_value = p_value)
    res$robust[[oc]] <- bind_rows(pull_or(fixed, "Complete cases"), pull_or(fixed_cat, "Missing as its own category"),
                                  pull_or(fixed_mi, "Multiple imputation"))

    ## Model details quoted in the reports (complete-case value, and the value after multiple imputation where there is one)
    miss_any <- d_all %>% filter(if_any(all_of(demographics), is.na))
    ok_any   <- d_all %>% filter(!if_any(all_of(demographics), is.na))
    denom_col <- outcomes[[oc]]$denom
    share_imm <- function(x) sum(x[[denom_col]][x$offense_category == "Immigration"]) / sum(x[[denom_col]])
    one <- function(metric, complete, imputed = NA_real_) tibble(outcome = oc, metric = metric, complete = unname(complete), imputed = unname(imputed))
    res$details[[oc]] <- bind_rows(
      one("n_eligible", nrow(d_all)),
      one("n_complete", nrow(d_comp)),
      one("complete_pct", round(100 * nrow(d_comp) / nrow(d_all), 1)),
      one("sd_state", round(re1["sd_state"], 3)),
      one("sd_district", round(re1["sd_district_nested"], 3)),
      one("aic_fixed_year", round(AIC(m_primary), 1)),
      one("aic_random_year", round(aic_random, 1)),
      one("aic_ar1", round(aic_ar1, 1)),
      one("ar1_sd_state",          round(pick(re_ar1_sum, "sd_state"), 3),           round(pick(re_mi, "sd_state"), 3)),
      one("ar1_sd_district",       round(pick(re_ar1_sum, "sd_district_nested"), 3), round(pick(re_mi, "sd_district_nested"), 3)),
      one("ar1_sd_national",       round(pick(re_ar1_sum, "sd_national"), 3),        round(pick(re_mi, "sd_national"), 3)),
      one("ar1_sd_district_trend", round(pick(re_ar1_sum, "sd_district_ar1"), 3),    round(pick(re_mi, "sd_district_ar1"), 3)),
      one("ar1_rho_national",      round(pick(re_ar1_sum, "cor_national"), 3),       round(pick(re_mi, "cor_national"), 3)),
      one("ar1_rho_district",      round(pick(re_ar1_sum, "cor_district"), 3),       round(pick(re_mi, "cor_district"), 3)),
      # who has missing demographics, and how many people each row stands for
      one("immigration_share_people_missing_demographics", round(100 * share_imm(miss_any), 1)),
      one("immigration_share_people_complete",             round(100 * share_imm(ok_any), 1)),
      one("rows_single_person_pct", round(100 * mean(d_comp[[denom_col]] == 1), 1)),
      one("rows_denominator_max",   max(d_comp[[denom_col]])),
      one("rows_denominator_mean",  round(mean(d_comp[[denom_col]]), 2)))
  }
  res
}

# Combine the new results with what is already on disk: anything a switched-off step did not compute is kept
read_existing <- function(file, ...) {
  p <- file.path(out_dir, file)
  if(file.exists(p)) readr::read_csv(p, show_col_types = FALSE, ...) else NULL
}

write_results <- function(res){
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  # When only some outcomes were fitted (FIT_OUTCOMES), the rows of the other outcomes already in the file are kept
  w <- function(x, file){
    old <- read_existing(file, col_types = readr::cols(.default = readr::col_character()))
    if(!is.null(old) && nrow(x) > 0 && "outcome" %in% names(old)){
      others <- old %>% filter(!outcome %in% unique(x$outcome))
      if(nrow(others) > 0){
        x <- bind_rows(others, mutate(x, across(everything(), as.character))) %>%
          arrange(match(outcome, names(outcomes)))      # written as text, so pre-formatted values such as "1.010" keep their zeros
      }
    }
    readr::write_csv(x, file.path(out_dir, file), na = "NA")
  }

  w(bind_rows(res$odds), "odds_ratios.csv")
  w(bind_rows(res$fy), "fiscal_year_odds_ratios.csv")
  w(bind_rows(res$miss_field), "missing_by_field.csv")
  w(bind_rows(res$miss_group), "missing_education_by_group.csv")

  ## ICC: keep the multiple-imputation (and AR1) columns from the existing file when those steps were skipped
  icc <- bind_rows(res$icc)
  old <- read_existing("icc.csv")
  if(!is.null(old)){
    for(col in c("ar1", "ar1_mice")){
      if(all(is.na(icc[[col]]))){
        icc[[col]] <- old[[col]][match(paste(icc$outcome, icc$component), paste(old$outcome, old$component))]
        message("Kept the existing '", col, "' column of icc.csv (step switched off)")
      }
    }
  }
  w(icc, "icc.csv")

  ## Robustness: numbers from the models, labels and order from robustness_labels.csv
  labels <- read_existing("robustness_labels.csv")
  rb <- bind_rows(res$robust) %>% mutate(note = NA_character_)
  old <- read_existing("robustness.csv", col_types = readr::cols(display = readr::col_character(), note = readr::col_character(), .default = readr::col_guess()))
  rb$display <- NA_character_
  if(!any(rb$approach == "Multiple imputation") && !is.null(old)){
    keep <- old %>% filter(approach == "Multiple imputation", outcome %in% unique(rb$outcome)) %>% mutate(p_value = NA_real_) %>%
      select(outcome, comparison, approach, or, p_value, note, display)
    rb <- bind_rows(rb, keep)
    message("Kept the existing multiple-imputation rows of robustness.csv (step switched off)")
  }
  # a value is flagged when it is not significant but the complete-case value is
  cc_sig <- rb %>% filter(approach == "Complete cases") %>% transmute(outcome, comparison, cc_sig = p_value < 0.05)
  rb <- rb %>%
    left_join(cc_sig, by = c("outcome", "comparison")) %>%
    mutate(note = if_else(is.na(note) & !is.na(p_value) & p_value >= 0.05 & cc_sig %in% TRUE, "not significant", note),
           or = round(or, 3), display = coalesce(display, sprintf("%.3f", or))) %>%
    inner_join(labels %>% select(outcome, order, comparison, result_kind, result_label), by = c("outcome", "comparison")) %>%
    arrange(outcome, order, match(approach, c("Complete cases", "Missing as its own category", "Multiple imputation"))) %>%
    # the probation report compares complete cases with multiple imputation only
    filter(!(outcome == "Probation" & approach == "Missing as its own category")) %>%
    select(outcome, order, comparison, approach, or, note, display, result_kind, result_label)
  w(rb, "robustness.csv")

  ## Model details: keep the imputed values from the existing file when multiple imputation was skipped
  det <- bind_rows(res$details)
  old <- read_existing("model_details.csv")
  if(!is.null(old) && all(is.na(det$imputed))){
    det$imputed <- old$imputed[match(paste(det$outcome, det$metric), paste(old$outcome, old$metric))]
    message("Kept the existing 'imputed' column of model_details.csv (step switched off)")
  }
  w(det, "model_details.csv")

  ## Odds ratios for every term when missing demographics are kept as their own category
  w(bind_rows(res$miss_cat), "missing_category_odds_ratios.csv")
}

# (set the environment variable FIT_SUPERVISION_MODELS to "skip" to load the functions without running anything)
if(!identical(Sys.getenv("FIT_SUPERVISION_MODELS"), "skip")){
  tictoc_start <- Sys.time()
  results <- fit_everything()
  # If writing fails, keep the fitted results so the (slow) models do not have to be run again
  tryCatch(write_results(results), error = function(e){
    saveRDS(results, file.path(out_dir, "fit-results-unsaved.rds"))
    message("Writing the CSV files failed. The fitted results were saved to ", file.path(out_dir, "fit-results-unsaved.rds"), " (delete it when you are done).")
    stop(e)
  })
  obs <- observed_tables()
  readr::write_csv(obs$by_year,  file.path(out_dir, "observed_national_by_year.csv"))
  readr::write_csv(obs$by_group, file.path(out_dir, "observed_by_group.csv"))
  cat("\nDone in", round(difftime(Sys.time(), tictoc_start, units = "mins"), 1), "minutes. Results are in", out_dir, "\n")
}
