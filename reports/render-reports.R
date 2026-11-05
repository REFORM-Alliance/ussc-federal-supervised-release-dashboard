####Render the two analysis reports to PDF####
# Knits supervised-release-report.Rmd and probation-report.Rmd to HTML, then prints each page to PDF with headless Chrome.
# The PDFs are written to www/reports/, where the dashboard's Modeling tab links to them.
#
#   Rscript reports/render-reports.R                  # both reports
#   Rscript reports/render-reports.R probation        # one report (name contains the text you give)
#   Rscript reports/render-reports.R --keep-html      # also leave the knitted HTML next to the Rmd files
#
# Needs: the R packages rmarkdown, knitr, tidyverse, patchwork, svglite, here (pandoc comes with RStudio),
# and Google Chrome or Chromium 131 or newer (for the page numbers). If Chrome is not found automatically,
# set CHROME_BIN to its path. The Inter font is used if installed; otherwise the browser falls back to Helvetica or Arial.
# To refresh the numbers first, run R/fit-supervision-models.R (it rewrites data/modeling-results/).

suppressPackageStartupMessages(library(here))

# The figures contain a few non-ASCII characters (arrows, a dagger), which need a UTF-8 locale.
# RStudio and current R on Windows and macOS already use one; on a bare Linux shell it may be "C".
if(!isTRUE(l10n_info()[["UTF-8"]])) try(Sys.setlocale("LC_CTYPE", "C.UTF-8"), silent = TRUE)
if(!isTRUE(l10n_info()[["UTF-8"]])) warning("R is not running in a UTF-8 locale, so arrows and daggers in the figures may not print correctly.")

args <- commandArgs(trailingOnly = TRUE)
keep_html <- "--keep-html" %in% args
only <- setdiff(args, "--keep-html")

reports_dir <- here("reports")
out_dir <- Sys.getenv("REPORTS_OUT_DIR", here("www", "reports"))   # REPORTS_OUT_DIR lets you write the PDFs somewhere else, for testing
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

reports <- c("supervised-release-report", "probation-report")
if(length(only) > 0) reports <- reports[Reduce(`|`, lapply(only, function(x) grepl(x, reports, fixed = TRUE)))]
if(length(reports) == 0) stop("No report matches: ", paste(only, collapse = ", "))

####Find Chrome####
find_chrome <- function(){
  env <- Sys.getenv("CHROME_BIN")
  if(nzchar(env) && file.exists(env)) return(env)
  on_path <- Sys.which(c("google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"))
  on_path <- on_path[nzchar(on_path)]
  if(length(on_path) > 0) return(unname(on_path[1]))
  known <- c("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
             "/Applications/Chromium.app/Contents/MacOS/Chromium",
             "C:/Program Files/Google/Chrome/Application/chrome.exe",
             "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
             "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe")
  known <- known[file.exists(known)]
  if(length(known) > 0) return(known[1])
  stop("Could not find Chrome or Chromium. Install it, or set CHROME_BIN to the program's path.")
}

# Chrome's own printing: no browser header or footer (the report draws its own with CSS), tagged PDF with an outline
html_to_pdf <- function(html, pdf, chrome = find_chrome()){
  url <- paste0("file://", ifelse(startsWith(html, "/"), "", "/"), html)
  flags <- c("--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--export-tagged-pdf", "--generate-pdf-document-outline",
             "--run-all-compositor-stages-before-draw", "--virtual-time-budget=20000",
             # Chrome refuses to start as root (for example in a container) without this
             if(identical(Sys.info()[["user"]], "root")) "--no-sandbox",
             paste0("--print-to-pdf=", pdf), url)
  status <- system2(chrome, shQuote(flags), stdout = FALSE, stderr = FALSE)
  if(!identical(status, 0L) || !file.exists(pdf)) stop("Chrome could not print ", basename(html), " to PDF (exit status ", status, ").")
  invisible(pdf)
}

####Knit and print####
for(r in reports){
  html <- file.path(if(keep_html) reports_dir else tempdir(), paste0(r, ".html"))
  message("Knitting ", r, " ...")
  rmarkdown::render(file.path(reports_dir, paste0(r, ".Rmd")), output_file = basename(html), output_dir = dirname(html), quiet = TRUE,
                    envir = new.env(parent = globalenv()))
  pdf <- file.path(out_dir, paste0(r, ".pdf"))
  html_to_pdf(normalizePath(html, winslash = "/"), normalizePath(pdf, winslash = "/", mustWork = FALSE))
  message("  wrote ", pdf)
}
