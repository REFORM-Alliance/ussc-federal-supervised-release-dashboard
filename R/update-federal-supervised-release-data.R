rm(list = ls())


####Read in Libraries####
library(tidyverse)
library(data.table)
library(janitor)
library(here)
library(tictoc)
library(SAScii)
library(DBI)
library(dbplyr)
library(RPostgres)
library(glue)
library(tidygeocoder)


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


####Source Utils Functions####
source(here("R", "utils-functions.R"))


####Scrape New Files####
##Federal Individual Sentencing Data
base_url <- "https://www.ussc.gov/sites/default/files/zip/opafy{year}nid_csv.zip"
base_filename <- 
  "data-raw" %>% 
  here("federal-individual-sentencing-data") %>% 
  paste0("/opafy{year}nid_csv.zip")

years_to_check <- 
  c(2014:year(Sys.Date())) %>% 
  as.character() %>% 
  str_sub(3, 4) %>% 
  setdiff("data-raw" %>% 
            here("federal-individual-sentencing-data") %>% 
            list.files(full.names = FALSE) %>% 
            str_remove("^opafy") %>% 
            str_remove("nid.zip$") %>% 
            str_remove("nid_csv.zip$") %>% 
            unique())

years_to_check %>% 
  map(~base_url %>% 
        glue(year = .x) %>% 
        download.file(destfile = 
                        base_filename %>% 
                        glue(year = .x), 
                      quiet = TRUE) %>% 
        tryCatch(error = function(e) FALSE) %>% 
        suppressWarnings())

##Federal Criminal History Data
base_url_crim_hist <- "https://www.ussc.gov/sites/default/files/zip/crimhist{year}nid_csv.zip"
base_filename_crim_hist <- 
  "data-raw" %>% 
  here("federal-criminal-history-data") %>% 
  paste0("/crimhist{year}nid_csv.zip")

years_to_check_crim_hist <- 
  c(2016:year(Sys.Date())) %>% 
  as.character() %>% 
  str_sub(3, 4) %>% 
  setdiff("data-raw" %>% 
            here("federal-criminal-history-data") %>% 
            list.files(full.names = FALSE) %>% 
            str_remove("^crimhist") %>% 
            str_remove("nid.zip$") %>% 
            str_remove("nid_csv.zip$") %>% 
            unique())

years_to_check_crim_hist %>% 
  map(~base_url_crim_hist %>% 
        glue(year = .x) %>% 
        download.file(destfile = 
                        base_filename_crim_hist %>% 
                        glue(year = .x), 
                      quiet = TRUE) %>% 
        tryCatch(error = function(e) FALSE) %>% 
        suppressWarnings())


####Read in Data####
##Find Years to Read
years_to_read <- 
  c(2016:year(Sys.Date())) %>% 
  setdiff(con %>% 
            tbl(in_schema("ussc_federal_data", "sentencing_data_full")) %>% 
            dplyr::select(fiscal_year) %>% 
            distinct() %>% 
            collect() %>% 
            pull(fiscal_year))

##Individual Sentencing Data
years_to_read %>%
  map(~"data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("opafy") %>% 
        str_subset("csv.zip$") %>% 
        str_subset(str_sub(.x, 3, 4))) %>% 
  unlist() %>% 
  map(~{
    file = .x
    
    short_year = 
      file %>% 
      str_replace_all(getwd(), "") %>% 
      str_replace_all("^/data-raw/federal-individual-sentencing-data/opafy", "") %>% 
      str_sub(1, 2)
    
    year = paste0("20", short_year)
    
    dir <- 
      "data-raw" %>% 
      here("federal-individual-sentencing-data") %>% 
      here(year)
    
    if(dir.exists(dir) == TRUE){
      unlink(dir, recursive = TRUE)
    }
    
    dir %>% 
      dir.create()
    
    if(year >= 2024){
      file %>% 
        unzip(exdir = 
                "data-raw" %>% 
                here("federal-individual-sentencing-data") %>% 
                here(year))
      
      df_raw <- 
        "data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.csv]$") %>% 
        fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
        clean_names() %>% 
        mutate(across(everything(),
                      ~.x %>% 
                        as.character())) %>% 
        dplyr::select(any_of(c("usscidn", "offguide", "circdist", "citizen", "citwhere", 
                               "district", "educatn", "hisporig", "moncirc", "monrace", 
                               "monsex", "newcit", "neweduc", "newrace", "numdepen", 
                               "pooffice", "years", "suprdum", "suprel", "supermax", 
                               "supermin", "senspcap")), 
                      starts_with("dob"), starts_with("age"),
                      starts_with("depart"), starts_with("reas"),
                      starts_with("sent"), starts_with("crim"),
                      starts_with("ofbeg"), starts_with("ofend"), 
                      starts_with("offtyp"), starts_with("prob"),
                      starts_with("tot"), starts_with("timeserv"), 
                      starts_with("combdrg"), ends_with("_crim_hist"), 
                      starts_with("nwstat")) %>% 
        mutate(fiscal_year = year)
      
      safe_insert_table(con = con, df = df_raw, table_name = "federal_individual_sentencing_data", schema = "ussc_federal_data")
    }else{
      file %>% 
        unzip(exdir = 
                "data-raw" %>% 
                here("federal-individual-sentencing-data") %>% 
                here(year))
      
      sas_data_parsed <- 
        "data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.]sas$") %>% 
        readLines() %>% 
        as.data.frame()  %>% 
        clean_names() %>% 
        mutate(input = 
                 x %>% 
                 str_detect("^\\s*INPUT") %>% 
                 as.numeric() %>% 
                 cumsum() %>% 
                 lag(1),
               length = 
                 x %>% 
                 str_detect("^\\s*LENGTH") %>% 
                 as.numeric() %>% 
                 cumsum(),
               label = 
                 x %>% 
                 str_detect("^\\s*LABEL") %>% 
                 as.numeric() %>% 
                 cumsum()) %>% 
        filter(input == 1, length == 0, label == 0) %>% 
        mutate(x = 
                 x %>% 
                 str_replace_all("   ", ";") %>% 
                 str_replace_all(";{2,}", ";")) %>% 
        separate_rows(x, sep = ";") %>% 
        dplyr::select(-c(input, length)) %>% 
        mutate(x = 
                 x %>% 
                 str_trim(side = "both") %>% 
                 str_replace_all("\\$", " ") %>% 
                 str_replace_all(" {2,}", " ")) %>% 
        filter(x != "") %>% 
        separate(x, into = c("col_name", "start_end"), sep = " ") %>% 
        separate(start_end, into = c("start", "end"), sep = "-", fill = "right") %>% 
        mutate(end = ifelse(is.na(end) == TRUE, start, end), 
               across(c(end, start), 
                      ~.x %>% 
                        as.numeric()))
      
      colspecs <- 
        fwf_positions(start = sas_data_parsed$start, 
                      end = sas_data_parsed$end, 
                      col_names = sas_data_parsed$col_name)
      
      df_raw <- 
        "data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.dat]$") %>% 
        read_fwf(col_positions = colspecs, 
                 progress = TRUE, 
                 col_types = cols(.default = col_character())) %>% 
        clean_names() %>% 
        dplyr::select(any_of(c("usscidn", "offguide", "circdist", "citizen", "citwhere", 
                               "district", "educatn", "hisporig", "moncirc", "monrace", 
                               "monsex", "newcit", "neweduc", "newrace", "numdepen", 
                               "pooffice", "years", "suprdum", "suprel", "supermax", 
                               "supermin", "senspcap")), 
                      starts_with("dob"), starts_with("age"),
                      starts_with("depart"), starts_with("reas"),
                      starts_with("sent"), starts_with("crim"),
                      starts_with("ofbeg"), starts_with("ofend"), 
                      starts_with("offtyp"), starts_with("prob"),
                      starts_with("tot"), starts_with("timeserv"), 
                      starts_with("combdrg"), ends_with("_crim_hist"), 
                      starts_with("nwstat")) %>% 
        mutate(fiscal_year = year)
      
      safe_insert_table(con = con, df = df_raw, table_name = "federal_individual_sentencing_data", schema = "ussc_federal_data")
    }
    
    "data-raw" %>% 
      here("federal-individual-sentencing-data") %>% 
      here(year) %>% 
      unlink(recursive = TRUE)
    
    print(glue::glue("Individual Sentencing Data Read, Parsed, and Written in Database for USSC FY{short_year}"))
  })
  
##Criminal History Data Files
years_to_read %>%
  map(~"data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("crimhist") %>% 
        str_subset("csv.zip$") %>% 
        str_subset(str_sub(.x, 3, 4))) %>% 
  unlist() %>% 
  map(~{
    file = .x
    
    short_year <- 
      file %>% 
      str_replace_all(getwd(), "") %>% 
      str_replace_all("^/data-raw/federal-criminal-history-data/crimhist", "") %>% 
      str_sub(1, 2)
    
    year <- paste0("20", short_year)
    
    dir <- 
      "data-raw" %>% 
      here("federal-criminal-history-data") %>% 
      here(year)
    
    if(dir.exists(dir) == TRUE){
      unlink(dir, recursive = TRUE)
    }
    
    dir %>% 
      dir.create()
    
    file %>% 
      unzip(exdir = 
              "data-raw" %>% 
              here("federal-criminal-history-data") %>% 
              here(year))
    
    if(year < 2024){
      sas_data_parsed_crim <- 
        "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.]sps$") %>% 
        readLines() %>% 
        as.data.frame()  %>% 
        clean_names() %>% 
        mutate(x = 
                 x %>% 
                 str_replace_all("[(]A[)]", "") %>% 
                 str_replace_all("[(]DATE[)]", "") %>% 
                 str_replace_all("[.]$", ""),
               input = 
                 x %>% 
                 str_detect("^\\s*DATA LIST FILE") %>% 
                 as.numeric() %>% 
                 cumsum() %>% 
                 lag(1),
               length = 
                 x %>% 
                 str_detect("^\\s*LENGTH") %>% 
                 as.numeric() %>% 
                 cumsum(),
               label = 
                 x %>% 
                 str_detect("^\\s*VARIABLE LABELS") %>% 
                 as.numeric() %>% 
                 cumsum(), 
               save = 
                 x %>% 
                 str_detect("^\\s*SAVE") %>% 
                 as.numeric() %>% 
                 cumsum()) %>% 
        filter(input == 1, length == 0, label == 0, save == 0) %>% 
        mutate(x = 
                 x %>% 
                 str_replace_all("   ", ";") %>% 
                 str_replace_all(";{2,}", ";")) %>% 
        separate_rows(x, sep = ";") %>% 
        dplyr::select(-c(input, length)) %>% 
        mutate(x = 
                 x %>% 
                 str_trim(side = "both") %>% 
                 str_replace_all("\\$", " ") %>% 
                 str_replace_all(" {2,}", " ")) %>% 
        filter(x != "") %>% 
        separate(x, into = c("col_name", "start_end"), sep = " ") %>% 
        separate(start_end, into = c("start", "end"), sep = "-", fill = "right") %>% 
        mutate(end = ifelse(is.na(end) == TRUE, start, end), 
               across(c(end, start), 
                      ~.x %>% 
                        as.numeric()))
      
      colspecs_crim <- 
        fwf_positions(start = sas_data_parsed_crim$start, 
                      end = sas_data_parsed_crim$end, 
                      col_names = sas_data_parsed_crim$col_name)
      
      df_raw_crim <- 
        "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.dat]$") %>% 
        read_fwf(col_positions = colspecs_crim, 
                 progress = TRUE, 
                 col_types = cols(.default = col_character())) %>% 
        clean_names() %>% 
        mutate(fiscal_year = year,
               across(c(matches("\\d$")), 
                      ~.x %>% 
                        as.character())) %>% 
        pivot_longer(cols = matches("\\d$"), 
                     names_to = "col",
                     values_to = "val") %>% 
        mutate(col_num = 
                 col %>% 
                 str_extract_all("\\d+$") %>% 
                 as.numeric(), 
               col_clean = 
                 col %>% 
                 str_replace_all("\\d+$", "") %>% 
                 str_replace_all("_$", ""), 
               eventnum = 
                 eventnum %>% 
                 as.numeric()) %>% 
        filter(eventnum >= col_num) %>% 
        dplyr::select(-col) %>% 
        pivot_wider(names_from = "col_clean", 
                    values_from = "val") %>% 
        rename("total_eventnum" = "eventnum", 
               "eventnum" = "col_num") %>% 
        mutate(across(where(is.character), 
                      ~.x %>% 
                        iconv(from = "latin1", to = "UTF-8", sub = "")))
      
    }else{
      df_raw_crim <- 
        "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.csv]$") %>% 
        fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
        clean_names() %>% 
        mutate(fiscal_year = year,
               across(c(matches("\\d$")), 
                      ~.x %>% 
                        as.character())) %>% 
        pivot_longer(cols = matches("\\d$"), 
                     names_to = "col",
                     values_to = "val") %>% 
        mutate(col_num = 
                 col %>% 
                 str_extract_all("\\d+$") %>% 
                 as.numeric(), 
               col_clean = 
                 col %>% 
                 str_replace_all("\\d+$", "") %>% 
                 str_replace_all("_$", ""), 
               eventnum = 
                 eventnum %>% 
                 as.numeric()) %>% 
        filter(eventnum >= col_num) %>% 
        dplyr::select(-col) %>% 
        pivot_wider(names_from = "col_clean", 
                    values_from = "val") %>% 
        rename("total_eventnum" = "eventnum", 
               "eventnum" = "col_num") %>% 
        mutate(across(where(is.character), 
                      ~.x %>% 
                        iconv(from = "latin1", to = "UTF-8", sub = "")), 
               across(c("fiscal_year", "usscidn"), 
                      ~.x %>% 
                        as.character()))
    }
    
    "data-raw" %>% 
      here("federal-criminal-history-data") %>% 
      here(year) %>% 
      unlink(recursive = TRUE)
    
    safe_insert_table(con = con, df = df_raw_crim, table_name = "federal_criminal_history_data", schema = "ussc_federal_data")
    
    print(glue::glue("Individual Criminal History Data Read, Parsed, and Written in Database for USSC FY{short_year}"))
  })


####Write Data to DB####
##Merge Raw Datasets and Write to DB
merge_ussc_data_sql <- 
  con %>% 
  tbl(DBI::Id("ussc_federal_data", "federal_individual_sentencing_data")) %>%
  filter(fiscal_year %in% c("2014", "2015", "2016", "2017")) %>% 
  db_remove_empty_cols() %>% 
  left_join(con %>% 
              tbl(DBI::Id("ussc_federal_data", "federal_individual_sentencing_supplemental_data")) %>% 
              filter(fiscal_year %in% c("2014", "2015", "2016", "2017")) %>% 
              db_remove_empty_cols(), 
            by = c("usscidn", "fiscal_year")) %>% 
  mutate(across(any_of(c("offguide", "senspcap", "sentrnge", "senttcap")), 
                ~.x %>% 
                  as.double())) %>% 
  union_all(con %>% 
              tbl(DBI::Id("ussc_federal_data", "federal_individual_sentencing_data")) %>%
              mutate(fiscal_year = 
                       fiscal_year %>% 
                       as.numeric()) %>% 
              filter(fiscal_year > 2017) %>% 
              mutate(fiscal_year = 
                       fiscal_year %>% 
                       as.character(),
                     across(any_of(c("offguide", "senspcap", "sentrnge", "senttcap")), 
                            ~.x %>% 
                              as.double())) %>% 
              db_remove_empty_cols()) %>% 
  left_join(con %>% 
              tbl(DBI::Id("ussc_federal_data", "federal_criminal_history_data")) %>% 
              rename_with(~.x %>% 
                            paste0("_crim_hist") %>% 
                            str_replace_all("^usscidn_crim_hist$", "usscidn") %>% 
                            str_replace_all("^fiscal_year_crim_hist$", "fiscal_year")), 
            by = c("usscidn", "fiscal_year")) %>% 
  mutate(across(c("total_eventnum_crim_hist", "eventnum_crim_hist"), 
                ~ifelse(is.na(.x) == TRUE, "1", .x)),
         across(c(ends_with("dt"), ends_with("dt_crim_hist")),
                ~case_when(
                  str_detect(.x, "^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}$") == TRUE ~ TO_DATE(.x, "DD-Mon-YYYY"),
                  str_detect(.x, "^[0-9]{1,2}/[0-9]{4}$") == TRUE ~ TO_DATE(.x, "MM/YYYY"),
                  TRUE ~ NA
                )),
         across(c(any_of(c("circdist", "citizen", "citwhere", "district", "educatn", "hisporig",
                           "moncirc", "monrace", "monsex", "newcit", "neweduc", "newrace", "numdepen",
                           "years", "suprdum", "suprel", "dobmon", "dobyr", "age", "timeserv",
                           "combdrg2", "fiscal_year", "supermax", "supermin", "offguide", "sentrnge",
                           "senspcap", "agecat", "chpts_crim_hist", "chfed_crim_hist", "chage_crim_hist")),
                  starts_with("reas"), starts_with("sent"), starts_with("crim"), starts_with("offtyp"),
                  starts_with("prob"), starts_with("tot"), ends_with("dept"), starts_with("choff")),
                ~.x %>%
                  as.numeric())) %>% 
  sql_render()

dbRemoveTable(con, DBI::Id("ussc_federal_data", "sentencing_data_full"))
dbExecute(con, paste0("CREATE TABLE ", qualify_table("ussc_federal_data", "sentencing_data_full"), " AS ", merge_ussc_data_sql))


####Transform Data####
##Merge Other CSVs to Data
merge_sql_call <- 
  con %>% 
  tbl(in_schema("ussc_federal_data", "sentencing_data_full")) %>% 
  mutate(combdrg2 = 
           combdrg2 %>% 
           as.numeric()) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "OFFTYPSB")) %>% 
              rename("offtypsb_code" = "code", 
                     "offtypsb_description" = "description") %>% 
              filter(offtypsb_code != ".") %>% 
              mutate(offtypsb_code = 
                       offtypsb_code %>%
                       as.numeric()), 
            by = c("offtypsb" = "offtypsb_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "COMBDRG2")) %>% 
              rename("combdrg_code" = "code", 
                     "combdrg_description" = "description") %>% 
              filter(combdrg_code != ".") %>% 
              mutate(combdrg_code = 
                       combdrg_code %>%
                       as.numeric()), 
            by = c("combdrg2" = "combdrg_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "OFFGUIDE")) %>% 
              rename("offguide_code" = "code", 
                     "offguide_description" = "description") %>% 
              filter(offguide_code != ".") %>% 
              mutate(offguide_code = 
                       offguide_code %>%
                       as.numeric()), 
            by = c("offguide" = "offguide_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "districts_with_states")), 
            by = c("district" = "district_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "CITIZEN")) %>% 
              rename_with(~paste0("citizen_", .x)),
            by = c("citizen" = "citizen_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "CITWHERE")) %>% 
              filter(code != ".") %>% 
              filter(code != "*REMOVED*") %>% 
              rename_with(~paste0("citwhere_", .x)) %>% 
              mutate(citwhere_code = 
                       citwhere_code %>%
                       as.numeric()),
            by = c("citwhere" = "citwhere_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "EDUCATN")) %>% 
              filter(code != ".") %>% 
              rename_with(~paste0("educatn_", .x)) %>% 
              mutate(educatn_code = 
                       educatn_code %>%
                       as.numeric()),
            by = c("educatn" = "educatn_code")) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "NEWEDUC")) %>% 
              rename_with(~paste0("neweduc_", .x)) %>% 
              mutate(neweduc_code = 
                       neweduc_code %>%
                       as.numeric()),
            by = c("neweduc" = "neweduc_code")) %>% 
  mutate(hisporig_description = 
           case_when(hisporig == 0 ~ "Information on Hispanic Origin Not Available", 
                     hisporig == 1 ~ "Non-Hispanic", 
                     hisporig == 2 ~ "Hispanic", 
                     TRUE ~ NA), 
         monrace_description = 
           case_when(
             monrace == 1  ~ "White/Caucasian",
             monrace == 2  ~ "Black/African American",
             monrace == 3  ~ "American Indian/Alaskan Native",
             monrace == 4  ~ "Asian or Pacific Islander",
             monrace == 5  ~ "Multi-racial",
             monrace == 7  ~ "Other",
             monrace == 8  ~ "Info on Race Not Available in Docs (FY07+)",
             monrace == 9  ~ "Non-US American Indians",
             monrace == 10 ~ "American Indians Citizenship Unknown",
             TRUE ~ NA
           ),
         newrace_description = 
           case_when(newrace == 1 ~ "White", 
                     newrace == 2 ~ "Black", 
                     newrace == 3 ~ "Hispanic", 
                     newrace == 6 ~ "Other", 
                     TRUE ~ NA),
         monsex_description = 
           case_when(monsex == 0 ~ "Male", 
                     monsex == 1 ~ "Female", 
                     TRUE ~ NA),
         newcit_description = 
           case_when(
             newcit == 0 ~ "U.S. Citizen",
             newcit == 1 ~ "Non-U.S. Citizen",
             TRUE ~ NA
           ),
         numdepen = 
           numdepen %>% 
           na_if(97),
         supervised_release_flag = 
           case_when(suprdum == 1 & sentimp %in% c(1, 2) ~ 1,
                     TRUE ~ 0),
         total_sentenced = 1,
         total_sentenced_fsr = 
           case_when(sentimp %in% c(1, 2) ~ 1, 
                     TRUE ~ 0),
         age_cat_description = 
           case_when(
             years == 1 ~ "< 21",
             years == 2 ~ "21 thru 25",
             years == 3 ~ "26 thru 30",
             years == 4 ~ "31 thru 35",
             years == 5 ~ "36 thru 40",
             years == 6 ~ "41 thru 50",
             years == 7 ~ "> 50",
             TRUE ~ NA
           ),
         sentence_type_description = 
           case_when(
             sentimp == 0 ~ "No Prison/Probation (Fine Only)",
             sentimp == 1 ~ "Prison Only (No Alternatives)",
             sentimp == 2 ~ "Prison + Confinement Conditions (Alternatives, Including Zone C Split Sentences)",
             sentimp == 3 ~ "Probation + Confinement Conditions (Alternatives)",
             sentimp == 4 ~ "Probation Only",
             TRUE ~ NA
           ),
         probation_sentence_flag = 
           case_when((probdum == 1 & sentimp != 1) ~ 1,
                     sentimp == 3 ~ 1,  
                     sentimp == 4 ~ 1, 
                     TRUE ~ 0),
         sentpts_description = 
           case_when(
             sentpts == 0 ~ "None",
             sentpts == 1 ~ "Adjustment Applied (Only Valid Starting in AMENDYR 2023)",
             sentpts == 2 ~ "Adjustment Applied (Only Valid Prior to AMENDYR 2023) ",
             TRUE ~ NA
           ),
         sentrnge_description = 
           case_when(
             sentrnge == 0 ~ "Within Range",
             sentrnge == 1 ~ "Upward Departure",
             sentrnge == 2 ~ "5K1.1/Substantial Assistance",
             sentrnge == 3 ~ "Early Disposition/5K3.1",
             sentrnge == 4 ~ "Government Sponsored Departure",
             sentrnge == 5 ~ "Downward Departure",
             sentrnge == 6 ~ "Above Range Variance",
             sentrnge == 7 ~ "Government Sponsored Variance",
             sentrnge == 8 ~ "Below Range Variance",
             TRUE ~ NA
           )
  ) %>% 
  left_join(con %>% 
              tbl(in_schema("ussc_federal_data", "POOFFICE")) %>% 
              mutate(state_district = 
                       state_district %>% 
                       na_if(""),
                     state_district = 
                       ifelse(is.na(state_district) == TRUE, state, paste0(state, " ", state_district))) %>%
              rename("po_office_name" = "office_name", 
                     "pooffice" = "office_code", 
                     "district" = "state_district_number", 
                     "state_name" = "state"),
            by = c("district", "pooffice")) %>% 
  mutate(fsa_qualifying_offense = sql("
                                        CASE WHEN EXISTS (
                                          SELECT 1 FROM ussc_federal_data.fsa_qualifying_offenses fsa 
                                          WHERE nwstat1 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat2 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat3 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat4 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat5 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat6 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat7 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat8 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat9 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat10 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat11 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat12 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat13 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat14 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat15 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat16 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat17 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat18 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat19 LIKE fsa.title_subtitle_text || '%'
                                             OR nwstat20 LIKE fsa.title_subtitle_text || '%'
                                        ) THEN 1 ELSE 0 END
                                      "),
         fsa_matching_title = sql("
                                    (SELECT fsa.title_subtitle_text 
                                     FROM ussc_federal_data.fsa_qualifying_offenses fsa 
                                     WHERE nwstat1 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat2 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat3 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat4 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat5 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat6 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat7 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat8 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat9 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat10 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat11 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat12 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat13 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat14 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat15 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat16 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat17 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat18 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat19 LIKE fsa.title_subtitle_text || '%'
                                        OR nwstat20 LIKE fsa.title_subtitle_text || '%'
                                     LIMIT 1)
                                  ")) %>%
  left_join(con %>% 
              tbl(DBI::Id("ussc_federal_data", "fsa_qualifying_offenses")),
            by = c("fsa_matching_title" = "title_subtitle_text")) %>% 
  sql_render()

dbRemoveTable(con, DBI::Id("ussc_federal_data", "sentencing_data_full_parsed"))
dbExecute(con, paste0("CREATE TABLE ", qualify_table("ussc_federal_data", "sentencing_data_full_parsed"), " AS ", merge_sql_call))

##Aggregate Data for Dashboard
dashboard_df_sql <- 
  con %>% 
  tbl(in_schema("ussc_federal_data", "sentencing_data_full_parsed")) %>%
  group_by(usscidn, total_eventnum_crim_hist) %>% 
  slice_sample(n = 1) %>% 
  ungroup() %>% 
  mutate(offense_category = 
           case_when(offguide %in% c(9, 10) ~ "Drug Trafficking",
                     offguide == 17 ~ "Immigration",
                     offguide == 13 ~ "Firearms",
                     offguide == 16 ~ "Fraud",
                     offguide == 7 ~ "Sexual Offense",   
                     offguide == 27 ~ "Sexual Offense", 
                     offguide %in% c(4, 19, 20, 22, 26, 28) ~ "Violent Offense",
                     TRUE ~ "Other")) %>% 
  dplyr::select(usscidn, age, state_name, state_district, po_office_name, fiscal_year, sentyr, offense_category,
                starts_with("dob"), ends_with("description"), ends_with("flag"), starts_with("total_sentenced")) %>% 
  distinct() %>% 
  dplyr::select(age, state_name, state_district, po_office_name, fiscal_year,
                offguide_description, neweduc_description, newrace_description, citizen_description, newcit_description,
                monsex_description, sentence_type_description, ends_with("flag"), starts_with("total_sentenced"), offense_category) %>% 
  group_by(across(-c(ends_with("flag"), starts_with("total_sentenced")))) %>% 
  dplyr::summarize(across(c(ends_with("flag"), starts_with("total_sentenced")), 
                          ~.x %>% 
                            sum(na.rm = TRUE))) %>% 
  ungroup() %>% 
  sql_render()

dbRemoveTable(con, DBI::Id("ussc_federal_data", "sentencing_data_aggregated_dashboard"))
dbExecute(con, paste0("CREATE TABLE ", qualify_table("ussc_federal_data", "sentencing_data_aggregated_dashboard"), " AS ", dashboard_df_sql))




  
  
  
  
  
  
  