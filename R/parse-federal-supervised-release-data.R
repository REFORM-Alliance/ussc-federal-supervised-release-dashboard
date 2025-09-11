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


####Read in Data####
##Parse Data 
years <- c(2014:2024)
years %>% 
  map(~{
    ##Parse Individual Data File
    year = .x
    dir.create("data-raw" %>% 
                 here("federal-individual-sentencing-data") %>% 
                 here(year))
    
    short_year <- 
      year %>% 
      str_sub(3, 4)
    
    if(year >= 2024){
      "data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset(short_year) %>% 
        str_subset("_csv[.]zip$") %>% 
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
                        as.character()))
    }else{
      "data-raw" %>% 
        here("federal-individual-sentencing-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset(short_year) %>% 
        str_subset("zip$") %>% 
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
        clean_names() 
    }
    
    "data-raw" %>% 
      here("federal-individual-sentencing-data") %>% 
      here(year) %>% 
      unlink(recursive = TRUE)
    
    ##Parse Supplemental Data File
    supplement_data_file <- 
      "data-raw" %>% 
      here("federal-individual-sentencing-supplemental-data") %>% 
      list.files(full.names = TRUE) %>% 
      str_subset(short_year)
    
    if(length(supplement_data_file) > 0){
      
      dir.create("data-raw" %>% 
                   here("federal-individual-sentencing-supplemental-data") %>% 
                   here(year))
      
      "data-raw" %>% 
        here("federal-individual-sentencing-supplemental-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset(short_year) %>% 
        str_subset("zip$") %>% 
        unzip(exdir = 
                "data-raw" %>% 
                here("federal-individual-sentencing-supplemental-data") %>% 
                here(year))
      
      df_raw_supp <- 
        read.SAScii(
          "data-raw" %>% 
            here("federal-individual-sentencing-supplemental-data") %>% 
            here(year) %>% 
            list.files(full.names = TRUE) %>% 
            str_subset("[.]dat$"),
          "data-raw" %>% 
            here("federal-individual-sentencing-supplemental-data") %>% 
            here(year) %>% 
            list.files(full.names = TRUE) %>% 
            str_subset("[.]sas$")
          )
      
      "data-raw" %>% 
        here("federal-individual-sentencing-supplemental-data") %>% 
        here(year) %>% 
        unlink(recursive = TRUE)
        
      df_plus_supp <- 
        df_raw %>% 
        left_join(df_raw_supp %>% 
                    clean_names() %>% 
                    mutate(usscidn = 
                             usscidn %>% 
                             as.character()), 
                  by = "usscidn")
      
      rm(df_raw)
      rm(df_raw_supp)
    }else{
      df_plus_supp <- df_raw
      rm(df_raw)
    }
    
    ##Parse Criminal History Data File
    criminal_history_data_file <- 
      "data-raw" %>% 
      here("federal-criminal-history-data") %>% 
      list.files(full.names = TRUE) %>% 
      str_subset(short_year)
    
    if(length(criminal_history_data_file) == 1){
      dir.create("data-raw" %>% 
                   here("federal-criminal-history-data") %>% 
                   here(year))
      
      "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset(short_year) %>% 
        str_subset("zip$") %>% 
        unzip(exdir = 
                "data-raw" %>% 
                here("federal-criminal-history-data") %>% 
                here(year))
      
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
        clean_names()
      
      "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        unlink(recursive = TRUE)
      
      df_plus_crim <- 
        df_plus_supp %>% 
        left_join(df_raw_crim %>% 
                    clean_names() %>% 
                    mutate(usscidn = 
                             usscidn %>% 
                             as.character()) %>% 
                    rename_with(~.x %>% 
                                  paste0("_crim_hist") %>% 
                                  str_replace_all("usscidn_crim_hist", "usscidn")), 
                  by = "usscidn")
      
      rm(df_plus_supp)
      rm(df_raw_crim)
    }else if(length(criminal_history_data_file) > 1){
      dir.create("data-raw" %>% 
                   here("federal-criminal-history-data") %>% 
                   here(year))
      
      "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        list.files(full.names = TRUE) %>% 
        str_subset(short_year) %>% 
        str_subset("_csv[.]zip$") %>% 
        unzip(exdir = 
                "data-raw" %>% 
                here("federal-criminal-history-data") %>% 
                here(year))
      
      df_raw_crim <- 
        "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        list.files(full.names = TRUE) %>% 
        str_subset("[.csv]$") %>% 
        fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
        clean_names() 
      
      "data-raw" %>% 
        here("federal-criminal-history-data") %>% 
        here(year) %>% 
        unlink(recursive = TRUE)
      
      df_plus_crim <- 
        df_plus_supp %>% 
        left_join(df_raw_crim %>% 
                    mutate(usscidn = 
                             usscidn %>% 
                             as.character()) %>% 
                    rename_with(~.x %>% 
                                  paste0("_crim_hist") %>% 
                                  str_replace_all("usscidn_crim_hist", "usscidn")), 
                  by = "usscidn") 
      
      rm(df_plus_supp)
      rm(df_raw_crim)
    }else{
      df_plus_crim <- df_plus_supp
      rm(df_plus_supp)
    }
    
    ##Finalize Data and Write to DB
    if(length(criminal_history_data_file) > 0){
      df_final <- 
        df_plus_crim %>% 
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
                      ends_with("_crim_hist")) %>% 
        mutate(across(where(is_character), 
                      ~.x %>% 
                        na_if("") %>% 
                        iconv(from = "", to = "UTF-8", sub = "byte")),
               across(any_of(c("offguide", "senspcap", "sentrnge", "senttcap")), 
                      ~.x %>% 
                        as.double()),
               across(c(any_of(c("circdist", "citizen", "eventnum_crim_hist")), 
                        matches("^chpts\\d{1,3}_crim_hist$"),
                        matches("^choff\\d_\\d{1,3}_crim_hist$"),
                        matches("^chfed\\d{1,3}_crim_hist$"),
                        matches("^chtrib\\d{1,3}_crim_hist$"),
                        matches("^chsndt\\d{1,3}_crim_hist$"),
                        matches("^chage\\d{1,3}_crim_hist$"),
                        matches("^chardt\\d{1,3}_crim_hist$")),
                      ~.x %>% 
                        as.character()),
               fiscal_year = 
                 year %>% 
                 as.numeric()) %>% 
        select(where(~ !all(is.na(.x)))) %>% 
        pivot_longer(cols = c(ends_with("_crim_hist"), -eventnum_crim_hist), 
                     names_to = "crim_hist_var", 
                     values_to = "crim_hist_val") %>% 
        filter(is.na(crim_hist_val) == FALSE) %>% 
        mutate(crim_hist_event_num = 
                 crim_hist_var %>% 
                 str_replace_all("_crim_hist", "") %>% 
                 str_extract_all("\\d+$") %>% 
                 str_replace_all("_$", "") %>% 
                 as.numeric(),
               crim_hist_var = 
                 crim_hist_var %>% 
                 str_replace_all("_crim_hist", "") %>% 
                 str_replace_all("\\d+$", "") %>% 
                 str_replace_all("_$", "") %>% 
                 paste0("_crim_hist")) %>% 
        pivot_wider(names_from = "crim_hist_var",
                    values_from = "crim_hist_val") %>% 
        mutate(across(c(ends_with("dt"), ends_with("df_crim_hist")), 
                      ~.x %>% 
                        as.Date(format = "%d-%b-%Y")), 
               across(c(offguide:numdepen, years:crim_hist_event_num), 
                      ~.x %>% 
                        as.numeric()), 
               across(c("chpts_crim_hist", "chfed_crim_hist", "chage_crim_hist", starts_with("choff")), 
                      ~.x %>% 
                        as.numeric())) %>% 
        rename("total_crim_hist_event_num" = "eventnum_crim_hist")
      
      rm(df_plus_crim)
    }else{
      df_final <- 
        df_plus_crim %>% 
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
                      ends_with("_crim_hist")) %>% 
        mutate(across(where(is_character), 
                      ~.x %>% 
                        na_if("") %>% 
                        iconv(from = "", to = "UTF-8", sub = "byte")),
               across(any_of(c("offguide", "senspcap", "sentrnge", "senttcap")), 
                      ~.x %>% 
                        as.double()),
               across(c(any_of(c("circdist", "citizen", "eventnum_crim_hist")), 
                        matches("^chpts\\d{1,3}_crim_hist$"),
                        matches("^choff\\d_\\d{1,3}_crim_hist$"),
                        matches("^chfed\\d{1,3}_crim_hist$"),
                        matches("^chtrib\\d{1,3}_crim_hist$"),
                        matches("^chsndt\\d{1,3}_crim_hist$"),
                        matches("^chage\\d{1,3}_crim_hist$"),
                        matches("^chardt\\d{1,3}_crim_hist$")),
                      ~.x %>% 
                        as.character()),
               across(c(ends_with("dt"), ends_with("df_crim_hist")), 
                      ~.x %>% 
                        as.Date(format = "%d-%b-%Y")), 
               across(c(offguide:numdepen, years:timeserv), 
                      ~.x %>% 
                        as.numeric()), 
               across(c(any_of(c("chpts_crim_hist", "chfed_crim_hist", "chage_crim_hist")), starts_with("choff")), 
                      ~.x %>% 
                        as.numeric()),
               fiscal_year = 
                 year %>% 
                 as.numeric()) %>% 
        select(where(~ !all(is.na(.x)))) 
      
      rm(df_plus_crim)
    }
    
    ##Write Table into DB
    safe_insert_table(con = con, df = df_final, table_name = "sentencing_data_full", schema = "ussc_federal_data")
    print(glue::glue("Data Read, Parsed, and Written in Database for USSC FY{short_year}"))
    rm(df_final)
  })


####Transform Data####
##Write CSV Files into Database
"data-raw" %>% 
  here("coding-csvs") %>% 
  list.files(full.names = TRUE) %>% 
  map(~{
    file_name = .x
    
    folder_name =
      "data-raw" %>% 
      here("coding-csvs")
    
    file_name_short =
      file_name %>% 
      str_replace_all(folder_name, "") %>% 
      str_replace_all("^/", "") %>% 
      str_replace_all("[.]csv$", "")
    
    df_csv <- 
      file_name %>% 
      fread(sep = ",", header = TRUE, stringsAsFactors = FALSE) %>% 
      clean_names()
    
    safe_insert_table(con = con, df = df_csv, table_name = file_name_short, schema = "ussc_federal_data")  
  })

##Merge Other CSVs to Data
merge_sql_call <- 
  con %>% 
  tbl(in_schema("ussc_federal_data", "sentencing_data_full")) %>% 
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
             newcit == 1 ~ "U.S. Citizen",
             newcit == 2 ~ "Resident/Legal Alien",
             newcit == 3 ~ "Illegal Alien",
             newcit == 4 ~ "Not a U.S. Citizen/Alien Status Unknown",
             newcit == 5 ~ "Extradited Alien",
             TRUE ~ NA
           ),
         numdepen = 
           numdepen %>% 
           na_if(97),
         supervised_release_flag = 
           ifelse(suprdum == 1, 1, 0),
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
         # probation_sentence_flag = 
         #   ifelse(senttot0 == 0, 1, 0),
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
           case_when(senttot0 == 0 ~ 1,
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
           ),
         across(ends_with("dt_crim_hist"),
                ~sql(paste0("to_date(", cur_column(), ", 'DD-Mon-YYYY')")),
                .names = "{.col}_clean")) %>% 
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
  sql_render()

dbExecute(con, paste0("CREATE TABLE ", qualify_table("ussc_federal_data", "sentencing_data_full_parsed"), " AS ", merge_sql_call))

##Aggregate Data for Dashboard
dashboard_df_sql <- 
  con %>% 
  tbl(in_schema("ussc_federal_data", "sentencing_data_full_parsed")) %>%
  group_by(usscidn, crim_hist_event_num) %>% 
  slice_sample(n = 1) %>% 
  ungroup() %>% 
  dplyr::select(usscidn, age, state_name, state_district, po_office_name, fiscal_year, sentyr,
                starts_with("dob"), ends_with("description"), ends_with("flag")) %>% 
  distinct() %>% 
  dplyr::select(age, state_name, state_district, po_office_name, fiscal_year,
                offguide_description, neweduc_description, newrace_description, 
                monsex_description, sentence_type_description, ends_with("flag")) %>% 
  group_by(across(-ends_with("flag"))) %>% 
  dplyr::summarize(across(ends_with("flag"), 
                          ~.x %>% 
                            sum(na.rm = TRUE)),
                   total_count = n()) %>% 
  ungroup() %>% 
  sql_render()

dbExecute(con, paste0("CREATE TABLE ", qualify_table("ussc_federal_data", "sentencing_data_aggregated_dashboard"), " AS ", dashboard_df_sql))


####Geocode PO Offices####
##Load US Cities Data
us.cities <- maps::us.cities

##PO Office
po_office_df <-
  supervision_df %>% 
  dplyr::select(po_office_name, state_name) %>% 
  distinct() %>% 
  left_join(data.frame(state_name = state.name, 
                       state_abb = state.abb) %>% 
              bind_rows(data.frame(state_name = c("District of Columbia", 
                                                  "Puerto Rico", 
                                                  "Guam", 
                                                  "Virgin Islands", 
                                                  "North Mariana Islands"), 
                                   state_abb = c("DC", "PR", "GU", "VI", "MP"))), 
            by = "state_name") %>% 
  mutate(po_office_name_full = 
           po_office_name %>% 
           paste(state_abb, sep = " ")) %>% 
  left_join(us.cities, by = c("po_office_name_full" = "name")) %>%
  (\(df) 
   bind_rows(
     df,
     df %>%
       filter(is.na(lat) == TRUE & is.na(long) == TRUE) %>% 
       dplyr::select(-c(lat, long)) %>% 
       geocode(po_office_name_full, 
               method = "osm", 
               lat = "latitude", 
               long = "longitude")
   )) %>%
  filter(!(is.na(lat) == TRUE & is.na(long) == TRUE & 
           is.na(latitude) == TRUE & is.na(longitude) == TRUE)) %>% 
  mutate(lat = 
           ifelse(is.na(lat) == TRUE, latitude, lat),
         long = 
           ifelse(is.na(long) == TRUE, longitude, long)) %>% 
  dplyr::select(-c(latitude, longitude)) %>% 
  dplyr::select(po_office_name, state_name, state_abb, lat, long)
  
fwrite(po_office_df, here("data-raw", "po_office_locations.csv"), sep = ",", row.names = FALSE)

  















