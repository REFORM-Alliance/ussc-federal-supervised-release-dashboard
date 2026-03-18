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
library(scales)


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
##Aggregate Data by State/Year/Qualifying Offense
qualifying_offense_agg_df <- 
  con %>% 
  tbl(DBI::Id("ussc_federal_data", "sentencing_data_full_parsed")) %>% 
  group_by(fiscal_year, usscidn) %>% 
  dplyr::summarize(fsa_qualifying_offense = 
                     fsa_qualifying_offense %>% 
                     max(na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(fiscal_year, fsa_qualifying_offense) %>% 
  dplyr::summarize(count = n()) %>% 
  ungroup() %>% 
  mutate(fsa_qualifying_offense = 
           case_when(fsa_qualifying_offense == 0 ~ "not_fsa_qualifying_offense", 
                     fsa_qualifying_offense == 1 ~ "fsa_qualifying_offense", 
                     TRUE ~ NA)) %>% 
  pivot_wider(names_from = fsa_qualifying_offense, 
              values_from = count) %>% 
  collect() %>% 
  mutate(across(contains("fsa"), 
                ~.x %>% 
                  as.numeric()),
         percentage_fsa_qualifying_offenses = 
           fsa_qualifying_offense/(fsa_qualifying_offense + not_fsa_qualifying_offense),
         percentage_fsa_qualifying_offenses = 
           percentage_fsa_qualifying_offenses %>% 
           as.numeric() %>% 
           percent(),
         across(c(starts_with("fsa"), starts_with("not_fsa")),
                ~.x %>% 
                  as.numeric() %>% 
                  comma()))

##Aggregate Data by Statute
qualifying_offense_by_statute_df <- 
  con %>% 
  tbl(DBI::Id("ussc_federal_data", "sentencing_data_full_parsed")) %>% 
  filter(fiscal_year >= 2022) %>% 
  group_by(title, title_name, chapter, section, violation_summary) %>% 
  dplyr::summarize(sentence_count = 
                     fsa_qualifying_offense %>% 
                     sum(na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(desc(sentence_count)) %>% 
  collect() %>% 
  na.omit() %>% 
  mutate(sentence_count = 
           sentence_count %>% 
           as.numeric() %>% 
           comma())

fwrite(qualifying_offense_by_statute_df, file = here("data", "qualifying_offense_counts_by_statute.csv"), row.names = FALSE)




