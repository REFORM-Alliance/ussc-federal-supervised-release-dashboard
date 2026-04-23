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
library(readr)
library(sf)


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


####Read in Data####
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
           as.numeric()) %>%
  rename("po_office" = "po_office_name")

fwrite(supervision_df, here("data-raw", "aggregated_ussc_sentencing_data.csv"), row.names = FALSE)

##Load US Cities Data
us.cities <- maps::us.cities

##PO Office
po_office_df <-
  supervision_df %>% 
  dplyr::select(po_office, state_name) %>% 
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
  mutate(po_office_full = 
           po_office %>% 
           paste(state_abb, sep = " ")) %>% 
  left_join(us.cities, by = c("po_office_full" = "name")) %>%
  (\(df) 
   bind_rows(
     df,
     df %>%
       filter(is.na(lat) == TRUE & is.na(long) == TRUE) %>% 
       dplyr::select(-c(lat, long)) %>% 
       geocode(po_office_full, 
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
  dplyr::select(po_office, state_name, state_abb, lat, long)

fwrite(po_office_df, here("data-raw", "po_office_locations.csv"), sep = ",", row.names = FALSE)

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

judicial_sf %>% 
  write_rds(file = here("data", "judicial_sf_data.rds"))







