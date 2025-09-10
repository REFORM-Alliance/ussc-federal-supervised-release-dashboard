#rm(list = ls())

####Read in Libraries####
library(tidyverse)
library(data.table)
library(janitor)
library(here)
library(DBI)
library(dbplyr)
library(RPostgres)


####Safe Insert Table Function####
##Sub-Functions
schema_exists <- function(con, schema_name) {
  sql <- glue::glue_sql(
    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = {schema}",
    schema = schema_name,
    .con = con
  )
  res <- dbGetQuery(con, sql)
  nrow(res) > 0
}

qualify_table <- function(schema, tbl) {
  DBI::SQL(paste0(DBI::dbQuoteIdentifier(con, schema), ".", DBI::dbQuoteIdentifier(con, tbl)))
}

##Full Function
safe_insert_table <- function(con, df, table_name, schema){
  if(schema_exists(con, schema) == FALSE){
    dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ", schema))
  }
  
  if(dbExistsTable(con, DBI::Id(schema = schema, table = table_name)) == FALSE){
    dbWriteTable(con, DBI::Id(schema = schema, table = table_name), df, overwrite = TRUE)
  }else{
    temp_table <- paste0("temp_", table_name)
    new_table <- paste0("new_", table_name)
    table_id <- DBI::Id(schema = schema, table = table_name)
    temp_table_id <- DBI::Id(schema = schema, table = temp_table)
    new_table_id  <- DBI::Id(schema = schema, table = new_table)
    dbWriteTable(con, temp_table_id, df, temporary = FALSE, overwrite = TRUE)
    
    append_unique_sql <- 
      con %>% 
      tbl(table_id) %>% 
      union_all(con %>% 
                  tbl(temp_table_id)) %>% 
      distinct() %>% 
      sql_render()
    
    dbExecute(con, paste0("CREATE TABLE ", qualify_table(schema, new_table), " AS ", append_unique_sql))
    dbExecute(con, paste0("DROP TABLE IF EXISTS ", qualify_table(schema, temp_table), ";"))
    dbExecute(con, paste0("DROP TABLE IF EXISTS ", qualify_table(schema, table_name), ";"))
    dbExecute(con, paste0("ALTER TABLE ", qualify_table(schema, new_table), " RENAME TO ", DBI::dbQuoteIdentifier(con, table_name)))
  }
}
