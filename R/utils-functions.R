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

##Add Missing Columns
add_missing_columns <- function(con, df, schema, table_name) {
  db_cols <- dbGetQuery(
    con,
    glue::glue_sql(
      "
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = {schema}
        AND table_name   = {table}
      ORDER BY ordinal_position
      ",
      schema = schema,
      table  = table_name,
      .con   = con
    )
  )$column_name
  
  df_cols <- names(df)
  missing_cols <- setdiff(df_cols, db_cols)
  if(length(missing_cols) == 0) return(invisible())
  for(col in missing_cols){
    sql_type <- dbDataType(con, df[[col]])
    
    con %>% 
    dbExecute(paste0("ALTER TABLE ",
                     qualify_table(schema, table_name),
                     " ADD COLUMN ",
                     DBI::dbQuoteIdentifier(con, col),
                     " ",
                     sql_type))
  }
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

##Full Function - Safe Insert Plus
safe_insert_table_new <- function(con, df, table_name, schema) {
  if(!schema_exists(con, schema)){
    dbExecute(
      con,
      paste0("CREATE SCHEMA IF NOT EXISTS ",
             DBI::dbQuoteIdentifier(con, schema))
    )
  }
  
  table_id <- DBI::Id(schema = schema, table = table_name)
  constraint_name <- paste0(table_name, "_usscidn_key")
  
  if(!dbExistsTable(con, table_id)){
    # New table: create with constraint
    dbWriteTable(con, table_id, df, overwrite = TRUE)
    dbExecute(
      con,
      paste0(
        "ALTER TABLE ", qualify_table(schema, table_name),
        " ADD CONSTRAINT ",
        DBI::dbQuoteIdentifier(con, constraint_name),
        " UNIQUE (usscidn)"
      )
    )
    return(invisible())
  }
  
  # Check if usscidn column exists in the existing table
  db_cols <- dbGetQuery(
    con,
    glue::glue_sql(
      "
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = {schema}
        AND table_name   = {table}
      ",
      schema = schema,
      table  = table_name,
      .con   = con
    )
  )$column_name
  
  # If usscidn doesn't exist in the table, add it first
  if(!"usscidn" %in% db_cols) {
    sql_type <- dbDataType(con, df[["usscidn"]])
    dbExecute(
      con,
      paste0(
        "ALTER TABLE ", qualify_table(schema, table_name),
        " ADD COLUMN ",
        DBI::dbQuoteIdentifier(con, "usscidn"),
        " ", sql_type
      )
    )
  }
  
  # Add any other missing columns
  add_missing_columns(con, df, schema, table_name)
  
  # Check if constraint exists
  constraint_exists <- dbGetQuery(
    con,
    glue::glue_sql(
      "
      SELECT constraint_name
      FROM information_schema.table_constraints
      WHERE table_schema = {schema}
        AND table_name = {table}
        AND constraint_name = {constraint}
      ",
      schema = schema,
      table = table_name,
      constraint = constraint_name,
      .con = con
    )
  )
  
  if(nrow(constraint_exists) == 0){
    # Check for duplicates before adding constraint
    dup_check <- dbGetQuery(
      con,
      paste0(
        "SELECT usscidn, COUNT(*) as cnt ",
        "FROM ", qualify_table(schema, table_name),
        " WHERE usscidn IS NOT NULL ",
        "GROUP BY usscidn ",
        "HAVING COUNT(*) > 1"
      )
    )
    
    if(nrow(dup_check) > 0) {
      # warning(paste0(
      #   "Table '", table_name, "' has duplicate usscidn values. ",
      #   "Cannot add unique constraint. Cleaning duplicates first..."
      # ))
      
      # Remove duplicates, keeping the first occurrence
      dbExecute(
        con,
        paste0(
          "DELETE FROM ", qualify_table(schema, table_name), " a ",
          "USING ", qualify_table(schema, table_name), " b ",
          "WHERE a.ctid < b.ctid AND a.usscidn = b.usscidn"
        )
      )
    }
    
    # Now add the constraint
    tryCatch({
      dbExecute(
        con,
        paste0(
          "ALTER TABLE ", qualify_table(schema, table_name),
          " ADD CONSTRAINT ",
          DBI::dbQuoteIdentifier(con, constraint_name),
          " UNIQUE (usscidn)"
        )
      )
    }, error = function(e) {
      stop(paste0(
        "Failed to add unique constraint on usscidn. ",
        "Error: ", e$message
      ))
    })
  }
  
  # Create temp table and insert
  temp_table <- paste0("temp_", table_name)
  temp_table_id <- DBI::Id(schema = schema, table = temp_table)
  dbWriteTable(con, temp_table_id, df, overwrite = TRUE)
  
  cols <- paste(
    DBI::dbQuoteIdentifier(con, names(df)),
    collapse = ", "
  )
  
  # Insert with conflict handling
  dbExecute(
    con,
    paste0(
      "INSERT INTO ", qualify_table(schema, table_name),
      " (", cols, ") ",
      "SELECT ", cols,
      " FROM ", qualify_table(schema, temp_table),
      " ON CONFLICT (usscidn) DO NOTHING"
    )
  )
  
  dbExecute(
    con,
    paste0(
      "DROP TABLE IF EXISTS ",
      qualify_table(schema, temp_table)
    )
  )
  
  invisible()
}

db_remove_empty_cols <- function(remote_tbl) {
  # 1. Ask the DB for a count of non-NA values per column
  presence_counts <- remote_tbl %>%
    summarise(across(everything(), ~ sum(as.integer(!is.na(.x)), na.rm = TRUE))) %>%
    collect()
  
  # 2. Keep only columns where count > 0
  non_empty_names <- names(presence_counts)[presence_counts[1, ] > 0]
  
  # 3. Return the lazy query with only those columns selected
  remote_tbl %>% select(all_of(non_empty_names))
}
