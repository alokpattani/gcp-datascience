UploadTibbleToBigQueryTable <- function(tibble_to_upload, bq_table_name,
  bq_project, bq_dataset, bq_auth_file_path,
  upload_type = 'APPEND', test_upload = FALSE, add_upload_timestamp = TRUE,
  row_keys_for_duplicates_and_delete = NULL
  )
{
  #### Versatile function to upload tibble in R to BigQuery table ####

  # If no rows to return, print message and return early
  if(nrow(tibble_to_upload) == 0) {
    print("No Rows in Tibble to Upload")
    return()
  }

  # Re-authenticate BigQuery within function (can help when running in parallel)
  bq_auth(path = bq_auth_file_path, cache = TRUE)  
  
  # Turn to uppercase in case it didn't come in that way
  upload_type <- toupper(upload_type)  
    
  # If specified, add upload timestamp for tracking
  if(add_upload_timestamp) {
    tibble_for_bq_upload <- tibble_to_upload %>% 
      mutate(
        uploadTimestampUTC = now(tzone = "UTC")
        )
  } else {
    tibble_for_bq_upload <- tibble_to_upload
  }
  
  # IF testing upload, filter to 1st 5 rows only
  if(test_upload)
  {
    tibble_for_bq_upload <- tibble_for_bq_upload %>% slice(1:5)
  }

  output_bq_table <- bq_table(bq_project, bq_dataset, bq_table_name)

  output_bq_table_full_name <- paste0("`", bq_project, ".", bq_dataset, ".", 
    bq_table_name, "`")
  
  if(upload_type == "CREATE") {
    output_bq_table_new <- TRUE
    tibble_for_bq_upload <- tibble_for_bq_upload
    output_bq_fields_object <- as_bq_fields(tibble_for_bq_upload)
  } else {
    output_bq_table_new <- FALSE
    # Use pointer to BQ table to get table fields w/ appropriate types for upload  
    output_bq_fields_object <- output_bq_table %>% bq_table_fields()
    
    output_bq_fields_df <- bq_table_meta(output_bq_table)$schema$fields %>%
      map_df(~as.data.frame(t(.)))
    
    # Select fields from tibble that are in BQ output table, in order
    tibble_for_bq_upload <- tibble_for_bq_upload %>%
      select(any_of(unlist(output_bq_fields_df$name, use.names = FALSE)))
  }
  
  # How write is to be done for this table to BigQuery
  if(upload_type == 'APPEND') {
    table_upload_write_disposition <- 'WRITE_APPEND'
  } else {
    table_upload_write_disposition <- 'WRITE_TRUNCATE'
  }
  
  # Only need to execute delete query logic if appending & row keys provided
  if(upload_type == 'APPEND' & 
    !is_empty(unlist(row_keys_for_duplicates_and_delete)))
  {
    tibble_df_key_fields_unique_combos <- tibble_for_bq_upload %>%
      select(all_of(row_keys_for_duplicates_and_delete)) %>%
      unique()
    
    # Create & upload table containing keys to delete for original table
    bq_table_for_key_fields_to_delete <- bq_table(bq_project, bq_dataset, 
      paste0(bq_table_name, '_keys_to_delete'))
    
    bq_table_upload(
      x = bq_table_for_key_fields_to_delete,
      values = tibble_df_key_fields_unique_combos,
      write_disposition = 'WRITE_TRUNCATE'
      )
    
    # Create SQL text using MERGE statement in BQ, join to keys to delete table
    delete_query_sql_text <- paste0("MERGE", output_bq_table_full_name,
      " OutputTable \nUSING `", bq_project, ".", bq_dataset, '.',
      bq_table_name, '_keys_to_delete` KeysToDelete ON\n',
      row_keys_for_duplicates_and_delete %>% 
        paste0("\tOutputTable.", ., " = KeysToDelete.", .) %>% 
        paste0(collapse = " AND\n"), 
      "\nWHEN MATCHED THEN DELETE;\n"
      )

    bq_delete_query_result <- bq_project_query(bq_project, 
      delete_query_sql_text)

    # Remove table containing keys to delete, now that it has served its purpose
    bq_table_delete(bq_table_for_key_fields_to_delete)
    
    cat(paste0("\nDeleted rows from ", output_bq_table_full_name, 
      " using query\n", delete_query_sql_text, "\n"))
  }
  
  # Upload desired table to BigQuery
  bq_table_upload(
    x = output_bq_table,
    values = tibble_for_bq_upload, 
    fields = output_bq_fields_object,
    write_disposition = table_upload_write_disposition
    )
  
  cat(paste0("\nOutputted (", table_upload_write_disposition, ") ",
    nrow(tibble_for_bq_upload), " rows to ", 
    ifelse(output_bq_table_new, "new", "existing"), " BigQuery table ",
    output_bq_table_full_name, "\n")
    )
}
