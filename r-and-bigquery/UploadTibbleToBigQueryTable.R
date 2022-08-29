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
    # Use pointer to BQ table to get fields w/ appropriate types for upload  
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
  

  # Next section checks if tibble to upload is "too big", if so, chunk into
  # groups and upload separately to avoid getting error related to data size:
  # "Error in rawToChar(rawConnectionValue(con)) : 
  #   long vectors not supported yet: raw.c:68 "
  
  # Based on testing, use limits of 2M rows and 500MB object size as thresholds
  # for which to divide up tibble into smaller pieces before uploading
  TIBBLE_ROW_NUM_THRESHOLD <- 2E6
  TIBBLE_OBJECT_SIZE_THRESHOLD_MB <- 500
  
  # Calculate # groups to divide tibble into based on row # vs threshold
  tibble_row_num_groups <- (nrow(tibble_for_bq_upload) / 
    TIBBLE_ROW_NUM_THRESHOLD) %>%
    ceiling()

  # Calculate # groups to divide tibble into based on object size vs threshold
  tibble_object_size_num_groups <- ((object.size(tibble_for_bq_upload) %>% 
    format(units = "MB") %>% 
    str_replace(" Mb", "") %>% 
    as.numeric()
    ) / TIBBLE_OBJECT_SIZE_THRESHOLD_MB) %>%
    ceiling()
  
  # Take max of num groups based on row # and object size to be conservative
  tibble_num_groups <- max(tibble_row_num_groups, tibble_object_size_num_groups)
  
  # Build function within this function (using some fields created above) to 
  # abstract over BigQuery upload and printing summary to console
  nest_fn_upload_to_bq_and_print_summary <- function(fn_tibble_for_upload,
    fn_table_upload_write_disposition, fn_group_num = 1, fn_max_group_num = 1)
  {
    bq_table_upload(
      x = output_bq_table, # "Constant" w/in this function, from outer code
      values = fn_tibble_for_upload,
      fields = output_bq_fields_object, # "Constant" w/in this function
      write_disposition = fn_table_upload_write_disposition
      )
    
    # Print summary of upload job to R console
    bq_upload_summary_text <- paste0("\nUploaded (", 
      fn_table_upload_write_disposition, ") group ", fn_group_num, " of ",
      fn_max_group_num, " of data (", nrow(fn_tibble_for_upload), " rows) to ", 
      ifelse(output_bq_table_new, "new", "existing"), " BigQuery table ",
      output_bq_table_full_name, "\n")
      
    cat(bq_upload_summary_text)
  }
  
  if(tibble_num_groups == 1) { 
    # No need to divide up table in this scenario, can just upload directly
    nest_fn_upload_to_bq_and_print_summary(
     fn_tibble_for_upload = tibble_for_bq_upload, 
     fn_table_upload_write_disposition = table_upload_write_disposition
     )
  } else {
    cat(paste0("\nData to upload is potentially large, will be divided into ",
      tibble_num_groups, " groups for uploading to BigQuery.\n")
      )
    
    # Divide tibble into # groups determined above, in row # order, then nest
    # (split) tibbles for upload in list-column
    tibble_for_bq_upload_by_group_nest <- tibble_for_bq_upload %>%
      mutate(
        upload_group = (row_number() / nrow(tibble_for_bq_upload) * 
          tibble_num_groups) %>% ceiling()
        ) %>%
      nest_by(upload_group, .key = "slice_tibble_for_bq_upload") %>%
      ungroup()
    
    if(table_upload_write_disposition == 'WRITE_TRUNCATE')
    {
      # If original table upload write disposition is WRITE_TRUNCATE...
      # Filter down to 1st upload group and unnest data to be uploaded
      first_slice_tibble_for_bq_upload <- tibble_for_bq_upload_by_group_nest %>% 
        filter(upload_group == 1) %>%
        select(slice_tibble_for_bq_upload) %>%
        unnest(slice_tibble_for_bq_upload)

      # Upload 1st group by itself to make new table (& replace any old data)..
      nest_fn_upload_to_bq_and_print_summary(
        fn_tibble_for_upload = first_slice_tibble_for_bq_upload, 
        fn_table_upload_write_disposition = 'WRITE_TRUNCATE',
        fn_max_group_num = tibble_num_groups
        )
      
      rm(first_slice_tibble_for_bq_upload)      
      # ...then remove 1st group, set up for rest to be uploaded outside of loop
      tibble_for_bq_upload_by_group_nest <- (tibble_for_bq_upload_by_group_nest
        %>% filter(upload_group != 1))
    }
    
    # Parallelize uploading rest of groups - could be all groups (if original 
    # table was to be appended) or all minus 1st (if original was to create/
    # replace) - all appending (w/ no regard for order) at this point since
    # replacing/creating table already happened w/ 1st group if necessary (and 
    # using "WRITE_TRUNCATE" again would overwrite other groups from same data)
    pwalk(
      with(tibble_for_bq_upload_by_group_nest,
        list(
          fn_tibble_for_upload = slice_tibble_for_bq_upload,
          fn_table_upload_write_disposition = 'WRITE_APPEND',
          fn_group_num = upload_group,
          fn_max_group_num = tibble_num_groups
          )
        ),
      nest_fn_upload_to_bq_and_print_summary
      )
  }
}
