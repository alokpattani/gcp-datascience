#### BASIC R AND BIGQUERY SETUP ####

suppressMessages({
  # Install bigrquery package
  install.packages("bigrquery", quiet = TRUE)

  # Load tidyverse and bigrquery packages
  library(tidyverse)
  library(bigrquery)
  })

project_id <- "gcp-data-science-demo"

example_query_text <- "
  SELECT *
  FROM
    `bigquery-public-data.ml_datasets.iris`
  LIMIT 10
  "

# Run example query using bq_project_query, get results using bq_table_download
# This will likely ask to authenticate via browser 1st time; subsquently,
# will likely ask if you'd like to use a pre-authorized account for bigrquery
example_query_results <- bq_project_query(project_id, example_query_text) %>%
  bq_table_download()
 

example_query_results


# Alternate way to create connection and run query using BigQuery w/ DBI package
library(DBI)

# Create BigQuery connection using DBI::dbConnect
con <- DBI::dbConnect(bigquery(), project = project_id)

# Run example query and get results using DBI::dbGetQuery
# This will likely ask to authenticate via browser 1st time; subsquently,
# will likely ask if you'd like to use a pre-authorized account for bigrquery
example_query_results <- DBI::dbGetQuery(con, example_query_text)

example_query_results