/* 
  Create 2 embeddings tables as temporary tables, join them by key, output to 
  (permanent) product_embeddings table
*/
  
CREATE OR REPLACE TEMPORARY TABLE ShortDescriptionEmbeddings AS 
(
  SELECT
    key,
    brand_and_product_name,
    content AS short_description,

    ml_generate_embedding_status AS short_description_embedding_status,
    JSON_VALUE(ml_generate_embedding_statistics, "$.token_count") AS 
      short_description_embedding_token_count,
    JSON_VALUE(ml_generate_embedding_statistics, "$.truncated") AS 
      short_description_embedding_truncated,
         
    ml_generate_embedding_result AS short_description_embedding,
    ARRAY_LENGTH(ml_generate_embedding_result) AS short_description_embedding_length

  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `ice_cream_products.text-embedding-004`,
      (
        SELECT
          key,
          brand_and_product_name,
          /* Handle when description (from previous Gemini call) is not actually real */
          IF(TRIM(generate_short_product_description_result) = 
              'No valid product description to summarize.',
            NULL,
            generate_short_product_description_result
            ) AS content

        FROM
          `ice_cream_products.products_results` 
      ),
      STRUCT(
        TRUE AS flatten_json_output,
        'SEMANTIC_SIMILARITY' AS task_type
        )
    )
);

CREATE OR REPLACE TEMPORARY TABLE ReviewSummaryEmbeddings AS 
(
  SELECT
    key,
    brand_and_product_name,
    content AS review_summary,

    ml_generate_embedding_status AS review_summary_embedding_status,
    JSON_VALUE(ml_generate_embedding_statistics, "$.token_count") AS 
      review_summary_embedding_token_count,
    JSON_VALUE(ml_generate_embedding_statistics, "$.truncated") AS 
      review_summary_embedding_truncated,      
    
    ml_generate_embedding_result AS review_summary_embedding,
    ARRAY_LENGTH(ml_generate_embedding_result) AS review_summary_embedding_length

  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `ice_cream_products.text-embedding-004`,
      (
        SELECT
          key,
          brand_and_product_name,
          /* Handle when summary (from previous Gemini call) is not actually real */       
          IF(TRIM(generate_review_summary_result) = 'No valid reviews to summarize.',
            NULL,
            generate_review_summary_result
            ) AS content

        FROM
          `ice_cream_products.products_results` 
      ),
      STRUCT(
        TRUE AS flatten_json_output,
        'SEMANTIC_SIMILARITY' AS task_type
        )
    )
);

CREATE OR REPLACE TABLE `ice_cream_products.product_embeddings` AS
(
  SELECT
    IFNULL(ShortDescriptionEmbeddings.key, ReviewSummaryEmbeddings.key) AS key,
    IFNULL(ShortDescriptionEmbeddings.brand_and_product_name, 
      ReviewSummaryEmbeddings.brand_and_product_name)
      AS brand_and_product_name,
    
    ShortDescriptionEmbeddings.short_description,
    ReviewSummaryEmbeddings.review_summary,

    ShortDescriptionEmbeddings.short_description_embedding_status,
    ShortDescriptionEmbeddings.short_description_embedding_token_count,
    ShortDescriptionEmbeddings.short_description_embedding_truncated,

    ShortDescriptionEmbeddings.short_description_embedding,
    ShortDescriptionEmbeddings.short_description_embedding_length,

    ReviewSummaryEmbeddings.review_summary_embedding_status,
    ReviewSummaryEmbeddings.review_summary_embedding_token_count,
    ReviewSummaryEmbeddings.review_summary_embedding_truncated,

    ReviewSummaryEmbeddings.review_summary_embedding,
    ReviewSummaryEmbeddings.review_summary_embedding_length

  FROM
    ShortDescriptionEmbeddings

  FULL OUTER JOIN
    ReviewSummaryEmbeddings ON
      ShortDescriptionEmbeddings.key = ReviewSummaryEmbeddings.key
);
