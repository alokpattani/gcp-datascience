DECLARE CLOUD_STORAGE_BUCKET STRING DEFAULT 'ice_cream_products';
DECLARE CLOUD_STORAGE_SUBDIR STRING DEFAULT 'images';

CREATE OR REPLACE TEMPORARY TABLE Products AS
  WITH
  ProductsWithInfo AS
  (
    SELECT
      key,
      brand,
      (CASE
        WHEN brand = "bj" THEN "Ben & Jerry's"
        WHEN brand = "breyers" THEN "Breyers"
        WHEN brand = "hd" THEN "Häagen-Dazs"
        WHEN brand = "talenti" THEN "Talenti"
        ELSE NULL
        END
        ) AS brand_name,

      name,    
      subhead,
      description,
      ingredients,

      ('https://storage.googleapis.com/' || CLOUD_STORAGE_BUCKET || '/' || 
        CLOUD_STORAGE_SUBDIR || '/' || key || '.png')
        AS image_gcs_file_link,

      ('gs://' || CLOUD_STORAGE_BUCKET || '/' || CLOUD_STORAGE_SUBDIR || '/' || 
        key || '.png') AS image_gcs_file_uri,

      `ice_cream_products.get_product_short_description_prompt`(
        name, subhead, description) AS product_short_description_prompt

    FROM
      `ice_cream_products.products`
  ),

  ProductsWithShortDescriptions AS 
  (
    SELECT
      * EXCEPT (prompt, ml_generate_text_status, ml_generate_text_result),

      (brand_name || ' ' || name) AS brand_and_product_name,

      ml_generate_text_status AS generate_short_product_description_status,

      JSON_VALUE(ml_generate_text_result, "$.candidates[0].content.parts[0].text")
        AS generate_short_product_description_result

    FROM  
      ML.GENERATE_TEXT(
        MODEL `ice_cream_products.gemini-1dot5-flash`,
        (
          SELECT
            ProductsWithInfo.* EXCEPT (product_short_description_prompt),
            product_short_description_prompt AS prompt   
        
          FROM
            ProductsWithInfo
        ),
        STRUCT(
          0 AS temperature, /* 0 temp for most deterministic (hopefully more factual) output */
          60 AS max_output_tokens /* Small # since we are looking for short descriptions here */
          )
        )
  )

  SELECT *

  FROM ProductsWithShortDescriptions
;

CREATE OR REPLACE TEMPORARY TABLE ProductImages AS
  WITH
  ProductImagesWithAltText AS
  (
    SELECT
      * EXCEPT (ml_generate_text_result, ml_generate_text_status),
      
      ml_generate_text_status AS generate_alt_text_status,

      JSON_VALUE(ml_generate_text_result, "$.candidates[0].content.parts[0].text")
        AS generate_alt_text_result

    FROM
      ML.GENERATE_TEXT(
        MODEL `ice_cream_products.gemini-1dot5-flash`,
        TABLE `ice_cream_products.product_images`,
        STRUCT(
          '''You are an ice cream product image alt text generator, who takes in images of ice cream
            products, then outputs alt text that can be used to increase accesibility on apps/websites,
            on social media, etc.

            Use ONLY the image provided to generate the alt text (do not hallucinate additional
            information).

            If the input doesn't look like an image of an ice cream product, please respond with 'No
            valid product image or name to create alt text from.'

            Create alt text from the product image provided, conveying what the image shows and no more.
            The result should be no more than 15 words, and not get cut off mid-sentence.
          ''' AS PROMPT,
          0 AS temperature, /* 0 temp for most deterministic (hopefully more factual) output */
          60 AS max_output_tokens /* Small # since we are looking for short alt text here */
          )
        )
  )

  SELECT *

  FROM
    ProductImagesWithAltText
;

CREATE OR REPLACE TEMPORARY TABLE ProductReviewSummaryInfo AS
  WITH
  ProductReviewAggregates AS
  (
    SELECT
      key,

      AVG(stars) AS avg_rating,
      COUNTIF(stars IS NOT NULL) AS num_ratings,

      MIN(date) AS first_review_date,
      MAX(date) AS last_review_date,

      STRING_AGG(('"' || title || '"\nStars: ' || stars || '\nAuthor: ' || author || "\nDate: "
        || FORMAT_DATE('%b %d, %Y', date) || '\n\n' || text), 
        '\n\n\n'
        ORDER BY (helpful_yes - helpful_no) DESC, date DESC
        )
        AS all_reviews_concat

    FROM
      `ice_cream_products.reviews` 

    GROUP BY
      key, brand
    
    ORDER BY
      avg_rating DESC, num_ratings
  ),

  ProductReviewSummaries AS
  (
    SELECT
      * EXCEPT (prompt, ml_generate_text_result, ml_generate_text_status),
      
      ml_generate_text_status AS generate_review_summary_status,

      JSON_VALUE(ml_generate_text_result, "$.candidates[0].content.parts[0].text")
        AS generate_review_summary_result

    FROM
      ML.GENERATE_TEXT(
        MODEL `ice_cream_products.gemini-1dot5-flash`,
        (
          SELECT
            ProductReviewAggregates.*,
            `ice_cream_products.get_product_reviews_summarization_prompt`(
              ProductReviewAggregates.all_reviews_concat) AS prompt
        
          FROM
            ProductReviewAggregates
        ),
        STRUCT(
          0 AS temperature, /* 0 temp for most deterministic (hopefully more factual) output */
          150 AS max_output_tokens /* Small # since we are looking for shorter summaries here */
          )
        )
  )

  SELECT *

  FROM
    ProductReviewSummaries
;

CREATE OR REPLACE TEMPORARY TABLE ProductsWithAllInfo AS
  WITH 
  ProductsWithImagesAndReviewSummaries AS
  (
    SELECT
      Products.key,
      Products.brand_and_product_name,
      
      Products.brand,
      Products.brand_name,
      Products.name,
      
      Products.subhead,
      Products.description,
      Products.ingredients,

      Products.image_gcs_file_link,
      Products.image_gcs_file_uri,

      ProductReviewSummaryInfo.avg_rating,
      ProductReviewSummaryInfo.num_ratings,

      ProductReviewSummaryInfo.first_review_date,
      ProductReviewSummaryInfo.last_review_date,

      ProductReviewSummaryInfo.all_reviews_concat,
      (
        Products.brand_and_product_name ||
        IF(Products.subhead IS NULL, '', '\n' || Products.subhead) ||
        IF(Products.description IS NULL, '', '\n\n' || Products.description) ||
        IF(Products.ingredients IS NULL, '', '\n\nIngredients: ' || Products.ingredients) ||
        IF(ProductReviewSummaryInfo.all_reviews_concat IS NULL, '',  '\n\nAll ' || 
          ProductReviewSummaryInfo.num_ratings || ' Reviews:\n\n' || 
          ProductReviewSummaryInfo.all_reviews_concat
          )
      ) AS product_all_provided_info,

      Products.generate_short_product_description_status,
      Products.generate_short_product_description_result,      

      ProductImages.generate_alt_text_status,
      ProductImages.generate_alt_text_result,

      ProductReviewSummaryInfo.generate_review_summary_status,
      ProductReviewSummaryInfo.generate_review_summary_result

    FROM
      Products

    LEFT JOIN
      ProductImages ON (Products.image_gcs_file_uri = ProductImages.uri)

    LEFT JOIN
      ProductReviewSummaryInfo USING (key)
  )

  SELECT
    *

  FROM 
    ProductsWithImagesAndReviewSummaries
;

CREATE OR REPLACE TABLE `ice_cream_products.products_results` AS

  SELECT *

  FROM
    ProductsWithAllInfo

  ORDER BY
    avg_rating DESC, num_ratings DESC, key
;
