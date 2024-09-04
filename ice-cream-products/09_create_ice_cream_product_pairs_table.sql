/*
  Do vector searches (1 for each embedding type) as temporary tables, add %tile
  ranking/similarity score of distance and rank of similiarity by product,
  join to table with all potential product pairs for more product info, output
  to (permanent) product_pairs table to feed dashboard page
*/

CREATE OR REPLACE TEMPORARY TABLE ProductsWithShortDescriptionEmbeddings AS
(
  SELECT
    *
  
  FROM
    `ice_cream_products.product_embeddings`

  WHERE
    short_description_embedding_length != 0
)
;

CREATE OR REPLACE TEMPORARY TABLE ProductsWithReviewSummaryEmbeddings AS
(
  SELECT
    *
  
  FROM
    `ice_cream_products.product_embeddings`

  WHERE
    review_summary_embedding_length != 0
)
;

CREATE OR REPLACE TEMPORARY TABLE ProductPairsShortDescriptionDistances AS 
(
  SELECT
    query.key AS key_1,
    query.brand_and_product_name AS brand_and_product_name_1,
    
    base.key AS key_2,
    base.brand_and_product_name AS brand_and_product_name_2,

    /* Force distance to 0 if same product (to get around floating point issues) */
    IF(query.key = base.key, 0, distance) AS short_description_distance,
    
    IF(distance IS NULL, 
      CAST(NULL AS INT64),
      RANK() OVER (PARTITION BY (distance IS NULL) ORDER BY IF(query.key = base.key, 0, distance))
      ) AS ovr_rk_short_description_distance,
      
    (SUM(IF(distance IS NOT NULL, 1, 0)) OVER ()) AS ovr_num_short_description_distances,

    /* Rank product 2s by description distance vs product 1 (NULL for same product and NULL distances) */
    IF(distance IS NULL OR (query.key = base.key),
      CAST(NULL AS INT64),
      RANK() OVER (PARTITION BY query.key, (query.key = base.key), (distance IS NULL)
        ORDER BY IF(query.key = base.key, 0, distance))
      ) AS product_1_comps_rk_short_description_distance

  FROM
    /* Filter to products w/ actual short description embedding to avoid errors */  
    VECTOR_SEARCH(
      TABLE ProductsWithShortDescriptionEmbeddings,
      'short_description_embedding',
      TABLE ProductsWithShortDescriptionEmbeddings,
      'short_description_embedding',
      /* 241 products in entire dataset - getting distances for all pairs */
      top_k => 241,
      distance_type => 'COSINE',
      options => '{"use_brute_force":true}'
      )
);

CREATE OR REPLACE TEMPORARY TABLE ProductPairsReviewSummaryDistances AS 
(
  SELECT
    query.key AS key_1,
    query.brand_and_product_name AS brand_and_product_name_1,
    
    base.key AS key_2,
    base.brand_and_product_name AS brand_and_product_name_2,
    
    /* Force distance to 0 if same product (to get around floating point issues) */
    IF(query.key = base.key, 0, distance) AS review_summary_distance,

    IF(distance IS NULL, 
      CAST(NULL AS INT64),
      RANK() OVER (PARTITION BY (distance IS NULL) ORDER BY IF(query.key = base.key, 0, distance))
      ) AS ovr_rk_review_summary_distance,

    (SUM(IF(distance IS NOT NULL, 1, 0)) OVER ()) AS ovr_num_review_summary_distances,

    /* Rank product 2s by review summary distance vs product 1 (NULL for same product and NULL distances) */
    IF(distance IS NULL OR (query.key = base.key),
      CAST(NULL AS INT64),
      RANK() OVER (PARTITION BY query.key, (query.key = base.key), (distance IS NULL)
        ORDER BY IF(query.key = base.key, 0, distance))
      ) AS product_1_comps_rk_review_summary_distance

  FROM
    VECTOR_SEARCH(
      /* Filter to products w/ actual review summary embedding to avoid errors */
      TABLE ProductsWithReviewSummaryEmbeddings,
      'review_summary_embedding',
      TABLE ProductsWithReviewSummaryEmbeddings,
      'review_summary_embedding',
      /* 241 products in entire dataset - getting distances for all pairs */
      top_k => 241,
      distance_type => 'COSINE',
      options => '{"use_brute_force":true}'
      )
);

CREATE OR REPLACE TABLE `ice_cream_products.product_pairs` AS
(
  /* CROSS JOIN to get all product pairs, not just those w/ distance calcs*/
  WITH
  ProductPairs AS
  (
    SELECT 
      Products1.key AS key_1,
      Products1.brand_name AS brand_name_1,
      Products1.brand_and_product_name AS brand_and_product_name_1,
      Products1.image_gcs_file_link AS image_gcs_file_link_1,
      Products1.avg_rating AS avg_rating_1,
      Products1.num_ratings AS num_ratings_1,
      Products1.generate_short_product_description_result AS short_description_1,
      Products1.generate_review_summary_result AS review_summary_1,

      Products2.key AS key_2,
      Products2.brand_name AS brand_name_2,
      Products2.brand_and_product_name AS brand_and_product_name_2,
      Products2.image_gcs_file_link AS image_gcs_file_link_2,
      Products2.avg_rating AS avg_rating_2,
      Products2.num_ratings AS num_ratings_2,
      Products2.generate_short_product_description_result AS short_description_2,
      Products2.generate_review_summary_result AS review_summary_2,

      (Products1.key = Products2.key) AS same_product

    FROM
      `ice_cream_products.products_results` Products1

    CROSS JOIN
      `ice_cream_products.products_results` Products2
  ),

  ProductPairsWithDistances AS
  (
    SELECT
      ProductPairs.key_1,
      ProductPairs.brand_name_1,
      ProductPairs.brand_and_product_name_1,
      ProductPairs.image_gcs_file_link_1,
      ProductPairs.avg_rating_1,
      ProductPairs.num_ratings_1,

      ProductPairs.key_2,
      ProductPairs.brand_name_2,
      ProductPairs.brand_and_product_name_2,
      ProductPairs.image_gcs_file_link_2,
      ProductPairs.avg_rating_2,
      ProductPairs.num_ratings_2,

      ProductPairs.same_product,

      /* Use original descriptions instead of modified version for distance calcs, even though
        they include "No valid description"-type results (distance calcs NULL those out) */
      ProductPairs.short_description_1,
      ProductPairs.short_description_2,

      /* Short description distance calculation, rank, and overall # */
      ProductPairsShortDescriptionDistances.short_description_distance,
      ProductPairsShortDescriptionDistances.ovr_rk_short_description_distance,
      ProductPairsShortDescriptionDistances.ovr_num_short_description_distances,

      /* Turn short description distance rank into %tile-based similarity score (on 0-100 scale) */
      IF(
        (ProductPairsShortDescriptionDistances.short_description_distance IS NULL),
        CAST(NULL AS FLOAT64),
        SAFE_DIVIDE(
          (ProductPairsShortDescriptionDistances.ovr_num_short_description_distances
            - ProductPairsShortDescriptionDistances.ovr_rk_short_description_distance
            + 0.5),
          ProductPairsShortDescriptionDistances.ovr_num_short_description_distances
          )
        ) * 100 AS short_description_similarity_score,
      
      ProductPairsShortDescriptionDistances.product_1_comps_rk_short_description_distance,

      /* Use original review summaries instead of modfied version for distance calcs, even though
        they included "No valid reviews"-type results (distances calcs NULL those out) */
      ProductPairs.review_summary_1,
      ProductPairs.review_summary_2,

      /* Review summary distance calculation, rank, and overall # */
      ProductPairsReviewSummaryDistances.review_summary_distance,
      ProductPairsReviewSummaryDistances.ovr_rk_review_summary_distance,
      ProductPairsReviewSummaryDistances.ovr_num_review_summary_distances,

      /* Turn review summary distance rank into %tile-based similarity score (on 0-100 scale) */
      IF(
        (ProductPairsReviewSummaryDistances.review_summary_distance IS NULL),
        CAST(NULL AS FLOAT64),
        SAFE_DIVIDE(
          (ProductPairsReviewSummaryDistances.ovr_num_review_summary_distances
            - ProductPairsReviewSummaryDistances.ovr_rk_review_summary_distance
            + 0.5),
          ProductPairsReviewSummaryDistances.ovr_num_review_summary_distances
          )
        ) * 100 AS review_summary_similarity_score,

      ProductPairsReviewSummaryDistances.product_1_comps_rk_review_summary_distance

    FROM
      ProductPairs

    LEFT JOIN
      ProductPairsShortDescriptionDistances USING (key_1, key_2)

    LEFT JOIN
      ProductPairsReviewSummaryDistances USING (key_1, key_2)
  )

  SELECT
    *

  FROM
    ProductPairsWithDistances
  
  ORDER BY
    key_1, short_description_distance, review_summary_distance
);
