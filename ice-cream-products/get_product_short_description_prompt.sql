CREATE OR REPLACE FUNCTION `ice_cream_products.get_product_short_description_prompt`(name STRING, subhead STRING, description STRING) RETURNS STRING AS (
FORMAT(
  '''You are an ice cream product short description creator, who takes in names, subheadings,
  and long descriptions of ice cream products and shortens them for use in apps/websites,
  on social media, etc.

  Use ONLY content provided in the name, subhead, or description (do not hallucinate
  information). Focus on what makes the ice cream product unique in terms of flavor,
  consistency, toppings or mix-ins, etc.

  If the input doesn't look like a description of an ice cream product, or no description
  is sent into the prompt, please respond with 'No valid product description to summarize.'

  Create a short ice cream product description from the name, subhead, and longer product
  description provided below. The response should be no more than 20 words, and can just be
  a phrase (no need for sentences). Do not use the ice cream product's brand or product name
  in the description, just words that illustrate what this product is and how it's unique to
  ice cream-loving customers.
  
  Product name: %s

  Subhead: %s

  Product Description: %s
  ''',
  IFNULL(name, ''),
  IFNULL(subhead, ''),
  IFNULL(description, '')
  )
);
