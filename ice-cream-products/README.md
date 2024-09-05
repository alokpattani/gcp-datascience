# Ice Cream Products - Asset Generation and Recommendations at Scale

This folder contains [BigQuery](cloud.google.com/bigquery) SQL code showing how to use generative AI with [this well-curated Kaggle dataset of 240+ ice cream products](https://www.kaggle.com/datasets/tysonpo/ice-cream-dataset/data) to create assets (short product descriptions, alt text, and review summaries) and recommendations (which products are similar to a given product) at scale, leveraging the integration of [Vertex AI](cloud.google.com/vertex-ai) with BigQuery on [Google Cloud](cloud.google.com).

The SQL scripts in this folder are numbered in order of how they should be run to create the results, with the full process outlined in the following 2 blog posts:
- ["Building Out 🍨 Ice Cream 🍦 Product Assets at Scale with Gemini"](https://medium.com/google-cloud/building-out-ice-cream-product-assets-at-scale-with-gemini-8b629246345b) (corresponds to SQL scripts 01-06)
- ["Getting 🍨 Ice Cream 🍦 Recommendations at Scale with Gemini, Embeddings, and Vector Search"](https://medium.com/google-cloud/getting-ice-cream-recommendations-at-scale-with-gemini-embeddings-and-vector-search-cf1f61a3d55b) (corresponds to SQL scripts 07-09)

Results from this work can be found in the following interactive dashboard pages (Pages 1 and 2 of same Looker Studio dashboard):
- [goo.gle/icecreamproducts](https://lookerstudio.google.com/c/u/0/reporting/f64d610a-4a2d-4fd4-9fbb-ab5e6f9d392f/page/fU1uB)
- [goo.gle/icecreamproductrecs](https://lookerstudio.google.com/c/u/0/reporting/f64d610a-4a2d-4fd4-9fbb-ab5e6f9d392f/page/p_f5tdpmynkd)

Disclaimer: Not an official Google product. Sample code provided is for educational purposes only.
