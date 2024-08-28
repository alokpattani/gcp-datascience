CREATE OR REPLACE EXTERNAL TABLE `ice_cream_products.product_images`
WITH CONNECTION `us.vertex_connection`
OPTIONS (
 object_metadata = 'SIMPLE',
 uris = ['gs://ice_cream_products/images/*']
 );
