CREATE OR REPLACE MODEL `ice_cream_products.text-embedding-004`
REMOTE WITH CONNECTION `us.vertex_connection`
OPTIONS (endpoint = 'text-embedding-004');
