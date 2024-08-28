CREATE OR REPLACE MODEL `ice_cream_products.gemini-1dot5-flash`
REMOTE WITH CONNECTION `us.vertex_connection`
OPTIONS (endpoint = 'gemini-1.5-flash');
