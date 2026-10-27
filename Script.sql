CREATE SCHEMA bronze.lakehouse;
CREATE SCHEMA silver.lakehouse;
CREATE SCHEMA gold.lakehouse;


DROP TABLE IF EXISTS gold.lakehouse.q3_shipping_priority;
DROP TABLE IF EXISTS silver.lakehouse.fact_order_lines;
DROP TABLE IF EXISTS silver.lakehouse.customer_building;
DROP TABLE IF EXISTS bronze.lakehouse.customer;
DROP TABLE IF EXISTS bronze.lakehouse.orders;
DROP TABLE IF EXISTS bronze.lakehouse.lineitem;

SET SESSION bronze.compression_codec = 'ZSTD';
SET SESSION bronze.compression_codec = 'GZIP';

-- Клиенты
CREATE TABLE bronze.lakehouse.customer
WITH (format = 'PARQUET')
AS SELECT 
    custkey,
    mktsegment
FROM tpch.sf1.customer;

-- Заказы
CREATE TABLE bronze.lakehouse.orders
WITH (format = 'PARQUET')
AS SELECT 
    orderkey,
    custkey,
    orderdate,
    shippriority
FROM tpch.sf1.orders;

-- Позиции заказов
CREATE TABLE bronze.lakehouse.lineitem
WITH (format = 'PARQUET')
AS SELECT 
    orderkey,
    extendedprice,
    discount,
    shipdate
FROM tpch.sf1.lineitem;


SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."customer$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."orders$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."lineitem$files";

SELECT count(*) FROM bronze.lakehouse.customer;
SELECT count(*) FROM bronze.lakehouse.orders;
SELECT count(*) FROM bronze.lakehouse.lineitem;

SET SESSION silver.compression_codec = 'ZSTD';

-- Клиенты сегмента BUILDING
CREATE TABLE silver.lakehouse.customer_building
WITH (format = 'PARQUET')
AS SELECT 
    custkey
FROM bronze.lakehouse.customer
WHERE mktsegment = 'BUILDING';

-- фильтр по сегменту и датам, джойн в факт «строки заказов»;
CREATE TABLE silver.lakehouse.fact_order_lines
WITH (format = 'PARQUET')
AS SELECT 
    l.orderkey,
    l.extendedprice,
    l.discount,
    o.orderdate,
    o.shippriority
FROM silver.lakehouse.customer_building c
JOIN bronze.lakehouse.orders o ON c.custkey = o.custkey
JOIN bronze.lakehouse.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15';

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM silver.lakehouse."customer_building$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM silver.lakehouse."fact_order_lines$files";

SELECT count(*) FROM silver.lakehouse.customer_building;
SELECT count(*) FROM silver.lakehouse.fact_order_lines;

SET SESSION gold.compression_codec = 'ZSTD';

CREATE TABLE gold.lakehouse.shipping_priority
WITH (format = 'PARQUET')
AS SELECT
    orderkey,
    SUM(extendedprice * (1 - discount)) AS revenue,
    orderdate,
    shippriority
FROM silver.lakehouse.fact_order_lines
GROUP BY 
    orderkey, 
    orderdate, 
    shippriority
ORDER BY revenue DESC
LIMIT 10;

SELECT * FROM gold.lakehouse.shipping_priority;

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM gold.lakehouse."shipping_priority$files";

SELECT
    l.orderkey,
    SUM(l.extendedprice * (1 - l.discount)) AS revenue,
    o.orderdate,
    o.shippriority
FROM bronze.lakehouse.customer AS c
JOIN bronze.lakehouse.orders   AS o ON c.custkey = o.custkey
JOIN bronze.lakehouse.lineitem AS l ON o.orderkey = l.orderkey
WHERE c.mktsegment = 'BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY 
    l.orderkey, 
    o.orderdate, 
    o.shippriority
ORDER BY revenue DESC
LIMIT 10;

SELECT * 
FROM silver.lakehouse.fact_order_lines 
FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '1' DAY);
