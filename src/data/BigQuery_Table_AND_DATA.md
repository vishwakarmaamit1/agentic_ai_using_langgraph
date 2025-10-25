# create schema in GCP bigquery

-- ============================================
-- CREATE DATASET (Run once)
-- ============================================
CREATE SCHEMA IF NOT EXISTS `retail_analytics_db`;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- 1. Dimension: Business Group
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_business_group` (
  business_group_id STRING,
  business_group_name STRING,
  description STRING
);

-- 2. Dimension: Product Group
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_product_group` (
  product_group_id STRING,
  product_group_name STRING,
  business_group_id STRING,
  description STRING
);

-- 3. Dimension: Vendor
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_vendor` (
  vendor_id STRING,
  vendor_name STRING,
  contact_email STRING,
  contact_number STRING,
  region STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Dimension: Product / Item
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_product` (
  product_id STRING,
  product_name STRING,
  item_code STRING,
  product_group_id STRING,
  business_group_id STRING,
  vendor_id STRING,
  brand STRING,
  price NUMERIC,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Dimension: Location (India)
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_location` (
  location_id STRING,
  state_name STRING,
  city_name STRING,
  region STRING
);

-- 6. Dimension: Sales Channel
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_channel` (
  channel_id STRING,
  channel_name STRING,  -- 'Online' or 'Store'
  description STRING
);

-- 7. Dimension: Date / Time
CREATE TABLE IF NOT EXISTS `retail_analytics_db.dimension_date` (
  date_id DATE,  -- Primary key
  day INT64,
  month INT64,
  month_name STRING,
  quarter INT64,
  year INT64,
  week_of_year INT64,
  is_weekend BOOL,
  year_month STRING,   -- e.g., 2025-10
  year_quarter STRING  -- e.g., 2025-Q4
);

-- ============================================
-- FACT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS `retail_analytics_db.fact_table_sales` (
  sale_id STRING,
  product_id STRING,
  vendor_id STRING,
  location_id STRING,
  channel_id STRING,
  date_id DATE,             -- Foreign key to dimension_date
  sale_timestamp TIMESTAMP,
  quantity_sold INT64,
  total_sales NUMERIC,
  margin NUMERIC,
  discount NUMERIC,
  net_amount NUMERIC,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
);

# Insert data into table

# dimension_business_group

INSERT INTO `retail_analytics_db.dimension_business_group` (business_group_id, business_group_name, description)
VALUES
('BG001', 'Electronics', 'Electronic appliances and gadgets'),
('BG002', 'Home Essentials', 'Household and furnishing products'),
('BG003', 'Fashion', 'Apparel and accessories');

# dimension_product_group

INSERT INTO `retail_analytics_db.dimension_product_group` (product_group_id, product_group_name, business_group_id, description)
VALUES
('PG001', 'Televisions', 'BG001', 'Smart and LED TVs'),
('PG002', 'Refrigerators', 'BG001', 'Double-door and smart refrigerators'),
('PG003', 'Furniture', 'BG002', 'Home and office furniture'),
('PG004', 'Men Clothing', 'BG003', 'Men’s shirts and trousers');

# dimension_vendor

INSERT INTO `retail_analytics_db.dimension_vendor` (vendor_id, vendor_name, contact_email, contact_number, region)
VALUES
('V001', 'Samsung India', 'sales@samsung.com', '+91-9876543210', 'South'),
('V002', 'LG Electronics', 'support@lg.com', '+91-8765432109', 'West'),
('V003', 'Godrej Interio', 'info@godrejinterio.com', '+91-9123456789', 'North'),
('V004', 'Raymond Apparel', 'sales@raymond.com', '+91-9988776655', 'All India');


# dimension_product

INSERT INTO `retail_analytics_db.dimension_product`
(product_id, product_name, item_code, product_group_id, business_group_id, vendor_id, brand, price)
VALUES
('P001', 'Samsung 55" Smart TV', 'TV55SAM', 'PG001', 'BG001', 'V001', 'Samsung', 55000),
('P002', 'LG Double Door Refrigerator', 'FRLGDD', 'PG002', 'BG001', 'V002', 'LG', 42000),
('P003', 'Godrej Office Chair', 'CHGOD', 'PG003', 'BG002', 'V003', 'Godrej', 7500),
('P004', 'Raymond Formal Shirt', 'RAYFS', 'PG004', 'BG003', 'V004', 'Raymond', 2500);

# dimension_location

INSERT INTO `retail_analytics_db.dimension_location` (location_id, state_name, city_name, region)
VALUES
('LOC001', 'Karnataka', 'Bengaluru', 'South'),
('LOC002', 'Maharashtra', 'Mumbai', 'West'),
('LOC003', 'Delhi', 'New Delhi', 'North'),
('LOC004', 'Tamil Nadu', 'Chennai', 'South');

# dimension_channel

INSERT INTO `retail_analytics_db.dimension_channel` (channel_id, channel_name, description)
VALUES
('CH001', 'Online', 'Online e-commerce channel'),
('CH002', 'Store', 'Physical retail store channel');


# dimension_date
INSERT INTO `retail_analytics_db.dimension_date`
SELECT
  date_value AS date_id,
  EXTRACT(DAY FROM date_value) AS day,
  EXTRACT(MONTH FROM date_value) AS month,
  FORMAT_DATE('%B', date_value) AS month_name,
  EXTRACT(QUARTER FROM date_value) AS quarter,
  EXTRACT(YEAR FROM date_value) AS year,
  EXTRACT(WEEK FROM date_value) AS week_of_year,
  EXTRACT(DAYOFWEEK FROM date_value) IN (1,7) AS is_weekend,
  FORMAT_DATE('%Y-%m', date_value) AS year_month,
  CONCAT(EXTRACT(YEAR FROM date_value), '-Q', EXTRACT(QUARTER FROM date_value)) AS year_quarter
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31', INTERVAL 1 DAY)) AS date_value;

# fact_table_sales

INSERT INTO `retail_analytics_db.fact_table_sales`
(sale_id, product_id, vendor_id, location_id, channel_id, date_id, sale_timestamp, quantity_sold, total_sales, margin, discount, net_amount)
VALUES
-- January (Q1)
('S001', 'P001', 'V001', 'LOC001', 'CH001', '2024-01-10', TIMESTAMP '2024-01-10 10:30:00', 10, 550000, 80000, 10000, 540000),
('S002', 'P002', 'V002', 'LOC002', 'CH002', '2024-01-15', TIMESTAMP '2024-01-15 15:45:00', 5, 210000, 35000, 5000, 205000),

-- April (Q2)
('S003', 'P003', 'V003', 'LOC003', 'CH001', '2024-04-18', TIMESTAMP '2024-04-18 14:00:00', 20, 150000, 25000, 5000, 145000),
('S004', 'P004', 'V004', 'LOC004', 'CH002', '2024-04-25', TIMESTAMP '2024-04-25 18:15:00', 40, 100000, 20000, 2000, 98000),

-- July (Q3)
('S005', 'P001', 'V001', 'LOC002', 'CH002', '2024-07-08', TIMESTAMP '2024-07-08 11:00:00', 12, 660000, 96000, 8000, 652000),
('S006', 'P002', 'V002', 'LOC001', 'CH001', '2024-07-20', TIMESTAMP '2024-07-20 16:45:00', 6, 252000, 42000, 6000, 246000),

-- October (Q4)
('S007', 'P003', 'V003', 'LOC004', 'CH001', '2024-10-12', TIMESTAMP '2024-10-12 13:00:00', 25, 187500, 30000, 2500, 185000),
('S008', 'P004', 'V004', 'LOC003', 'CH002', '2024-10-22', TIMESTAMP '2024-10-22 17:30:00', 60, 150000, 27000, 3000, 147000),

-- Year-end (for YTD aggregation)
('S009', 'P001', 'V001', 'LOC001', 'CH001', '2024-12-05', TIMESTAMP '2024-12-05 12:00:00', 8, 440000, 70000, 6000, 434000),
('S010', 'P002', 'V002', 'LOC002', 'CH002', '2024-12-15', TIMESTAMP '2024-12-15 19:00:00', 10, 420000, 65000, 5000, 415000);
