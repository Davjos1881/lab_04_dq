-- ==============================
-- CREAR BASE DE DATOS
-- ==============================

CREATE DATABASE IF NOT EXISTS lab04
CHARACTER SET utf8mb4
COLLATE utf8mb4_general_ci;

USE lab04;

-- ==============================
-- DIMENSION: DATE
-- ==============================

DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
  invoice_date_key INT PRIMARY KEY,
  full_date DATE NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: PRODUCT
-- ==============================

DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
  product_key INT AUTO_INCREMENT PRIMARY KEY,
  product VARCHAR(100) NOT NULL,
  price DECIMAL(10,2) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: CUSTOMER
-- ==============================

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
  customer_key INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: LOCATION
-- ==============================

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location (
  location_key INT AUTO_INCREMENT PRIMARY KEY,
  country VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- FACT TABLE: SALES
-- ==============================

DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
  sales_key INT AUTO_INCREMENT PRIMARY KEY,

  invoice_id VARCHAR(100) NOT NULL,
  quantity INT NOT NULL,
  total_revenue DECIMAL(12,2) NOT NULL,
  revenue_bin VARCHAR(50),

  invoice_date_key INT NOT NULL,
  product_key INT NOT NULL,
  customer_key INT NOT NULL,
  location_key INT NOT NULL,

  -- Índices
  INDEX (invoice_date_key),
  INDEX (product_key),
  INDEX (customer_key),
  INDEX (location_key),

  -- Foreign Keys
  CONSTRAINT fk_fact_date
    FOREIGN KEY (invoice_date_key)
    REFERENCES dim_date(invoice_date_key),

  CONSTRAINT fk_fact_product
    FOREIGN KEY (product_key)
    REFERENCES dim_product(product_key),

  CONSTRAINT fk_fact_customer
    FOREIGN KEY (customer_key)
    REFERENCES dim_customer(customer_key),

  CONSTRAINT fk_fact_location
    FOREIGN KEY (location_key)
    REFERENCES dim_location(location_key)

) ENGINE=InnoDB;