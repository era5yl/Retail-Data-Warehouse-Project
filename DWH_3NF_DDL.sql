-- DDL: BL_3NF layer

CREATE SCHEMA IF NOT EXISTS bl_3nf;

-- ---------- SEQUENCES (no SERIAL) ----------
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_geography          START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_products           START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_employees          START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_resellers          START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_stores              START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_dates              START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_customers_scd2     START WITH 1 INCREMENT BY 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_sales_orders       START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_targets            START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_seasons            START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_categories START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_subcategories START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_categories START WITH 1 INCREMENT BY 1;
-- ---------- TABLES ----------
DROP TABLE IF EXISTS bl_3nf.ce_targets CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_sales_orders  CASCADE;

DROP TABLE IF EXISTS bl_3nf.ce_customers_scd2 CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_resellers CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_stores CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_employees CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_products CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_product_subcategories CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_product_categories CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_employee_territory CASCADE;

DROP TABLE IF EXISTS bl_3nf.ce_dates CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_geography CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_seasons  CASCADE;

-- 1) Seasons 
CREATE TABLE bl_3nf.ce_seasons
(
    season_surr_id BIGINT PRIMARY KEY,
    europe_season  VARCHAR(50),
    north_america_season VARCHAR(50),
    asia_season VARCHAR(50),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 2) Dates (Seasons are foreighn key)
CREATE TABLE bl_3nf.ce_dates
(
    date_surr_id BIGINT PRIMARY KEY,
    full_date_label DATE UNIQUE,
    quarter VARCHAR(10),
    season_surr_id BIGINT REFERENCES bl_3nf.ce_seasons(season_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 3) Geography
CREATE TABLE bl_3nf.ce_geography
(
    geography_surr_id BIGINT PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    geography_type VARCHAR(50),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 4) Employees
CREATE TABLE bl_3nf.ce_employees
(
    employee_surr_id BIGINT PRIMARY KEY,
    employee_business_id VARCHAR(50) UNIQUE,
    name VARCHAR(200),
    title VARCHAR(100),
    email VARCHAR(200),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 5) Employees-geography many-to-many relationship
CREATE TABLE bl_3nf.ce_employee_territory
(
    geography_surr_id BIGINT REFERENCES bl_3nf.ce_geography(geography_surr_id),
    employee_surr_id  BIGINT REFERENCES bl_3nf.ce_employees(employee_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200),

    PRIMARY KEY (geography_surr_id, employee_surr_id)
);

-- 6) Resellers (AdventureWorks) -> Geography
CREATE TABLE bl_3nf.ce_resellers
(
    reseller_surr_id BIGINT PRIMARY KEY,
    reseller_name VARCHAR(200),
    reseller_business_type VARCHAR(100),
    geography_surr_id BIGINT REFERENCES bl_3nf.ce_geography(geography_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 7) Stores (Contoso) -> Geography
CREATE TABLE bl_3nf.ce_stores
(
    store_surr_id BIGINT PRIMARY KEY,
    store_key VARCHAR(100) UNIQUE,
    store_status VARCHAR(50),
    store_parent_entity VARCHAR(200),
    entity_description VARCHAR(200),
    entity_type VARCHAR(50),
    entity_status VARCHAR(50),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 8) Product categories
CREATE TABLE bl_3nf.ce_product_categories
(
    product_category_surr_id BIGINT PRIMARY KEY,
    product_category_description VARCHAR(200),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 9) Product subcategory
CREATE TABLE bl_3nf.ce_product_subcategories
(
    product_subcategory_surr_id BIGINT PRIMARY KEY,
    product_subcategory_label VARCHAR(200) UNIQUE,
    product_subcategory_description VARCHAR(200),
    product_category_surr_id BIGINT REFERENCES bl_3nf.ce_product_categories(product_category_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 10) Product
CREATE TABLE bl_3nf.ce_products
(
    product_surr_id BIGINT PRIMARY KEY,
    product_description VARCHAR(255),
    manufacturer VARCHAR(100),
    brand_name VARCHAR(100),
    class_name VARCHAR(100),
    style_name VARCHAR(100),
    product_subcategory_surr_id BIGINT REFERENCES bl_3nf.ce_product_subcategories(product_subcategory_surr_id),
    size VARCHAR(50),
    weight NUMERIC(18,4),
    stock_type VARCHAR(50),
    status_product VARCHAR(50),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);

-- 11) Customers SCD2

CREATE TABLE bl_3nf.ce_customers_scd2
(
    -- Surrogate Key
    customer_surr_id      BIGINT       NOT NULL,

    -- Natural Key
    customer_label        VARCHAR(200) NOT NULL,

    -- Attributes
    customer_name         VARCHAR(200),
    birth_date            DATE,
    gender                VARCHAR(50),
    education             VARCHAR(100),
    occupation            VARCHAR(100),
    yearly_income         NUMERIC(18,2),
    total_children        INT,
    number_children_at_home INT,
    house_owner_flag      VARCHAR(10),
    number_cars_owned     INT,

    -- SCD2 Technical Columns
    start_dt              DATE         NOT NULL,
    end_dt                DATE         NOT NULL,
    is_active             CHAR(1)      NOT NULL,

    -- Technical Metadata
    insert_dt             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_dt             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system         VARCHAR(50),
    source_entity         VARCHAR(50),
    source_id             VARCHAR(200),

    -- Constraints
    CONSTRAINT pk_ce_customers_scd2 
        PRIMARY KEY (customer_surr_id, start_dt),

    CONSTRAINT chk_ce_customers_scd2_active
        CHECK (is_active IN ('Y','N'))
);


-- 12) SALES (fact-like unified transactions) - NO default row
CREATE TABLE bl_3nf.ce_sales_orders
(
    sales_surr_id BIGINT PRIMARY KEY,
    sales_order_id VARCHAR(200) UNIQUE,

    sales_amount NUMERIC(19,4),
    quantity INT,
    unit_price NUMERIC(19,4),
	cost NUMERIC(19,4),
    date_surr_id BIGINT REFERENCES bl_3nf.ce_dates(date_surr_id),
    customer_surr_id BIGINT,  -- if you want FK, point to ce_customers_scd2 (composite issue)
    product_surr_id BIGINT REFERENCES bl_3nf.ce_products(product_surr_id),
    employee_surr_id BIGINT REFERENCES bl_3nf.ce_employees(employee_surr_id),
    geography_surr_id BIGINT REFERENCES bl_3nf.ce_geography(geography_surr_id),
    reseller_surr_id BIGINT REFERENCES bl_3nf.ce_resellers(reseller_surr_id),
    store_surr_id    BIGINT REFERENCES bl_3nf.ce_stores(store_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);


-- 13) TARGETS (fact_like)
CREATE TABLE bl_3nf.ce_targets
(
    target_surr_id BIGINT PRIMARY KEY,
    target NUMERIC(19,4),

    date_surr_id BIGINT REFERENCES bl_3nf.ce_dates(date_surr_id),

    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    source_id VARCHAR(200)
);


