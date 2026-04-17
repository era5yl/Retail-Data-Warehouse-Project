CREATE SCHEMA IF NOT EXISTS bl_dm;

-- drop rerunnable
DROP TABLE IF EXISTS bl_dm.fct_sales_orders CASCADE;
DROP TABLE IF EXISTS bl_dm.fct_targets CASCADE;

DROP TABLE IF EXISTS bl_dm.dim_resellers CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_employees CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_customers_scd2_dm CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_stores CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_products CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_dates CASCADE;
DROP TABLE IF EXISTS bl_dm.dim_geography CASCADE;

DROP SEQUENCE IF EXISTS bl_dm.seq_dim_geography;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_dates;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_products;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_resellers;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_employees;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_stores;
DROP SEQUENCE IF EXISTS bl_dm.seq_dim_customers;

DROP SEQUENCE IF EXISTS bl_dm.seq_fct_sales_orders;
DROP SEQUENCE IF EXISTS bl_dm.seq_fct_targets;

-- sequences
CREATE SEQUENCE bl_dm.seq_dim_geography  START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_dates      START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_products   START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_resellers  START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_employees  START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_stores     START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_dim_customers  START WITH 1 INCREMENT BY 1;

CREATE SEQUENCE bl_dm.seq_fct_sales_orders START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE bl_dm.seq_fct_targets      START WITH 1 INCREMENT BY 1;

-- ---------------- DIM_GEOGRAPHY (from bl_3nf.ce_geography) ----------------
CREATE TABLE bl_dm.dim_geography
(
  geography_surr_id   BIGINT PRIMARY KEY,
  geography_3nf_id    BIGINT UNIQUE NOT NULL,

  city                VARCHAR(100) NOT NULL,
  state               VARCHAR(100) NOT NULL,
  country             VARCHAR(100) NOT NULL,
  geography_type      VARCHAR(50)  NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_DATES (from ce_dates + keep season_surr_id) ----------------
CREATE TABLE bl_dm.dim_dates
(
  date_surr_id        BIGINT PRIMARY KEY,
  date_3nf_id         BIGINT UNIQUE NOT NULL,

  full_date           DATE NOT NULL,
  quarter             VARCHAR(10) NOT NULL,
  season_3nf_id       BIGINT NOT NULL,     -- from 3NF ce_dates.season_surr_id

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_PRODUCTS (from ce_products + subcat + cat) ----------------
CREATE TABLE bl_dm.dim_products
(
  product_surr_id     BIGINT PRIMARY KEY,
  product_3nf_id      BIGINT UNIQUE NOT NULL,

  product_description VARCHAR(255) NOT NULL,
  manufacturer        VARCHAR(100) NOT NULL,
  brand_name          VARCHAR(100) NOT NULL,
  class_name          VARCHAR(100) NOT NULL,
  style_name          VARCHAR(100) NOT NULL,

  size                VARCHAR(50)  NOT NULL,
  weight              NUMERIC(18,4),
  stock_type          VARCHAR(50)  NOT NULL,
  status_product      VARCHAR(50)  NOT NULL,

  product_subcategory_label        VARCHAR(200) NOT NULL,
  product_subcategory_description  VARCHAR(200) NOT NULL,
  product_category_description     VARCHAR(200) NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_RESELLERS (from ce_resellers) ----------------
CREATE TABLE bl_dm.dim_resellers
(
  reseller_surr_id    BIGINT PRIMARY KEY,
  reseller_3nf_id     BIGINT UNIQUE NOT NULL,

  reseller_name       VARCHAR(200) NOT NULL,
  reseller_business_type VARCHAR(100) NOT NULL,

  geography_surr_id   BIGINT NOT NULL REFERENCES bl_dm.dim_geography(geography_surr_id),

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_EMPLOYEES (from ce_employees) ----------------
CREATE TABLE bl_dm.dim_employees
(
  employee_surr_id    BIGINT PRIMARY KEY,
  employee_3nf_id     BIGINT UNIQUE NOT NULL,

  employee_business_id VARCHAR(50) NOT NULL,
  name                VARCHAR(200) NOT NULL,
  title               VARCHAR(100) NOT NULL,
  email               VARCHAR(200) NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_STORES (from ce_stores) ----------------
CREATE TABLE bl_dm.dim_stores
(
  store_surr_id       BIGINT PRIMARY KEY,
  store_3nf_id        BIGINT UNIQUE NOT NULL,

  store_key           VARCHAR(100) NOT NULL,
  store_status        VARCHAR(50)  NOT NULL,
  store_parent_entity VARCHAR(200) NOT NULL,
  entity_description  VARCHAR(200) NOT NULL,
  entity_type         VARCHAR(50)  NOT NULL,
  entity_status       VARCHAR(50)  NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- ---------------- DIM_CUSTOMERS (from active rows in ce_customers_scd2) ----------------
CREATE TABLE bl_dm.dim_customers_scd2_dm
(
  customer_surr_id    BIGINT PRIMARY KEY,      -- DM surrogate (unique per version)
  customer_3nf_id     BIGINT NOT NULL,         -- business key (3NF surrogate)

  customer_label      VARCHAR(200) NOT NULL,
  customer_name       VARCHAR(200) NOT NULL,
  birth_date          DATE,
  gender              VARCHAR(50)  NOT NULL,
  education           VARCHAR(100) NOT NULL,
  occupation          VARCHAR(100) NOT NULL,
  yearly_income       NUMERIC(18,2),
  total_children      INT,
  number_children_at_home INT,
  house_owner_flag    VARCHAR(10)  NOT NULL,
  number_cars_owned   INT,

  -- SCD2 columns
  start_dt            DATE NOT NULL,
  end_dt              DATE NOT NULL,
  is_active           CHAR(1) NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL
);

-- Helpful constraint: only one active row per customer_3nf_id
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customers_scd2_active
  ON bl_dm.dim_customers_scd2_dm (customer_3nf_id)
  WHERE is_active = 'Y';

-- Optional: speed lookup by customer_3nf_id
CREATE INDEX IF NOT EXISTS ix_dim_customers_scd2_bk
  ON bl_dm.dim_customers_scd2_dm (customer_3nf_id, is_active, start_dt, end_dt);



-- ---------------- FACT SALES (from ce_sales_orders) ----------------
CREATE TABLE bl_dm.fct_sales_orders
(
  sales_order_surr_id BIGINT NOT NULL DEFAULT nextval('bl_dm.seq_fct_sales_orders'),
  sales_order_id      VARCHAR(200) NOT NULL,

  -- partition key (derived from dim_dates.full_date)
  sales_dt            DATE NOT NULL,

  date_surr_id        BIGINT NOT NULL REFERENCES bl_dm.dim_dates(date_surr_id),
  customer_surr_id    BIGINT NOT NULL REFERENCES bl_dm.dim_customers(customer_surr_id),
  product_surr_id     BIGINT NOT NULL REFERENCES bl_dm.dim_products(product_surr_id),
  employee_surr_id    BIGINT NOT NULL REFERENCES bl_dm.dim_employees(employee_surr_id),
  geography_surr_id   BIGINT NOT NULL REFERENCES bl_dm.dim_geography(geography_surr_id),
  reseller_surr_id    BIGINT NOT NULL REFERENCES bl_dm.dim_resellers(reseller_surr_id),
  store_surr_id       BIGINT NOT NULL REFERENCES bl_dm.dim_stores(store_surr_id),

  sales_amount        NUMERIC(19,4),
  quantity            INT,
  unit_price          NUMERIC(19,4),
  cost                NUMERIC(19,4),
  profit              NUMERIC(19,4),

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) NOT NULL,

  -- Postgres rule: PK/UNIQUE must include partition key
  CONSTRAINT pk_fct_sales_orders PRIMARY KEY (sales_order_surr_id, sales_dt),
  CONSTRAINT uq_fct_sales_orders_sales_order_id UNIQUE (sales_order_id, sales_dt)
)
PARTITION BY RANGE (sales_dt);

-- Helpful index for window filters
CREATE INDEX IF NOT EXISTS ix_fct_sales_orders_sales_dt
  ON bl_dm.fct_sales_orders (sales_dt);

-- Helpful lookup by sales_order_id
CREATE INDEX IF NOT EXISTS ix_fct_sales_orders_sales_order_id
  ON bl_dm.fct_sales_orders (sales_order_id);



-- ---------------- FACT TARGETS (from ce_targets) ----------------
CREATE TABLE bl_dm.fct_targets
(
  target_surr_id      BIGINT PRIMARY KEY,

  date_surr_id        BIGINT NOT NULL REFERENCES bl_dm.dim_dates(date_surr_id),
  target_amount       NUMERIC(19,4) NOT NULL,

  insert_dt           TIMESTAMP NOT NULL,
  update_dt           TIMESTAMP NOT NULL,
  source_system       VARCHAR(50) NOT NULL,
  source_entity       VARCHAR(50) NOT NULL,
  source_id           VARCHAR(200) UNIQUE NOT NULL
);