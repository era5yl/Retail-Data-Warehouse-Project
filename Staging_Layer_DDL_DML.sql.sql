
-- Task 1. Data Sourcing / Staging Area (SA)

-- 0) One-time prerequisites 
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- 1) Create a server for file_fdw 
DROP SERVER IF EXISTS sa_file_server CASCADE;
CREATE SERVER sa_file_server FOREIGN DATA WRAPPER file_fdw;

-- 2) Create SA schemas (one schema per dataset)

CREATE SCHEMA IF NOT EXISTS sa_adventureworks;
CREATE SCHEMA IF NOT EXISTS sa_contoso;

-- 3) External (foreign) tables EXT_*  (raw files)

--  AdventureWorks (denormalized) 
DROP FOREIGN TABLE IF EXISTS sa_adventureworks.ext_adventureworks_sales;

CREATE FOREIGN TABLE sa_adventureworks.ext_adventureworks_sales
(
    sales_order_number           text,
    sales_order_date             text,
    sales_order_date_day_of_week text,
    quantity                     text,
    unit_price                   text,
    total_sales                  text,
    cost                         text,
    product_key                  text,
    product_name                 text,
    reseller_key                 text,
    reseller_name                text,
    reseller_business_type       text,
    reseller_city                text,
    reseller_state               text,
    reseller_country             text,
    employee_id                  text,
    salesperson_fullname         text,
    salesperson_title            text,
    email_address                text,
    sales_territory_key          text,
    assigned_sales_territory     text,
    sales_territory_region       text,
    sales_territory_country      text,
    sales_territory_group        text,
    target                       text,
    target_date                  text,
    target_date_day_of_week      text
)
SERVER sa_file_server
OPTIONS (
    filename '/tmp/adventureworks.csv',
    format 'csv',
    header 'true',
    delimiter ',',
    quote '"',
    escape '"',
    null ''
);


-- Contoso
DROP FOREIGN TABLE IF EXISTS sa_contoso.ext_contoso_cb;

CREATE FOREIGN TABLE sa_contoso.ext_contoso_cb
(
    OnlineSalesKey                    text,
    SalesAmount                       text,
    SalesOrderNumber                  text,
    FullDateLabel                     text,
    CustomerLabel                     text,
    Quatter                           text,
    BirthDate                         text,
    Gender                            text,
    Education                         text,
    Occupation                        text,
    StatusStore                       text,
    EntityKey                         text,
    ParentEntityLabel                 text,
    EntityDescription                 text,
    EntityType                        text,
    StatusEntity                      text,
    GeographyType                     text,
    ContinentName                     text,
    CityName                          text,
    StateProvinceName                 text,
    RegionCountryName                 text,
    ProductDescription                text,
    Manufacturer                      text,
    BrandName                         text,
    ClassName                         text,
    StyleName                         text,
    Size                              text,
    Weight                            text,
    StockTypeName                     text,
    UnitCost                          text,
    UnitPrice                         text,
    StatusProduct                     text,
    ProductSubcategoryLabel           text,
    ProductSubcategoryDescription     text,
    ProductCategoryDescription        text,
    customer_name                     text,
    EuropeSeason                      text,
    NorthAmericaSeason                text,
    AsiaSeason                        text,
    YearlyIncome                      text,
    TotalChildren                     text,
    NumberChildrenAtHome              text,
    HouseOwnerFlag                    text,
    NumberCarsOwned                   text,
    DateFirstPurchase                 text
)
SERVER sa_file_server
OPTIONS (
  filename '/tmp/contoso_cb.csv',
  format 'csv',
  header 'true',
  delimiter ',',
  quote '"',
  escape '"',
  null '',
  encoding 'UTF8'
);




-- 4) Source tables SRC_* (typed, inside SA schemas)


-- ---------- AdventureWorks SRC ----------
DROP TABLE IF EXISTS sa_adventureworks.src_sales;

CREATE TABLE sa_adventureworks.src_sales
(
    sales_order_number           varchar(50) NOT NULL,
    sales_order_date             date NOT NULL,
    sales_order_date_day_of_week varchar(20),

    quantity                     integer,
    unit_price                   numeric(19,4),
    total_sales                  numeric(19,4),
    cost                         numeric(19,4),

    product_key                  integer,
    product_name                 varchar(255),

    reseller_key                 integer,
    reseller_name                varchar(255),
    reseller_business_type       varchar(100),
    reseller_city                varchar(100),
    reseller_state               varchar(100),
    reseller_country             varchar(100),

    employee_id                  varchar(50),
    salesperson_fullname         varchar(255),
    salesperson_title            varchar(100),
    email_address                varchar(255),

    sales_territory_key          integer,
    assigned_sales_territory     varchar(100),
    sales_territory_region       varchar(100),
    sales_territory_country      varchar(100),
    sales_territory_group        varchar(100),

    target                       numeric(19,4),
    target_date                  date,
    target_date_day_of_week      varchar(20),

    ta_insert_dt                 timestamp default now() NOT NULL
);
-- Deduplicated load from EXT -> SRC
INSERT INTO sa_adventureworks.src_sales
(
    sales_order_number,
    sales_order_date,
    sales_order_date_day_of_week,
    quantity,
    unit_price,
    total_sales,
    cost,
    product_key,
    product_name,
    reseller_key,
    reseller_name,
    reseller_business_type,
    reseller_city,
    reseller_state,
    reseller_country,
    employee_id,
    salesperson_fullname,
    salesperson_title,
    email_address,
    sales_territory_key,
    assigned_sales_territory,
    sales_territory_region,
    sales_territory_country,
    sales_territory_group,
    target,
    target_date,
    target_date_day_of_week
)
SELECT DISTINCT
    trim(e.sales_order_number),
    to_date(e.sales_order_date, 'YYYY-MM-DD'),
    nullif(trim(e.sales_order_date_day_of_week), ''),

    nullif(e.quantity, '')::int,
    NULLIF(regexp_replace(e.unit_price,  '[$,]', '', 'g'), '')::numeric(19,4),
    NULLIF(regexp_replace(e.total_sales, '[$,]', '', 'g'), '')::numeric(19,4),
    NULLIF(regexp_replace(e.cost,        '[$,]', '', 'g'), '')::numeric(19,4),

    nullif(e.product_key, '')::int,
    nullif(trim(e.product_name), ''),

    nullif(e.reseller_key, '')::int,
    nullif(trim(e.reseller_name), ''),
    nullif(trim(e.reseller_business_type), ''),
    nullif(trim(e.reseller_city), ''),
    nullif(trim(e.reseller_state), ''),
    nullif(trim(e.reseller_country), ''),

    nullif(trim(e.employee_id), ''),
    nullif(trim(e.salesperson_fullname), ''),
    nullif(trim(e.salesperson_title), ''),
    nullif(trim(e.email_address), ''),

    nullif(e.sales_territory_key, '')::int,
    nullif(trim(e.assigned_sales_territory), ''),
    nullif(trim(e.sales_territory_region), ''),
    nullif(trim(e.sales_territory_country), ''),
    nullif(trim(e.sales_territory_group), ''),

    NULLIF(regexp_replace(e.target, '[$,]', '', 'g'), '')::numeric(19,4),
    CASE WHEN nullif(e.target_date,'') IS NULL THEN NULL ELSE to_date(e.target_date,'YYYY-MM-DD') END,
    nullif(trim(e.target_date_day_of_week), '')

FROM sa_adventureworks.ext_adventureworks_sales e;

-- ---------- Contoso SRC (CB only with dedup on OnlineSalesKey) ----------
DROP TABLE IF EXISTS sa_contoso.src_online_sales;

CREATE TABLE sa_contoso.src_online_sales
(
    online_sales_key                   bigint NOT NULL,
    sales_amount                       numeric(19,4),
    sales_order_number                 varchar(50),
    full_date                          date,
    customer_label                     varchar(100),
    quarter_label                      varchar(20),

    birth_date                         date,
    gender                             varchar(20),
    education                          varchar(50),
    occupation                         varchar(50),

    status_store                       varchar(50),
    entity_key                         bigint,
    parent_entity_label                varchar(200),
    entity_description                 varchar(200),
    entity_type                        varchar(50),
    status_entity                      varchar(50),

    geography_type                     varchar(50),
    continent_name                     varchar(50),
    city_name                          varchar(100),
    state_province_name                varchar(100),
    region_country_name                varchar(100),

    product_description                varchar(255),
    manufacturer                       varchar(100),
    brand_name                         varchar(100),
    class_name                         varchar(100),
    style_name                         varchar(100),
    size_txt                           varchar(50),
    weight_num                         numeric(18,4),
    stock_type_name                    varchar(50),
    unit_cost                          numeric(19,4),
    unit_price                         numeric(19,4),
    status_product                     varchar(50),

    product_subcategory_label          varchar(100),
    product_subcategory_description    varchar(255),
    product_category_description       varchar(255),

    customer_name                      varchar(255),

    europe_season                      varchar(50),
    north_america_season               varchar(50),
    asia_season                        varchar(50),

    yearly_income                      numeric(19,4),
    total_children                     smallint,
    number_children_at_home            smallint,
    house_owner_flag                   varchar(10),
    number_cars_owned                  smallint,
    date_first_purchase                date,

    source_entity                      varchar(20) NOT NULL,
    ta_insert_dt                       timestamp default now() NOT NULL
);

-- Deduplicated load from ext to src
WITH typed AS (
    SELECT
        NULLIF(OnlineSalesKey,'')::bigint                 AS online_sales_key,
        NULLIF(SalesAmount,'')::numeric(19,4)             AS sales_amount,
        NULLIF(SalesOrderNumber,'')                       AS sales_order_number,
        CASE WHEN NULLIF(FullDateLabel,'') IS NULL THEN NULL ELSE to_date(FullDateLabel,'YYYY-MM-DD') END AS full_date,
        NULLIF(CustomerLabel,'')                          AS customer_label,
        NULLIF(Quatter,'')                                AS quarter_label,
        CASE WHEN NULLIF(BirthDate,'') IS NULL THEN NULL ELSE to_date(BirthDate,'YYYY-MM-DD') END AS birth_date,
        NULLIF(Gender,'')                                 AS gender,
        NULLIF(Education,'')                              AS education,
        NULLIF(Occupation,'')                             AS occupation,
        NULLIF(StatusStore,'')                            AS status_store,
        NULLIF(EntityKey,'')::bigint                      AS entity_key,
        NULLIF(ParentEntityLabel,'')                      AS parent_entity_label,
        NULLIF(EntityDescription,'')                      AS entity_description,
        NULLIF(EntityType,'')                             AS entity_type,
        NULLIF(StatusEntity,'')                           AS status_entity,
        NULLIF(GeographyType,'')                          AS geography_type,
        NULLIF(ContinentName,'')                          AS continent_name,
        NULLIF(CityName,'')                               AS city_name,
        NULLIF(StateProvinceName,'')                      AS state_province_name,
        NULLIF(RegionCountryName,'')                      AS region_country_name,
        NULLIF(ProductDescription,'')                     AS product_description,
        NULLIF(Manufacturer,'')                           AS manufacturer,
        NULLIF(BrandName,'')                              AS brand_name,
        NULLIF(ClassName,'')                              AS class_name,
        NULLIF(StyleName,'')                              AS style_name,
        NULLIF(Size,'')                                   AS size_txt,
        NULLIF(Weight,'')::numeric(18,4)                  AS weight_num,
        NULLIF(StockTypeName,'')                          AS stock_type_name,
        NULLIF(UnitCost,'')::numeric(19,4)                AS unit_cost,
        NULLIF(UnitPrice,'')::numeric(19,4)               AS unit_price,
        NULLIF(StatusProduct,'')                          AS status_product,
        NULLIF(ProductSubcategoryLabel,'')                AS product_subcategory_label,
        NULLIF(ProductSubcategoryDescription,'')          AS product_subcategory_description,
        NULLIF(ProductCategoryDescription,'')             AS product_category_description,

        NULLIF(customer_name,'')                          AS customer_name,

        NULLIF(EuropeSeason,'')                           AS europe_season,
        NULLIF(NorthAmericaSeason,'')                     AS north_america_season,
        NULLIF(AsiaSeason,'')                             AS asia_season,
        NULLIF(YearlyIncome,'')::numeric(19,4)            AS yearly_income,
        NULLIF(TotalChildren,'')::smallint                AS total_children,
        NULLIF(NumberChildrenAtHome,'')::smallint         AS number_children_at_home,
        NULLIF(HouseOwnerFlag,'')                         AS house_owner_flag,
        NULLIF(NumberCarsOwned,'')::smallint              AS number_cars_owned,
        CASE WHEN NULLIF(DateFirstPurchase,'') IS NULL THEN NULL ELSE to_date(DateFirstPurchase,'YYYY-MM-DD') END AS date_first_purchase,

        'contoso_cb'                                              AS source_entity
    FROM sa_contoso.ext_contoso_cb
    WHERE NULLIF(OnlineSalesKey,'') IS NOT NULL
),
dedup AS (
    SELECT *
    FROM (
        SELECT
            t.*,
            row_number() OVER (PARTITION BY online_sales_key ORDER BY online_sales_key) AS rn
        FROM typed t
    ) x
    WHERE rn = 1
)
INSERT INTO sa_contoso.src_online_sales
(
    online_sales_key, sales_amount, sales_order_number, full_date,
    customer_label, quarter_label,
    birth_date, gender, education, occupation,
    status_store, entity_key, parent_entity_label, entity_description, entity_type, status_entity,
    geography_type, continent_name, city_name, state_province_name, region_country_name,
    product_description, manufacturer, brand_name, class_name, style_name, size_txt, weight_num,
    stock_type_name, unit_cost, unit_price, status_product,
    product_subcategory_label, product_subcategory_description, product_category_description,
    customer_name, europe_season, north_america_season, asia_season,
    yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned, date_first_purchase,
    source_entity
)
SELECT
    online_sales_key, sales_amount, sales_order_number, full_date,
    customer_label, quarter_label,
    birth_date, gender, education, occupation,
    status_store, entity_key, parent_entity_label, entity_description, entity_type, status_entity,
    geography_type, continent_name, city_name, state_province_name, region_country_name,
    product_description, manufacturer, brand_name, class_name, style_name, size_txt, weight_num,
    stock_type_name, unit_cost, unit_price, status_product,
    product_subcategory_label, product_subcategory_description, product_category_description,
    customer_name, europe_season, north_america_season, asia_season,
    yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned, date_first_purchase,
    source_entity
FROM dedup;


-- 5) SELECTs for screenshots (EXT_* and SRC_*)


-- EXT tables (raw)
SELECT * FROM sa_adventureworks.ext_adventureworks_sales LIMIT 10;
SELECT * FROM sa_contoso.ext_contoso_cb LIMIT 10;

-- SRC tables (typed + deduped)
SELECT * FROM sa_adventureworks.src_sales ORDER BY sales_order_date DESC LIMIT 10;
SELECT * FROM sa_contoso.src_online_sales ORDER BY full_date DESC NULLS LAST LIMIT 10;

-- Quick duplicate checks
-- AdventureWorks: check exact duplicates by chosen key
SELECT
  sales_order_number, sales_order_date, product_key, reseller_key, employee_id,
  COUNT(*) AS cnt
FROM sa_adventureworks.src_sales
GROUP BY 1,2,3,4,5
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- Contoso: OnlineSalesKey must be unique after dedup
SELECT online_sales_key, COUNT(*) AS cnt
FROM sa_contoso.src_online_sales
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
