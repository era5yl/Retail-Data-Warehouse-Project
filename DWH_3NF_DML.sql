-- =========================================================
-- DML: Load BL_3NF (rerunnable, reusable, no duplicates)
-- Includes:
--  - default rows in every table EXCEPT fact tables (CE_SALES, CE_SALES_TARGETS)
--  - COALESCE null handling
--  - LEFT JOIN for FK resolution
--  - NOT EXISTS for rerunnable loads
--  - SCD2 for CE_CUSTOMERS_SCD
--  - COMMIT after DML blocks
--
-- Assumed sources:
--   sa_adventureworks.src_sales
--   sa_contoso.src_online_sales
-- =========================================================

-- =========================================================
-- 0) DEFAULT ROWS (NO defaults in CE_SALES and CE_SALES_TARGETS)
-- =========================================================

-- CE_SEASONS
INSERT INTO bl_3nf.ce_seasons
(
    season_surr_id,
    europe_season,
    north_america_season,
    asia_season,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_seasons'),
    COALESCE(TRIM(europe_season),'n.a.'),
    COALESCE(TRIM(north_america_season),'n.a.'),
    COALESCE(TRIM(asia_season),'n.a.'),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    COALESCE(TRIM(europe_season),'n.a.')||'|'||
    COALESCE(TRIM(north_america_season),'n.a.')||'|'||
    COALESCE(TRIM(asia_season),'n.a.')
FROM sa_contoso.src_online_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_seasons t
    WHERE t.europe_season = COALESCE(TRIM(s.europe_season),'n.a.')
      AND t.north_america_season = COALESCE(TRIM(s.north_america_season),'n.a.')
      AND t.asia_season = COALESCE(TRIM(s.asia_season),'n.a.')
);


-- CE_DATES default (-1)
INSERT INTO bl_3nf.ce_dates
(
    date_surr_id,
    full_date_label,
    quarter,
    season_surr_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_dates'),
    s.full_date,
    'Q'||EXTRACT(QUARTER FROM s.full_date),
    COALESCE(se.season_surr_id, -1),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    s.full_date::varchar
FROM sa_contoso.src_online_sales s
LEFT JOIN bl_3nf.ce_seasons se
    ON se.europe_season = COALESCE(TRIM(s.europe_season),'n.a.')
   AND se.north_america_season = COALESCE(TRIM(s.north_america_season),'n.a.')
   AND se.asia_season = COALESCE(TRIM(s.asia_season),'n.a.')
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_dates d
    WHERE d.full_date_label = s.full_date
);


-- CE_GEOGRAPHY
INSERT INTO bl_3nf.ce_geography
(
    geography_surr_id,
    city,
    state,
    country,
    geography_type,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_geography'),
    COALESCE(TRIM(city_name),'n.a.'),
    COALESCE(TRIM(state_province_name),'n.a.'),
    COALESCE(TRIM(region_country_name),'n.a.'),
    COALESCE(TRIM(geography_type),'n.a.'),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    COALESCE(TRIM(region_country_name),'n.a.')||'|'||
    COALESCE(TRIM(state_province_name),'n.a.')||'|'||
    COALESCE(TRIM(city_name),'n.a.')
FROM sa_contoso.src_online_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_geography g
    WHERE g.city = COALESCE(TRIM(s.city_name),'n.a.')
      AND g.state = COALESCE(TRIM(s.state_province_name),'n.a.')
      AND g.country = COALESCE(TRIM(s.region_country_name),'n.a.')
);

-- CE_PRODUCT_CATEGORY
INSERT INTO bl_3nf.ce_product_categories
(
    product_category_surr_id,
    product_category_description,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_product_categories'),
    COALESCE(TRIM(product_category_description),'n.a.'),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    COALESCE(TRIM(product_category_description),'n.a.')
FROM sa_contoso.src_online_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_categories t
    WHERE t.product_category_description =
          COALESCE(TRIM(s.product_category_description),'n.a.')
);

-- CE_PRODUCT_SUBCATEGORY
INSERT INTO bl_3nf.ce_product_subcategories
(
    product_subcategory_surr_id,
    product_subcategory_label,
    product_subcategory_description,
    product_category_surr_id,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_product_subcategories'),
    COALESCE(TRIM(product_subcategory_label),'n.a.'),
    COALESCE(TRIM(product_subcategory_description),'n.a.'),
    COALESCE(cat.product_category_surr_id, -1),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    COALESCE(TRIM(product_subcategory_label),'n.a.')
FROM sa_contoso.src_online_sales s
LEFT JOIN bl_3nf.ce_product_categories cat
  ON cat.product_category_description =
     COALESCE(TRIM(s.product_category_description),'n.a.')
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_subcategories t
    WHERE t.product_subcategory_label =
          COALESCE(TRIM(s.product_subcategory_label),'n.a.')
);

-- CE_PRODUCTS
INSERT INTO bl_3nf.ce_products
(
    product_surr_id,
    product_description,
    manufacturer,
    brand_name,
    class_name,
    style_name,
    product_subcategory_surr_id,
    size,
    weight,
    stock_type,
    status_product,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_products'),
    COALESCE(TRIM(product_description),'n.a.'),
    COALESCE(TRIM(manufacturer),'n.a.'),
    COALESCE(TRIM(brand_name),'n.a.'),
    COALESCE(TRIM(class_name),'n.a.'),
    COALESCE(TRIM(style_name),'n.a.'),
    COALESCE(sub.product_subcategory_surr_id, -1),
    COALESCE(TRIM(size_txt),'n.a.'),
    weight_num,
    COALESCE(TRIM(stock_type_name),'n.a.'),
    COALESCE(TRIM(status_product),'n.a.'),
    NOW(), NOW(),
    'SA_CONTOSO',
    'SRC_ONLINE_SALES',
    product_key::varchar
FROM sa_contoso.src_online_sales s
LEFT JOIN bl_3nf.ce_product_subcategories sub
  ON sub.product_subcategory_label =
     COALESCE(TRIM(s.product_subcategory_label),'n.a.')
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_products p
    WHERE p.source_id = s.product_key::varchar
);


-- CE_EMPLOYEES
IINSERT INTO bl_3nf.ce_employees
(
    employee_surr_id,
    employee_business_id,
    name,
    title,
    email,
    insert_dt,
    update_dt,
    source_system,
    source_entity,
    source_id
)
SELECT DISTINCT
    nextval('bl_3nf.seq_ce_employees'),
    employee_id,
    salesperson_fullname,
    salesperson_title,
    email_address,
    NOW(), NOW(),
    'SA_ADVENTUREWORKS',
    'SRC_SALES',
    employee_id::varchar
FROM sa_adventureworks.src_sales s
WHERE employee_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_employees e
    WHERE e.employee_business_id = s.employee_id
);

-- CE_STORES default (-1)
INSERT INTO bl_3nf.ce_stores
(store_id, store_nk, entity_key, parent_entity_label, entity_description, entity_type, status_store, status_entity, geography_id,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    -1, 'N/A', NULL, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', -1,
    'MANUAL', 'MANUAL', '-1',
    now(), now()
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id = -1);

-- CE_CUSTOMERS_SCD default (-1, 1900-01-01)  (IMPORTANT for composite FK usage)
INSERT INTO bl_3nf.ce_customers_scd
(customer_id, customer_nk, customer_label, customer_name, birth_dt, gender, education, occupation, yearly_income,
 total_children, children_at_home, house_owner_flg, cars_owned, date_first_purchase, geography_id,
 start_dt, end_dt, is_active,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    -1, 'N/A', 'N/A', 'n.a.', NULL, 'n.a.', 'n.a.', 'n.a.', NULL,
    NULL, NULL, 'n.a.', NULL, NULL, -1,
    DATE '1900-01-01', DATE '9999-12-31', 'Y',
    'MANUAL', 'MANUAL', '-1',
    now(), now()
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_customers_scd
    WHERE customer_id = -1 AND start_dt = DATE '1900-01-01'
);

COMMIT;

-- =========================================================
-- 1) CE_COUNTRIES (from AW territory + Contoso geography)
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        COALESCE(TRIM(s.sales_territory_country), 'n.a.') AS country_name,
        COALESCE(TRIM(s.sales_territory_group),   'n.a.') AS continent_name, -- territory_group == continent
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar        AS source_entity,
        COALESCE(TRIM(s.sales_territory_country), 'n.a.') AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.sales_territory_country IS NOT NULL OR s.sales_territory_group IS NOT NULL

    UNION

    SELECT DISTINCT
        COALESCE(TRIM(c.region_country_name), 'n.a.') AS country_name,
        COALESCE(TRIM(c.continent_name),      'n.a.') AS continent_name,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        COALESCE(TRIM(c.region_country_name), 'n.a.') AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.region_country_name IS NOT NULL OR c.continent_name IS NOT NULL
)
INSERT INTO bl_3nf.ce_countries
(country_id, country_nk, country_name, continent_name, source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_countries'),
    UPPER(src.country_name) AS country_nk,
    src.country_name,
    src.continent_name,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_countries t
    WHERE t.country_nk = UPPER(src.country_name)
);

COMMIT;

-- =========================================================
-- 2) CE_GEOGRAPHIES (Contoso geo + AW reseller geo), FK -> CE_COUNTRIES
-- =========================================================
WITH contoso_geo AS (
    SELECT DISTINCT
        COALESCE(TRIM(c.region_country_name), 'n.a.') AS country_name,
        COALESCE(TRIM(c.state_province_name), 'n.a.') AS state_province_name,
        COALESCE(TRIM(c.city_name),           'n.a.') AS city_name,
        COALESCE(TRIM(c.geography_type),      'n.a.') AS geography_type_name,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        COALESCE(TRIM(c.region_country_name), 'n.a.') || '|' ||
        COALESCE(TRIM(c.state_province_name), 'n.a.') || '|' ||
        COALESCE(TRIM(c.city_name),           'n.a.') || '|' ||
        COALESCE(TRIM(c.geography_type),      'n.a.') AS source_id
    FROM sa_contoso.src_online_sales c
),
aw_geo AS (
    SELECT DISTINCT
        COALESCE(TRIM(s.reseller_country), 'n.a.') AS country_name,
        COALESCE(TRIM(s.reseller_state),   'n.a.') AS state_province_name,
        COALESCE(TRIM(s.reseller_city),    'n.a.') AS city_name,
        'City'::varchar AS geography_type_name,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        COALESCE(TRIM(s.reseller_country), 'n.a.') || '|' ||
        COALESCE(TRIM(s.reseller_state),   'n.a.') || '|' ||
        COALESCE(TRIM(s.reseller_city),    'n.a.') || '|City' AS source_id
    FROM sa_adventureworks.src_sales s
)
, all_geo AS (
    SELECT * FROM contoso_geo
    UNION
    SELECT * FROM aw_geo
)
INSERT INTO bl_3nf.ce_geographies
(geography_id, geography_nk, country_id, state_province_name, city_name, geography_type_name,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_geographies'),
    UPPER(all_geo.country_name) || '|' ||
    UPPER(all_geo.state_province_name) || '|' ||
    UPPER(all_geo.city_name) || '|' ||
    UPPER(all_geo.geography_type_name) AS geography_nk,
    COALESCE(ct.country_id, -1) AS country_id,
    all_geo.state_province_name,
    all_geo.city_name,
    all_geo.geography_type_name,
    all_geo.source_system,
    all_geo.source_entity,
    all_geo.source_id,
    now(), now()
FROM all_geo
LEFT JOIN bl_3nf.ce_countries ct
    ON ct.country_nk = UPPER(all_geo.country_name)
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_geographies g
    WHERE g.geography_nk =
        UPPER(all_geo.country_name) || '|' ||
        UPPER(all_geo.state_province_name) || '|' ||
        UPPER(all_geo.city_name) || '|' ||
        UPPER(all_geo.geography_type_name)
);

COMMIT;

-- =========================================================
-- 3) CE_SALES_TERRITORIES (AdventureWorks), FK -> CE_COUNTRIES
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        COALESCE(TRIM(CAST(s.sales_territory_key AS varchar)), 'n.a.') AS sales_territory_key,
        COALESCE(TRIM(s.assigned_sales_territory), 'n.a.') AS assigned_sales_territory,
        COALESCE(TRIM(s.sales_territory_region),   'n.a.') AS region,
        COALESCE(TRIM(s.sales_territory_country),  'n.a.') AS country_name,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        COALESCE(TRIM(CAST(s.sales_territory_key AS varchar)), 'n.a.') AS source_id
    FROM sa_adventureworks.src_sales s
)
INSERT INTO bl_3nf.ce_sales_territories
(territory_id, territory_nk, sales_territory_key, assigned_sales_territory, region, country_id,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_sales_territories'),
    'AW|' || src.sales_territory_key AS territory_nk,
    src.sales_territory_key,
    src.assigned_sales_territory,
    src.region,
    COALESCE(cn.country_id, -1) AS country_id,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
LEFT JOIN bl_3nf.ce_countries cn
    ON cn.country_nk = UPPER(src.country_name)
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_sales_territories t
    WHERE t.territory_nk = 'AW|' || src.sales_territory_key
);

COMMIT;

-- =========================================================
-- 4) CE_DATES (from AW sales dates + AW target dates + Contoso full_date)
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        s.sales_order_date::date AS date_dt,
        COALESCE(TRIM(s.sales_order_date_day_of_week), TRIM(TO_CHAR(s.sales_order_date::date, 'Day'))) AS dow,
        COALESCE(TRIM(s.sales_order_date_month),       TRIM(TO_CHAR(s.sales_order_date::date, 'Month'))) AS mon,
        EXTRACT(YEAR FROM s.sales_order_date::date)::smallint AS yr,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        s.sales_order_date::varchar AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.sales_order_date IS NOT NULL

    UNION

    SELECT DISTINCT
        s.target_date::date AS date_dt,
        COALESCE(TRIM(s.target_date_day_of_week), TRIM(TO_CHAR(s.target_date::date, 'Day'))) AS dow,
        COALESCE(TRIM(s.target_date_month),       TRIM(TO_CHAR(s.target_date::date, 'Month'))) AS mon,
        EXTRACT(YEAR FROM s.target_date::date)::smallint AS yr,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        s.target_date::varchar AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.target_date IS NOT NULL

    UNION

    SELECT DISTINCT
        c.full_date::date AS date_dt,
        TRIM(TO_CHAR(c.full_date::date, 'Day')) AS dow,
        COALESCE(TRIM(c.calendar_month_label), TRIM(TO_CHAR(c.full_date::date, 'Month'))) AS mon,
        COALESCE(c.calendar_year, EXTRACT(YEAR FROM c.full_date::date))::smallint AS yr,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        c.full_date::varchar AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.full_date IS NOT NULL
)
INSERT INTO bl_3nf.ce_dates
(date_id, date_dt, day_of_week_name, month_name, year_no,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_dates'),
    src.date_dt,
    src.dow,
    src.mon,
    src.yr,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_dates d
    WHERE d.date_dt = src.date_dt
);

COMMIT;

-- =========================================================
-- 5) CE_PRODUCTS (AW + Contoso)
-- =========================================================
WITH aw AS (
    SELECT DISTINCT
        ('AW|' || COALESCE(CAST(s.product_key AS varchar), 'n.a.')) AS product_nk,
        s.product_key,
        COALESCE(TRIM(s.product_name), 'n.a.') AS product_name,
        NULL::varchar AS product_description,
        NULL::varchar AS manufacturer,
        NULL::varchar AS brand_name,
        NULL::varchar AS class_name,
        NULL::varchar AS style_name,
        NULL::varchar AS size_txt,
        NULL::numeric AS weight_num,
        NULL::numeric AS unit_cost,
        NULL::numeric AS unit_price,
        NULL::varchar AS status_product,
        NULL::varchar AS product_subcategory_label,
        NULL::varchar AS product_subcategory_description,
        NULL::varchar AS product_category_description,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        ('PRODUCT_KEY=' || COALESCE(CAST(s.product_key AS varchar), 'n.a.')) AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.product_key IS NOT NULL OR s.product_name IS NOT NULL
),
ct AS (
    SELECT DISTINCT
        ('CT|' || COALESCE(TRIM(c.product_description), 'n.a.')) AS product_nk,
        NULL::int AS product_key,
        NULL::varchar AS product_name,
        COALESCE(TRIM(c.product_description), 'n.a.') AS product_description,
        COALESCE(TRIM(c.manufacturer), 'n.a.') AS manufacturer,
        COALESCE(TRIM(c.brand_name), 'n.a.') AS brand_name,
        COALESCE(TRIM(c.class_name), 'n.a.') AS class_name,
        COALESCE(TRIM(c.style_name), 'n.a.') AS style_name,
        COALESCE(TRIM(c.size_txt), 'n.a.') AS size_txt,
        c.weight_num,
        c.unit_cost,
        c.unit_price,
        COALESCE(TRIM(c.status_product), 'n.a.') AS status_product,
        COALESCE(TRIM(c.product_subcategory_label), 'n.a.') AS product_subcategory_label,
        COALESCE(TRIM(c.product_subcategory_description), 'n.a.') AS product_subcategory_description,
        COALESCE(TRIM(c.product_category_description), 'n.a.') AS product_category_description,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        ('PRODUCT_DESC=' || COALESCE(TRIM(c.product_description), 'n.a.')) AS source_id
    FROM sa_contoso.src_online_sales c
),
src AS (
    SELECT * FROM aw
    UNION
    SELECT * FROM ct
)
INSERT INTO bl_3nf.ce_products
(product_id, product_nk, product_key, product_name, product_description, manufacturer, brand_name, class_name, style_name, size_txt,
 weight_num, unit_cost, unit_price, status_product, product_subcategory_label, product_subcategory_description, product_category_description,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_products'),
    src.product_nk,
    src.product_key,
    src.product_name,
    src.product_description,
    src.manufacturer,
    src.brand_name,
    src.class_name,
    src.style_name,
    src.size_txt,
    src.weight_num,
    src.unit_cost,
    src.unit_price,
    src.status_product,
    src.product_subcategory_label,
    src.product_subcategory_description,
    src.product_category_description,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_products p
    WHERE p.product_nk = src.product_nk
);

COMMIT;

-- =========================================================
-- 6) CE_EMPLOYEES (AdventureWorks)
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        ('AW|' || COALESCE(CAST(s.employee_key AS varchar), COALESCE(TRIM(s.employee_id),'n.a.'))) AS employee_nk,
        s.employee_key,
        s.employee_id AS employee_business_id,
        COALESCE(TRIM(s.salesperson_fullname), 'n.a.') AS employee_fullname,
        COALESCE(TRIM(s.salesperson_title), 'n.a.') AS employee_title,
        COALESCE(TRIM(s.email_address), 'n.a.') AS email_address,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        ('EMPLOYEE_KEY=' || COALESCE(CAST(s.employee_key AS varchar), 'n.a.')) AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.employee_key IS NOT NULL OR s.employee_id IS NOT NULL
)
INSERT INTO bl_3nf.ce_employees
(employee_id, employee_nk, employee_key, employee_business_id, employee_fullname, employee_title, email_address,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_employees'),
    src.employee_nk,
    src.employee_key,
    src.employee_business_id,
    src.employee_fullname,
    src.employee_title,
    src.email_address,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_employees e
    WHERE e.employee_nk = src.employee_nk
);

COMMIT;

-- =========================================================
-- 7) CE_RESELLERS (AdventureWorks), FK -> CE_GEOGRAPHIES
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        ('AW|' || COALESCE(CAST(s.reseller_key AS varchar),'n.a.')) AS reseller_nk,
        s.reseller_key,
        COALESCE(TRIM(s.reseller_name), 'n.a.') AS reseller_name,
        COALESCE(TRIM(s.reseller_business_type), 'n.a.') AS reseller_business_type,
        UPPER(COALESCE(TRIM(s.reseller_country), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(s.reseller_state),   'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(s.reseller_city),    'n.a.')) || '|CITY' AS geography_nk,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        ('RESELLER_KEY=' || COALESCE(CAST(s.reseller_key AS varchar),'n.a.')) AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.reseller_key IS NOT NULL OR s.reseller_name IS NOT NULL
)
INSERT INTO bl_3nf.ce_resellers
(reseller_id, reseller_nk, reseller_key, reseller_name, reseller_business_type, geography_id,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_resellers'),
    src.reseller_nk,
    src.reseller_key,
    src.reseller_name,
    src.reseller_business_type,
    COALESCE(g.geography_id, -1) AS geography_id,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
LEFT JOIN bl_3nf.ce_geographies g
    ON g.geography_nk = src.geography_nk
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_resellers r
    WHERE r.reseller_nk = src.reseller_nk
);

COMMIT;

-- =========================================================
-- 8) CE_STORES (Contoso), FK -> CE_GEOGRAPHIES
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        ('CT|' || COALESCE(CAST(c.entity_key AS varchar),'n.a.')) AS store_nk,
        c.entity_key,
        COALESCE(TRIM(c.parent_entity_label), 'n.a.') AS parent_entity_label,
        COALESCE(TRIM(c.entity_description),  'n.a.') AS entity_description,
        COALESCE(TRIM(c.entity_type),         'n.a.') AS entity_type,
        COALESCE(TRIM(c.status_store),        'n.a.') AS status_store,
        COALESCE(TRIM(c.status_entity),       'n.a.') AS status_entity,
        UPPER(COALESCE(TRIM(c.region_country_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.state_province_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.city_name),           'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.geography_type),      'n.a.')) AS geography_nk,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        ('ENTITY_KEY=' || COALESCE(CAST(c.entity_key AS varchar),'n.a.')) AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.entity_key IS NOT NULL
)
INSERT INTO bl_3nf.ce_stores
(store_id, store_nk, entity_key, parent_entity_label, entity_description, entity_type, status_store, status_entity, geography_id,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_stores'),
    src.store_nk,
    src.entity_key,
    src.parent_entity_label,
    src.entity_description,
    src.entity_type,
    src.status_store,
    src.status_entity,
    COALESCE(g.geography_id, -1) AS geography_id,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
LEFT JOIN bl_3nf.ce_geographies g
    ON g.geography_nk = src.geography_nk
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_stores st
    WHERE st.store_nk = src.store_nk
);

COMMIT;

-- =========================================================
-- 9) CE_CUSTOMERS_SCD (SCD2) - Contoso
-- A) Insert new customers (no active)
-- B) Close active if changed
-- C) Insert new version after close
-- =========================================================

-- A) Insert brand-new customers
WITH snap AS (
    SELECT DISTINCT
        COALESCE(TRIM(c.customer_label), 'n.a.') AS customer_nk,
        COALESCE(TRIM(c.customer_label), 'n.a.') AS customer_label,
        COALESCE(TRIM(c.customer_name),  'n.a.') AS customer_name,
        c.birth_date::date AS birth_dt,
        COALESCE(TRIM(c.gender),     'n.a.') AS gender,
        COALESCE(TRIM(c.education),  'n.a.') AS education,
        COALESCE(TRIM(c.occupation), 'n.a.') AS occupation,
        c.yearly_income,
        c.total_children,
        c.number_children_at_home AS children_at_home,
        COALESCE(TRIM(c.house_owner_flag), 'n.a.') AS house_owner_flg,
        c.number_cars_owned AS cars_owned,
        c.date_first_purchase::date AS date_first_purchase,
        UPPER(COALESCE(TRIM(c.region_country_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.state_province_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.city_name),           'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.geography_type),      'n.a.')) AS geography_nk,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        ('CUSTOMER_LABEL=' || COALESCE(TRIM(c.customer_label),'n.a.')) AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.customer_label IS NOT NULL
)
INSERT INTO bl_3nf.ce_customers_scd
(customer_id, customer_nk, customer_label, customer_name, birth_dt, gender, education, occupation, yearly_income,
 total_children, children_at_home, house_owner_flg, cars_owned, date_first_purchase, geography_id,
 start_dt, end_dt, is_active,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_customers'),
    s.customer_nk,
    s.customer_label,
    s.customer_name,
    s.birth_dt,
    s.gender,
    s.education,
    s.occupation,
    s.yearly_income,
    s.total_children,
    s.children_at_home,
    s.house_owner_flg,
    s.cars_owned,
    s.date_first_purchase,
    COALESCE(g.geography_id, -1) AS geography_id,
    current_date,
    DATE '9999-12-31',
    'Y',
    s.source_system,
    s.source_entity,
    s.source_id,
    now(), now()
FROM snap s
LEFT JOIN bl_3nf.ce_geographies g
    ON g.geography_nk = s.geography_nk
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_customers_scd c
    WHERE c.customer_nk = s.customer_nk
      AND c.is_active = 'Y'
);

COMMIT;

-- B) Close active rows that changed
WITH snap AS (
    SELECT DISTINCT
        COALESCE(TRIM(c.customer_label), 'n.a.') AS customer_nk,
        COALESCE(TRIM(c.customer_name),  'n.a.') AS customer_name,
        c.birth_date::date AS birth_dt,
        COALESCE(TRIM(c.gender),     'n.a.') AS gender,
        COALESCE(TRIM(c.education),  'n.a.') AS education,
        COALESCE(TRIM(c.occupation), 'n.a.') AS occupation,
        c.yearly_income,
        c.total_children,
        c.number_children_at_home AS children_at_home,
        COALESCE(TRIM(c.house_owner_flag), 'n.a.') AS house_owner_flg,
        c.number_cars_owned AS cars_owned,
        c.date_first_purchase::date AS date_first_purchase,
        UPPER(COALESCE(TRIM(c.region_country_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.state_province_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.city_name),           'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.geography_type),      'n.a.')) AS geography_nk
    FROM sa_contoso.src_online_sales c
    WHERE c.customer_label IS NOT NULL
),
snap_with_geo AS (
    SELECT s.*, COALESCE(g.geography_id, -1) AS geography_id
    FROM snap s
    LEFT JOIN bl_3nf.ce_geographies g
        ON g.geography_nk = s.geography_nk
),
changed AS (
    SELECT cur.customer_id, cur.start_dt
    FROM bl_3nf.ce_customers_scd cur
    JOIN snap_with_geo s
      ON s.customer_nk = cur.customer_nk
    WHERE cur.is_active = 'Y'
      AND (
            COALESCE(cur.customer_name,'') <> COALESCE(s.customer_name,'')
         OR COALESCE(cur.birth_dt, DATE '1900-01-01') <> COALESCE(s.birth_dt, DATE '1900-01-01')
         OR COALESCE(cur.gender,'') <> COALESCE(s.gender,'')
         OR COALESCE(cur.education,'') <> COALESCE(s.education,'')
         OR COALESCE(cur.occupation,'') <> COALESCE(s.occupation,'')
         OR COALESCE(cur.yearly_income, 0) <> COALESCE(s.yearly_income, 0)
         OR COALESCE(cur.total_children, -1) <> COALESCE(s.total_children, -1)
         OR COALESCE(cur.children_at_home, -1) <> COALESCE(s.children_at_home, -1)
         OR COALESCE(cur.house_owner_flg,'') <> COALESCE(s.house_owner_flg,'')
         OR COALESCE(cur.cars_owned, -1) <> COALESCE(s.cars_owned, -1)
         OR COALESCE(cur.date_first_purchase, DATE '1900-01-01') <> COALESCE(s.date_first_purchase, DATE '1900-01-01')
         OR COALESCE(cur.geography_id, -1) <> COALESCE(s.geography_id, -1)
      )
)
UPDATE bl_3nf.ce_customers_scd cur
SET
    end_dt    = current_date - 1,
    is_active = 'N',
    update_dt = now()
FROM changed ch
WHERE cur.customer_id = ch.customer_id
  AND cur.start_dt    = ch.start_dt;

COMMIT;

-- C) Insert new version rows for changed customers
WITH snap AS (
    SELECT DISTINCT
        COALESCE(TRIM(c.customer_label), 'n.a.') AS customer_nk,
        COALESCE(TRIM(c.customer_label), 'n.a.') AS customer_label,
        COALESCE(TRIM(c.customer_name),  'n.a.') AS customer_name,
        c.birth_date::date AS birth_dt,
        COALESCE(TRIM(c.gender),     'n.a.') AS gender,
        COALESCE(TRIM(c.education),  'n.a.') AS education,
        COALESCE(TRIM(c.occupation), 'n.a.') AS occupation,
        c.yearly_income,
        c.total_children,
        c.number_children_at_home AS children_at_home,
        COALESCE(TRIM(c.house_owner_flag), 'n.a.') AS house_owner_flg,
        c.number_cars_owned AS cars_owned,
        c.date_first_purchase::date AS date_first_purchase,
        UPPER(COALESCE(TRIM(c.region_country_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.state_province_name), 'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.city_name),           'n.a.')) || '|' ||
        UPPER(COALESCE(TRIM(c.geography_type),      'n.a.')) AS geography_nk,
        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        ('CUSTOMER_LABEL=' || COALESCE(TRIM(c.customer_label),'n.a.')) AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.customer_label IS NOT NULL
),
snap_with_geo AS (
    SELECT s.*, COALESCE(g.geography_id, -1) AS geography_id
    FROM snap s
    LEFT JOIN bl_3nf.ce_geographies g
        ON g.geography_nk = s.geography_nk
)
INSERT INTO bl_3nf.ce_customers_scd
(customer_id, customer_nk, customer_label, customer_name, birth_dt, gender, education, occupation, yearly_income,
 total_children, children_at_home, house_owner_flg, cars_owned, date_first_purchase, geography_id,
 start_dt, end_dt, is_active,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_customers'),
    s.customer_nk,
    s.customer_label,
    s.customer_name,
    s.birth_dt,
    s.gender,
    s.education,
    s.occupation,
    s.yearly_income,
    s.total_children,
    s.children_at_home,
    s.house_owner_flg,
    s.cars_owned,
    s.date_first_purchase,
    s.geography_id,
    current_date,
    DATE '9999-12-31',
    'Y',
    s.source_system,
    s.source_entity,
    s.source_id,
    now(), now()
FROM snap_with_geo s
-- Insert only if there is no active row now (it was closed in step B)
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_customers_scd cur
    WHERE cur.customer_nk = s.customer_nk
      AND cur.is_active = 'Y'
);

COMMIT;

-- =========================================================
-- 10) CE_SALES_TARGETS (fact-like) - NO default row
-- =========================================================
WITH src AS (
    SELECT DISTINCT
        s.target_date::date AS target_dt,
        s.employee_key,
        CAST(s.sales_territory_key AS varchar) AS sales_territory_key,
        s.target::numeric(19,4) AS target_amt,
        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        ('TGT|'||COALESCE(s.target_date::varchar,'n.a.')||'|'||
              COALESCE(CAST(s.employee_key AS varchar),'n.a.')||'|'||
              COALESCE(CAST(s.sales_territory_key AS varchar),'n.a.')) AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.target_date IS NOT NULL AND s.target IS NOT NULL
)
INSERT INTO bl_3nf.ce_sales_targets
(target_id, target_nk, target_date_id, employee_id, territory_id, target_amt,
 source_system, source_entity, source_id, insert_dt, update_dt)
SELECT
    nextval('bl_3nf.seq_ce_sales_targets'),
    ('AW|'||src.target_dt::varchar||'|'||COALESCE(src.employee_key::varchar,'n.a.')||'|'||COALESCE(src.sales_territory_key,'n.a.')) AS target_nk,
    COALESCE(d.date_id, -1) AS target_date_id,
    COALESCE(e.employee_id, -1) AS employee_id,
    COALESCE(t.territory_id, -1) AS territory_id,
    src.target_amt,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(), now()
FROM src
LEFT JOIN bl_3nf.ce_dates d
    ON d.date_dt = src.target_dt
LEFT JOIN bl_3nf.ce_employees e
    ON e.employee_nk = ('AW|' || COALESCE(src.employee_key::varchar,'n.a.'))
LEFT JOIN bl_3nf.ce_sales_territories t
    ON t.territory_nk = ('AW|' || COALESCE(src.sales_territory_key,'n.a.'))
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_sales_targets x
    WHERE x.target_nk = ('AW|'||src.target_dt::varchar||'|'||COALESCE(src.employee_key::varchar,'n.a.')||'|'||COALESCE(src.sales_territory_key,'n.a.'))
);

COMMIT;

-- =========================================================
-- 11) CE_SALES (fact-like unified transactions) - NO default row
-- IMPORTANT: Option A (SCD2 FK): populate BOTH customer_id and customer_start_dt
-- =========================================================
WITH aw AS (
    SELECT DISTINCT
        ('AW|'||COALESCE(TRIM(s.sales_order_number),'n.a.')
             ||'|'||COALESCE(CAST(s.product_key AS varchar),'n.a.')
             ||'|'||COALESCE(s.sales_order_date::varchar,'n.a.')) AS sales_nk,
        ('AW|'||COALESCE(TRIM(s.sales_order_number),'n.a.')) AS order_nk,
        s.sales_order_number AS order_no,
        s.sales_order_date::date AS event_dt,

        ('AW|'||COALESCE(CAST(s.product_key AS varchar),'n.a.')) AS product_nk,
        NULL::varchar AS customer_nk,
        ('AW|'||COALESCE(CAST(s.reseller_key AS varchar),'n.a.')) AS reseller_nk,
        ('AW|'||COALESCE(CAST(s.employee_key AS varchar),COALESCE(TRIM(s.employee_id),'n.a.'))) AS employee_nk,
        NULL::varchar AS store_nk,
        ('AW|'||COALESCE(CAST(s.sales_territory_key AS varchar),'n.a.')) AS territory_nk,

        s.quantity,
        s.unit_price,
        s.total_sales AS sales_amt,
        s.cost AS cost_amt,

        'SA_ADVENTUREWORKS'::varchar AS source_system,
        'SRC_SALES'::varchar AS source_entity,
        ('AW_ROW='||COALESCE(TRIM(s.sales_order_number),'n.a.')) AS source_id
    FROM sa_adventureworks.src_sales s
    WHERE s.sales_order_date IS NOT NULL
),
ct AS (
    SELECT DISTINCT
        ('CT|'||COALESCE(TRIM(c.sales_order_number),'n.a.')
             ||'|'||COALESCE(CAST(c.online_sales_key AS varchar),'n.a.')) AS sales_nk,
        ('CT|'||COALESCE(TRIM(c.sales_order_number),'n.a.')) AS order_nk,
        c.sales_order_number AS order_no,
        c.full_date::date AS event_dt,

        ('CT|'||COALESCE(TRIM(c.product_description),'n.a.')) AS product_nk,
        COALESCE(TRIM(c.customer_label),'n.a.') AS customer_nk,
        NULL::varchar AS reseller_nk,
        NULL::varchar AS employee_nk,
        ('CT|'||COALESCE(CAST(c.entity_key AS varchar),'n.a.')) AS store_nk,
        NULL::varchar AS territory_nk,

        NULL::int AS quantity,
        c.unit_price,
        c.sales_amount AS sales_amt,
        NULL::numeric(19,4) AS cost_amt,

        'SA_CONTOSO'::varchar AS source_system,
        COALESCE(TRIM(c.source_entity), 'SRC_ONLINE_SALES')::varchar AS source_entity,
        ('ONLINE_SALES_KEY='||COALESCE(CAST(c.online_sales_key AS varchar),'n.a.')) AS source_id
    FROM sa_contoso.src_online_sales c
    WHERE c.full_date IS NOT NULL
),
src AS (
    SELECT * FROM aw
    UNION
    SELECT * FROM ct
)
INSERT INTO bl_3nf.ce_sales
(
    sales_id, sales_nk, order_nk, order_no,
    date_id, product_id,
    customer_id, customer_start_dt,
    reseller_id, employee_id, store_id, territory_id,
    quantity, unit_price, sales_amt, cost_amt,
    source_system, source_entity, source_id,
    insert_dt, update_dt
)
SELECT
    nextval('bl_3nf.seq_ce_sales'),
    s.sales_nk,
    s.order_nk,
    s.order_no,

    COALESCE(d.date_id, -1) AS date_id,
    COALESCE(p.product_id, -1) AS product_id,

    -- Option A: SCD2 composite FK
    COALESCE(ca.customer_id, -1) AS customer_id,
    COALESCE(ca.start_dt, DATE '1900-01-01') AS customer_start_dt,

    COALESCE(r.reseller_id, -1) AS reseller_id,
    COALESCE(e.employee_id, -1) AS employee_id,
    COALESCE(st.store_id, -1) AS store_id,
    COALESCE(t.territory_id, -1) AS territory_id,

    s.quantity,
    s.unit_price,
    s.sales_amt,
    s.cost_amt,

    s.source_system,
    s.source_entity,
    s.source_id,
    now(), now()
FROM src s
LEFT JOIN bl_3nf.ce_dates d
    ON d.date_dt = s.event_dt
LEFT JOIN bl_3nf.ce_products p
    ON p.product_nk = s.product_nk

-- Active SCD2 customer for Contoso; AW falls back to (-1, 1900-01-01)
LEFT JOIN bl_3nf.ce_customers_scd ca
    ON ca.customer_nk = COALESCE(s.customer_nk, 'N/A')
   AND ca.is_active = 'Y'

LEFT JOIN bl_3nf.ce_resellers r
    ON r.reseller_nk = COALESCE(s.reseller_nk,'N/A')
LEFT JOIN bl_3nf.ce_employees e
    ON e.employee_nk = COALESCE(s.employee_nk,'N/A')
LEFT JOIN bl_3nf.ce_stores st
    ON st.store_nk = COALESCE(s.store_nk,'N/A')
LEFT JOIN bl_3nf.ce_sales_territories t
    ON t.territory_nk = COALESCE(s.territory_nk,'N/A')

WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_sales x
    WHERE x.sales_nk = s.sales_nk
);

COMMIT;
