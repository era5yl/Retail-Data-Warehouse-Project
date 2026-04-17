
-- 0) SCHEMA + LOGGING


CREATE SCHEMA IF NOT EXISTS bl_cl;

CREATE TABLE IF NOT EXISTS bl_cl.mta_etl_log
(
    log_id         BIGSERIAL PRIMARY KEY,
    log_dttm       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name VARCHAR(200) NOT NULL,
    rows_affected  INTEGER NOT NULL,
    log_message    VARCHAR(1000) NOT NULL
);

CREATE OR REPLACE PROCEDURE bl_cl.prc_write_log
(
    p_procedure_name VARCHAR,
    p_rows_affected  INTEGER,
    p_message        VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_cl.mta_etl_log(procedure_name, rows_affected, log_message)
    VALUES (p_procedure_name, COALESCE(p_rows_affected,0), COALESCE(p_message,''));
END;
$$;


-- 1) DEFAULT ROWS (for FK safety)


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_defaults_3nf()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT := 0;
    v_rc   INT := 0;
BEGIN
    -- CE_SEASONS default
    INSERT INTO bl_3nf.ce_seasons
    (season_surr_id, europe_season, north_america_season, asia_season,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_seasons WHERE season_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_DATES default
    INSERT INTO bl_3nf.ce_dates
    (date_surr_id, full_date_label, quarter, season_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, DATE '1900-01-01', 'n.a.', -1, now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_dates WHERE date_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_GEOGRAPHY default
    INSERT INTO bl_3nf.ce_geography
    (geography_surr_id, city, state, country, geography_type,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_geography WHERE geography_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_PRODUCT_CATEGORIES default
    INSERT INTO bl_3nf.ce_product_categories
    (product_category_surr_id, product_category_description,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_categories WHERE product_category_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_PRODUCT_SUBCATEGORIES default
    INSERT INTO bl_3nf.ce_product_subcategories
    (product_subcategory_surr_id, product_subcategory_label, product_subcategory_description, product_category_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', 'n.a.', -1, now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_subcategories WHERE product_subcategory_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_PRODUCTS default
    INSERT INTO bl_3nf.ce_products
    (product_surr_id, product_description, manufacturer, brand_name, class_name, style_name,
     product_subcategory_surr_id, size, weight, stock_type, status_product,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.',
           -1, 'n.a.', NULL, 'n.a.', 'n.a.',
           now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_products WHERE product_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_EMPLOYEES default
    INSERT INTO bl_3nf.ce_employees
    (employee_surr_id, employee_business_id, name, title, email,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'N/A', 'n.a.', 'n.a.', 'n.a.', now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_employees WHERE employee_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_RESELLERS default
    INSERT INTO bl_3nf.ce_resellers
    (reseller_surr_id, reseller_name, reseller_business_type, geography_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'n.a.', 'n.a.', -1, now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_resellers WHERE reseller_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_STORE default
    INSERT INTO bl_3nf.ce_stores
    (store_surr_id, store_key, store_status, store_parent_entity, entity_description, entity_type, entity_status,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT -1, 'N/A', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.',
           now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_surr_id = -1);
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    -- CE_CUSTOMERS_SCD2 default
    -- IMPORTANT: this assumes your DDL includes SCD2 columns (start_dt,end_dt,is_active).
    INSERT INTO bl_3nf.ce_customers_scd2
    (customer_surr_id, customer_label, customer_name, birth_date, gender, education, occupation,
     yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned,
     start_dt, end_dt, is_active,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        -1, 'N/A', 'n.a.', NULL, 'n.a.', 'n.a.', 'n.a.',
        NULL, NULL, NULL, 'n.a.', NULL,
        DATE '1900-01-01', DATE '9999-12-31', 'Y',
        now(), now(), 'MANUAL','MANUAL','-1'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_customers_scd2
        WHERE customer_surr_id = -1 AND start_dt = DATE '1900-01-01'
    );
    GET DIAGNOSTICS v_rc = ROW_COUNT; v_rows := v_rows + v_rc;

    CALL bl_cl.prc_write_log('bl_cl.prc_load_defaults_3nf', v_rows, 'Default rows loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_defaults_3nf', 0, SQLERRM);
        RAISE;
END;
$$;


-- 2) CE_SEASONS (from Contoso)

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_seasons()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            COALESCE(TRIM(europe_season),'n.a.')        AS europe_season,
            COALESCE(TRIM(north_america_season),'n.a.') AS north_america_season,
            COALESCE(TRIM(asia_season),'n.a.')          AS asia_season,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            (COALESCE(TRIM(europe_season),'n.a.')||'|'||
             COALESCE(TRIM(north_america_season),'n.a.')||'|'||
             COALESCE(TRIM(asia_season),'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales
    )
    INSERT INTO bl_3nf.ce_seasons
    (season_surr_id, europe_season, north_america_season, asia_season,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_seasons'),
        s.europe_season, s.north_america_season, s.asia_season,
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_seasons t
        WHERE COALESCE(t.europe_season,'') = COALESCE(s.europe_season,'')
          AND COALESCE(t.north_america_season,'') = COALESCE(s.north_america_season,'')
          AND COALESCE(t.asia_season,'') = COALESCE(s.asia_season,'')
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_seasons', v_rows, 'CE_SEASONS loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_seasons', 0, SQLERRM);
        RAISE;
END;
$$;


-- 3) CE_DATES (from AW sales_order_date + AW target_date + CT full_date)
--    Linked to seasons via season values (Contoso only; AW uses default -1)


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_dates()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src_raw AS (
        -- AW sales dates
        SELECT
            s.sales_order_date::date AS full_date_label,
            ('Q' || EXTRACT(QUARTER FROM s.sales_order_date::date)::int)::varchar AS quarter,
            NULL::varchar AS europe_season,
            NULL::varchar AS north_america_season,
            NULL::varchar AS asia_season,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            s.sales_order_date::varchar AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.sales_order_date IS NOT NULL

        UNION ALL

        -- AW target dates
        SELECT
            s.target_date::date AS full_date_label,
            ('Q' || EXTRACT(QUARTER FROM s.target_date::date)::int)::varchar AS quarter,
            NULL::varchar AS europe_season,
            NULL::varchar AS north_america_season,
            NULL::varchar AS asia_season,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            s.target_date::varchar AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.target_date IS NOT NULL

        UNION ALL

        -- Contoso dates + seasons
        SELECT
            c.full_date::date AS full_date_label,
            ('Q' || EXTRACT(QUARTER FROM c.full_date::date)::int)::varchar AS quarter,
            COALESCE(TRIM(c.europe_season),'n.a.')        AS europe_season,
            COALESCE(TRIM(c.north_america_season),'n.a.') AS north_america_season,
            COALESCE(TRIM(c.asia_season),'n.a.')          AS asia_season,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            c.full_date::varchar AS source_id
        FROM sa_contoso.src_online_sales c
        WHERE c.full_date IS NOT NULL
    ),
    src AS (
        SELECT
            full_date_label,
            MAX(quarter) AS quarter,
            MAX(europe_season) AS europe_season,
            MAX(north_america_season) AS north_america_season,
            MAX(asia_season) AS asia_season
        FROM src_raw
        GROUP BY full_date_label
    )
    INSERT INTO bl_3nf.ce_dates
    (date_surr_id, full_date_label, quarter, season_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_dates'),
        s.full_date_label,
        s.quarter,
        COALESCE(se.season_surr_id, -1) AS season_surr_id,
        now(), now(),
        'BL_CL','BL_CL', s.full_date_label::varchar
    FROM src s
    LEFT JOIN bl_3nf.ce_seasons se
      ON COALESCE(se.europe_season,'n.a.') = COALESCE(s.europe_season,'n.a.')
     AND COALESCE(se.north_america_season,'n.a.') = COALESCE(s.north_america_season,'n.a.')
     AND COALESCE(se.asia_season,'n.a.') = COALESCE(s.asia_season,'n.a.')
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_dates d WHERE d.full_date_label = s.full_date_label
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_dates', v_rows, 'CE_DATES loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_dates', 0, SQLERRM);
        RAISE;
END;
$$;


-- 4) CE_GEOGRAPHY
--   - Contoso: (city,state,country,geography_type)
--   - AW reseller city/state/country as geography_type='City'
--   - AW sales territory as geography_type='SalesTerritory'
--     (city='n.a.', state=region, country=country)


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_geography()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH ct AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.city_name),'n.a.') AS city,
            COALESCE(TRIM(c.state_province_name),'n.a.') AS state,
            COALESCE(TRIM(c.region_country_name),'n.a.') AS country,
            COALESCE(TRIM(c.geography_type),'n.a.') AS geography_type,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            (COALESCE(TRIM(c.region_country_name),'n.a.')||'|'||
             COALESCE(TRIM(c.state_province_name),'n.a.')||'|'||
             COALESCE(TRIM(c.city_name),'n.a.')||'|'||
             COALESCE(TRIM(c.geography_type),'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
    ),
    aw_city AS (
        SELECT DISTINCT
            COALESCE(TRIM(s.reseller_city),'n.a.') AS city,
            COALESCE(TRIM(s.reseller_state),'n.a.') AS state,
            COALESCE(TRIM(s.reseller_country),'n.a.') AS country,
            'City'::varchar AS geography_type,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            (COALESCE(TRIM(s.reseller_country),'n.a.')||'|'||
             COALESCE(TRIM(s.reseller_state),'n.a.')||'|'||
             COALESCE(TRIM(s.reseller_city),'n.a.')||'|City') AS source_id
        FROM sa_adventureworks.src_sales s
    ),
    aw_terr AS (
        SELECT DISTINCT
            'n.a.'::varchar AS city,
            COALESCE(TRIM(s.sales_territory_region),'n.a.') AS state,
            COALESCE(TRIM(s.sales_territory_country),'n.a.') AS country,
            'SalesTerritory'::varchar AS geography_type,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('TERR|'||COALESCE(s.sales_territory_key::varchar,'n.a.')
                  ||'|'||COALESCE(TRIM(s.sales_territory_country),'n.a.')
                  ||'|'||COALESCE(TRIM(s.sales_territory_region),'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.sales_territory_key IS NOT NULL
    ),
    src AS (
        SELECT * FROM ct
        UNION ALL
        SELECT * FROM aw_city
        UNION ALL
        SELECT * FROM aw_terr
    )
    INSERT INTO bl_3nf.ce_geography
    (geography_surr_id, city, state, country, geography_type,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_geography'),
        s.city, s.state, s.country, s.geography_type,
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_geography g
        WHERE COALESCE(g.city,'') = COALESCE(s.city,'')
          AND COALESCE(g.state,'') = COALESCE(s.state,'')
          AND COALESCE(g.country,'') = COALESCE(s.country,'')
          AND COALESCE(g.geography_type,'') = COALESCE(s.geography_type,'')
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_geography', v_rows, 'CE_GEOGRAPHY loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_geography', 0, SQLERRM);
        RAISE;
END;
$$;


-- 5) PRODUCT CATEGORIES / SUBCATEGORIES / PRODUCTS


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_product_categories()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.product_category_description),'n.a.') AS product_category_description,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            ('CAT|'||COALESCE(TRIM(c.product_category_description),'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
    )
    INSERT INTO bl_3nf.ce_product_categories
    (product_category_surr_id, product_category_description,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_product_categories'),
        s.product_category_description,
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_product_categories t
        WHERE t.product_category_description = s.product_category_description
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_product_categories', v_rows, 'CE_PRODUCT_CATEGORIES loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_product_categories', 0, SQLERRM);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_product_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.product_subcategory_label),'n.a.') AS product_subcategory_label,
            COALESCE(TRIM(c.product_subcategory_description),'n.a.') AS product_subcategory_description,
            COALESCE(TRIM(c.product_category_description),'n.a.') AS product_category_description,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            ('SUBCAT|'||COALESCE(TRIM(c.product_subcategory_label),'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
    )
    INSERT INTO bl_3nf.ce_product_subcategories
    (product_subcategory_surr_id, product_subcategory_label, product_subcategory_description, product_category_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_product_subcategories'),
        s.product_subcategory_label,
        s.product_subcategory_description,
        COALESCE(cat.product_category_surr_id, -1),
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    LEFT JOIN bl_3nf.ce_product_categories cat
      ON cat.product_category_description = s.product_category_description
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_product_subcategories t
        WHERE t.product_subcategory_label = s.product_subcategory_label
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_product_subcategories', v_rows, 'CE_PRODUCT_SUBCATEGORIES loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_product_subcategories', 0, SQLERRM);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_products()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH ct AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.product_description),'n.a.') AS product_description,
            COALESCE(TRIM(c.manufacturer),'n.a.') AS manufacturer,
            COALESCE(TRIM(c.brand_name),'n.a.') AS brand_name,
            COALESCE(TRIM(c.class_name),'n.a.') AS class_name,
            COALESCE(TRIM(c.style_name),'n.a.') AS style_name,
            COALESCE(TRIM(c.product_subcategory_label),'n.a.') AS product_subcategory_label,
            COALESCE(TRIM(c.size_txt),'n.a.') AS size,
            c.weight_num AS weight,
            COALESCE(TRIM(c.stock_type_name),'n.a.') AS stock_type,
            COALESCE(TRIM(c.status_product),'n.a.') AS status_product,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            ('CT_PROD|'||COALESCE(TRIM(c.product_description),'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
        WHERE c.product_description IS NOT NULL
    ),
    aw AS (
        SELECT DISTINCT
            COALESCE(TRIM(s.product_name),'n.a.') AS product_description,
            'n.a.'::varchar AS manufacturer,
            'n.a.'::varchar AS brand_name,
            'n.a.'::varchar AS class_name,
            'n.a.'::varchar AS style_name,
            'n.a.'::varchar AS product_subcategory_label,
            'n.a.'::varchar AS size,
            NULL::numeric(18,4) AS weight,
            'n.a.'::varchar AS stock_type,
            'n.a.'::varchar AS status_product,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('AW_PROD|'||COALESCE(s.product_key::varchar,'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.product_key IS NOT NULL OR s.product_name IS NOT NULL
    ),
    src AS (
        SELECT * FROM ct
        UNION ALL
        SELECT * FROM aw
    )
    INSERT INTO bl_3nf.ce_products
    (product_surr_id, product_description, manufacturer, brand_name, class_name, style_name,
     product_subcategory_surr_id, size, weight, stock_type, status_product,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_products'),
        s.product_description,
        s.manufacturer,
        s.brand_name,
        s.class_name,
        s.style_name,
        COALESCE(sub.product_subcategory_surr_id, -1),
        s.size,
        s.weight,
        s.stock_type,
        s.status_product,
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    LEFT JOIN bl_3nf.ce_product_subcategories sub
      ON sub.product_subcategory_label = s.product_subcategory_label
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_products p
        WHERE p.source_id = s.source_id
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_products', v_rows, 'CE_PRODUCTS loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_products', 0, SQLERRM);
        RAISE;
END;
$$;


-- 6) EMPLOYEES (from AW)


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_employees()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            COALESCE(TRIM(s.employee_id),'n.a.') AS employee_business_id,
            COALESCE(TRIM(s.salesperson_fullname),'n.a.') AS name,
            COALESCE(TRIM(s.salesperson_title),'n.a.') AS title,
            COALESCE(TRIM(s.email_address),'n.a.') AS email,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('EMP|'||COALESCE(TRIM(s.employee_id),'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.employee_id IS NOT NULL OR s.salesperson_fullname IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_employees
    (employee_surr_id, employee_business_id, name, title, email,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_employees'),
        s.employee_business_id,
        s.name,
        s.title,
        s.email,
        now(), now(), s.source_system, s.source_entity, s.source_id
    FROM src s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_employees e
        WHERE e.employee_business_id = s.employee_business_id
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_employees', v_rows, 'CE_EMPLOYEES loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_employees', 0, SQLERRM);
        RAISE;
END;
$$;


-- 7) EMPLOYEE_TERRITORY (M:N) from AW assigned_sales_territory + employee_id
--    We map territory numbers to GEOGRAPHY rows created as geography_type='SalesTerritory'
--    (state=region, country=country, city='n.a.')


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_employee_territory()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH base AS (
        SELECT DISTINCT
            COALESCE(TRIM(s.employee_id),'n.a.') AS employee_business_id,
            COALESCE(TRIM(s.sales_territory_region),'n.a.') AS region,
            COALESCE(TRIM(s.sales_territory_country),'n.a.') AS country,
            COALESCE(TRIM(s.assigned_sales_territory),'') AS assigned_list
        FROM sa_adventureworks.src_sales s
        WHERE s.employee_id IS NOT NULL
          AND s.assigned_sales_territory IS NOT NULL
          AND TRIM(s.assigned_sales_territory) <> ''
    ),
    exploded AS (
        SELECT
            b.employee_business_id,
            TRIM(x.val) AS terr_key,
            b.region,
            b.country
        FROM base b
        CROSS JOIN LATERAL unnest(string_to_array(b.assigned_list, ',')) AS x(val)
        WHERE TRIM(x.val) <> ''
    ),
    resolved AS (
        SELECT
            COALESCE(e.employee_surr_id, -1) AS employee_surr_id,
            COALESCE(g.geography_surr_id, -1) AS geography_surr_id,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('EMP_TERR|'||ex.employee_business_id||'|'||ex.terr_key) AS source_id
        FROM exploded ex
        LEFT JOIN bl_3nf.ce_employees e
          ON e.employee_business_id = ex.employee_business_id
        LEFT JOIN bl_3nf.ce_geography g
          ON g.geography_type = 'SalesTerritory'
         AND COALESCE(g.city,'n.a.') = 'n.a.'
         AND COALESCE(g.state,'n.a.') = COALESCE(ex.region,'n.a.')
         AND COALESCE(g.country,'n.a.') = COALESCE(ex.country,'n.a.')
    ),
    dedup AS (
        SELECT DISTINCT
            geography_surr_id,
            employee_surr_id,
            source_system,
            source_entity,
            source_id
        FROM resolved
        WHERE employee_surr_id <> -1
          AND geography_surr_id <> -1
    )
    INSERT INTO bl_3nf.ce_employee_territory
    (geography_surr_id, employee_surr_id, insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        d.geography_surr_id,
        d.employee_surr_id,
        NOW(), NOW(),
        d.source_system, d.source_entity, d.source_id
    FROM dedup d
    ON CONFLICT (geography_surr_id, employee_surr_id) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_employee_territory', v_rows, 'CE_EMPLOYEE_TERRITORY loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_employee_territory', 0, SQLERRM);
        RAISE;
END;
$$;


-- 8) RESELLERS -> GEOGRAPHY (AW)


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_resellers()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            COALESCE(TRIM(s.reseller_name),'n.a.') AS reseller_name,
            COALESCE(TRIM(s.reseller_business_type),'n.a.') AS reseller_business_type,
            COALESCE(TRIM(s.reseller_city),'n.a.') AS city,
            COALESCE(TRIM(s.reseller_state),'n.a.') AS state,
            COALESCE(TRIM(s.reseller_country),'n.a.') AS country,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('RESELLER|'||COALESCE(s.reseller_key::varchar,'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.reseller_key IS NOT NULL OR s.reseller_name IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_resellers
    (reseller_surr_id, reseller_name, reseller_business_type, geography_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_resellers'),
        s.reseller_name,
        s.reseller_business_type,
        COALESCE(g.geography_surr_id, -1),
        now(), now(),
        s.source_system, s.source_entity, s.source_id
    FROM src s
    LEFT JOIN bl_3nf.ce_geography g
      ON g.geography_type = 'City'
     AND COALESCE(g.city,'n.a.') = COALESCE(s.city,'n.a.')
     AND COALESCE(g.state,'n.a.') = COALESCE(s.state,'n.a.')
     AND COALESCE(g.country,'n.a.') = COALESCE(s.country,'n.a.')
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_resellers r
        WHERE r.source_id = s.source_id
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_resellers', v_rows, 'CE_RESELLERS loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_resellers', 0, SQLERRM);
        RAISE;
END;
$$;


-- 9) STORE (Contoso)

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            ('CT|'||COALESCE(c.entity_key::varchar,'n.a.')) AS store_key,
            COALESCE(TRIM(c.status_store),'n.a.') AS store_status,
            COALESCE(TRIM(c.parent_entity_label),'n.a.') AS store_parent_entity,
            COALESCE(TRIM(c.entity_description),'n.a.') AS entity_description,
            COALESCE(TRIM(c.entity_type),'n.a.') AS entity_type,
            COALESCE(TRIM(c.status_entity),'n.a.') AS entity_status,
            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            ('STORE|'||COALESCE(c.entity_key::varchar,'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
        WHERE c.entity_key IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_stores
    (store_surr_id, store_key, store_status, store_parent_entity, entity_description, entity_type, entity_status,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_stores'),
        s.store_key,
        s.store_status,
        s.store_parent_entity,
        s.entity_description,
        s.entity_type,
        s.entity_status,
        now(), now(),
        s.source_system, s.source_entity, s.source_id
    FROM src s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_stores st
        WHERE st.store_key = s.store_key
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_stores', v_rows, 'CE_STORES loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_stores', 0, SQLERRM);
        RAISE;
END;
$$;


-- 10) CUSTOMERS SCD2 (Contoso).


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_customers_scd2()
LANGUAGE plpgsql
AS $$
DECLARE
    v_ins_new   INT := 0;
    v_upd_close INT := 0;
    v_ins_ver   INT := 0;
BEGIN
    -- A) Insert brand-new customers (no active)
    WITH snap AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.customer_label),'n.a.') AS customer_label,
            COALESCE(TRIM(c.customer_name),'n.a.')  AS customer_name,
            c.birth_date::date AS birth_date,
            COALESCE(TRIM(c.gender),'n.a.')     AS gender,
            COALESCE(TRIM(c.education),'n.a.')  AS education,
            COALESCE(TRIM(c.occupation),'n.a.') AS occupation,
            c.yearly_income,
            c.total_children,
            c.number_children_at_home,
            COALESCE(TRIM(c.house_owner_flag),'n.a.') AS house_owner_flag,
            c.number_cars_owned
        FROM sa_contoso.src_online_sales c
        WHERE c.customer_label IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_customers_scd2
    (customer_surr_id, customer_label, customer_name, birth_date, gender, education, occupation,
     yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned,
     start_dt, end_dt, is_active,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_customers_scd2'),
        s.customer_label,
        s.customer_name,
        s.birth_date,
        s.gender,
        s.education,
        s.occupation,
        s.yearly_income,
        s.total_children,
        s.number_children_at_home,
        s.house_owner_flag,
        s.number_cars_owned,
        current_date,
        DATE '9999-12-31',
        'Y',
        now(), now(),
        'SA_CONTOSO', 'SRC_ONLINE_SALES',
        ('CUST|'||s.customer_label)
    FROM snap s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_customers_scd2 cur
        WHERE cur.customer_label = s.customer_label
          AND cur.is_active = 'Y'
    );
    GET DIAGNOSTICS v_ins_new = ROW_COUNT;

    -- B) Close active rows that changed
    WITH snap AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.customer_label),'n.a.') AS customer_label,
            COALESCE(TRIM(c.customer_name),'n.a.')  AS customer_name,
            c.birth_date::date AS birth_date,
            COALESCE(TRIM(c.gender),'n.a.')     AS gender,
            COALESCE(TRIM(c.education),'n.a.')  AS education,
            COALESCE(TRIM(c.occupation),'n.a.') AS occupation,
            c.yearly_income,
            c.total_children,
            c.number_children_at_home,
            COALESCE(TRIM(c.house_owner_flag),'n.a.') AS house_owner_flag,
            c.number_cars_owned
        FROM sa_contoso.src_online_sales c
        WHERE c.customer_label IS NOT NULL
    ),
    changed AS (
        SELECT cur.customer_surr_id, cur.start_dt
        FROM bl_3nf.ce_customers_scd2 cur
        JOIN snap s
          ON s.customer_label = cur.customer_label
        WHERE cur.is_active = 'Y'
          AND (
                COALESCE(cur.customer_name,'') <> COALESCE(s.customer_name,'')
             OR COALESCE(cur.birth_date, DATE '1900-01-01') <> COALESCE(s.birth_date, DATE '1900-01-01')
             OR COALESCE(cur.gender,'') <> COALESCE(s.gender,'')
             OR COALESCE(cur.education,'') <> COALESCE(s.education,'')
             OR COALESCE(cur.occupation,'') <> COALESCE(s.occupation,'')
             OR COALESCE(cur.yearly_income, 0) <> COALESCE(s.yearly_income, 0)
             OR COALESCE(cur.total_children, -1) <> COALESCE(s.total_children, -1)
             OR COALESCE(cur.number_children_at_home, -1) <> COALESCE(s.number_children_at_home, -1)
             OR COALESCE(cur.house_owner_flag,'') <> COALESCE(s.house_owner_flag,'')
             OR COALESCE(cur.number_cars_owned, -1) <> COALESCE(s.number_cars_owned, -1)
          )
    )
    UPDATE bl_3nf.ce_customers_scd2 cur
       SET end_dt = current_date - 1,
           is_active = 'N',
           update_dt = now()
      FROM changed ch
     WHERE cur.customer_surr_id = ch.customer_surr_id
       AND cur.start_dt = ch.start_dt;
    GET DIAGNOSTICS v_upd_close = ROW_COUNT;

    -- C) Insert new versions
    WITH snap AS (
        SELECT DISTINCT
            COALESCE(TRIM(c.customer_label),'n.a.') AS customer_label,
            COALESCE(TRIM(c.customer_name),'n.a.')  AS customer_name,
            c.birth_date::date AS birth_date,
            COALESCE(TRIM(c.gender),'n.a.')     AS gender,
            COALESCE(TRIM(c.education),'n.a.')  AS education,
            COALESCE(TRIM(c.occupation),'n.a.') AS occupation,
            c.yearly_income,
            c.total_children,
            c.number_children_at_home,
            COALESCE(TRIM(c.house_owner_flag),'n.a.') AS house_owner_flag,
            c.number_cars_owned
        FROM sa_contoso.src_online_sales c
        WHERE c.customer_label IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_customers_scd2
    (customer_surr_id, customer_label, customer_name, birth_date, gender, education, occupation,
     yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned,
     start_dt, end_dt, is_active,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_customers_scd2'),
        s.customer_label,
        s.customer_name,
        s.birth_date,
        s.gender,
        s.education,
        s.occupation,
        s.yearly_income,
        s.total_children,
        s.number_children_at_home,
        s.house_owner_flag,
        s.number_cars_owned,
        current_date,
        DATE '9999-12-31',
        'Y',
        now(), now(),
        'SA_CONTOSO', 'SRC_ONLINE_SALES',
        ('CUST|'||s.customer_label)
    FROM snap s
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_customers_scd2 cur
        WHERE cur.customer_label = s.customer_label
          AND cur.is_active = 'Y'
    );
    GET DIAGNOSTICS v_ins_ver = ROW_COUNT;

    CALL bl_cl.prc_write_log(
        'bl_cl.prc_load_ce_customers_scd2',
        v_ins_new + v_upd_close + v_ins_ver,
        'SCD2 done: new='||v_ins_new||', closed='||v_upd_close||', versions='||v_ins_ver
    );
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_customers_scd2', 0, SQLERRM);
        RAISE;
END;
$$;


-- 11) FACT-LIKE: CE_SALES_ORDERS
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_sales_orders()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT := 0;
BEGIN
    WITH aw AS (
        SELECT
            ('AW|'||COALESCE(TRIM(s.sales_order_number),'n.a.')
                 ||'|'||COALESCE(s.sales_order_date::varchar,'n.a.')
                 ||'|'||COALESCE(s.product_key::varchar,'n.a.')) AS sales_order_id,

            s.total_sales AS sales_amount,
            s.cost        AS cost,              
            s.quantity,
            s.unit_price,
            s.sales_order_date::date AS order_dt,

            COALESCE(TRIM(s.product_name),'n.a.') AS product_desc,
            COALESCE(TRIM(s.employee_id),'n.a.') AS employee_business_id,
            COALESCE(TRIM(s.reseller_key::varchar),'n.a.') AS reseller_id,
            NULL::varchar AS store_id,

            COALESCE(TRIM(s.reseller_city),'n.a.') AS city,
            COALESCE(TRIM(s.reseller_state),'n.a.') AS state,
            COALESCE(TRIM(s.reseller_country),'n.a.') AS country,
            'City'::varchar AS geography_type,

            NULL::varchar AS customer_label,

            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('AW_ROW|'||COALESCE(TRIM(s.sales_order_number),'n.a.')
                     ||'|'||COALESCE(s.product_key::varchar,'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.sales_order_date IS NOT NULL
    ),
    ct AS (
        SELECT
            ('CT|'||COALESCE(TRIM(c.sales_order_number),'n.a.')
                 ||'|'||COALESCE(c.online_sales_key::varchar,'n.a.')) AS sales_order_id,

            c.sales_amount AS sales_amount,
            c.unit_cost    AS cost,                    
            NULL::int AS quantity,
            c.unit_price AS unit_price,
            c.full_date::date AS order_dt,

            COALESCE(TRIM(c.product_description),'n.a.') AS product_desc,
            NULL::varchar AS employee_business_id,

            NULL::varchar AS reseller_id,

            NULL::varchar AS store_id,

            COALESCE(TRIM(c.city_name),'n.a.') AS city,
            COALESCE(TRIM(c.state_province_name),'n.a.') AS state,
            COALESCE(TRIM(c.region_country_name),'n.a.') AS country,
            COALESCE(TRIM(c.geography_type),'n.a.') AS geography_type,

            COALESCE(TRIM(c.customer_label),'n.a.') AS customer_label,

            'SA_CONTOSO'::varchar AS source_system,
            COALESCE(TRIM(c.source_entity),'SRC_ONLINE_SALES')::varchar AS source_entity,
            ('CT_ROW|'||COALESCE(c.online_sales_key::varchar,'n.a.')) AS source_id
        FROM sa_contoso.src_online_sales c
        WHERE c.full_date IS NOT NULL
    ),
    src AS (
        SELECT * FROM aw
        UNION ALL
        SELECT * FROM ct
    ),
    src_dedup AS (
        SELECT DISTINCT ON (s.sales_order_id)
            s.*
        FROM src s
        ORDER BY s.sales_order_id,
                 s.source_system DESC,
                 s.source_id DESC
    )
    INSERT INTO bl_3nf.ce_sales_orders
    (
        sales_surr_id,
        sales_order_id,
        sales_amount,
        cost,             
        quantity,
        unit_price,
        date_surr_id,
        customer_surr_id,
        product_surr_id,
        employee_surr_id,
        geography_surr_id,
        reseller_surr_id,  
        store_surr_id,     
        insert_dt,
        update_dt,
        source_system,
        source_entity,
        source_id
    )
    SELECT
        nextval('bl_3nf.seq_ce_sales_orders'),
        s.sales_order_id,
        s.sales_amount,
        s.cost,             
        s.quantity,
        s.unit_price,
        COALESCE(d.date_surr_id, -1),
        COALESCE(cu.customer_surr_id, -1),
        COALESCE(p.product_surr_id, -1),
        COALESCE(e.employee_surr_id, -1),
        COALESCE(g.geography_surr_id, -1),
        COALESCE(r.reseller_surr_id, -1),
        COALESCE(st.store_surr_id, -1),

        NOW(), NOW(),
        s.source_system,
        s.source_entity,
        s.source_id
    FROM src_dedup s
    LEFT JOIN bl_3nf.ce_dates d
      ON d.full_date_label = s.order_dt
    LEFT JOIN bl_3nf.ce_products p
      ON p.product_description = s.product_desc
    LEFT JOIN bl_3nf.ce_employees e
      ON e.employee_business_id = COALESCE(s.employee_business_id, 'N/A')
    LEFT JOIN bl_3nf.ce_geography g
      ON COALESCE(g.city,'n.a.') = COALESCE(s.city,'n.a.')
     AND COALESCE(g.state,'n.a.') = COALESCE(s.state,'n.a.')
     AND COALESCE(g.country,'n.a.') = COALESCE(s.country,'n.a.')
     AND COALESCE(g.geography_type,'n.a.') = COALESCE(s.geography_type,'n.a.')
    LEFT JOIN bl_3nf.ce_customers_scd2 cu
      ON cu.customer_label = COALESCE(s.customer_label, 'N/A')
     AND cu.is_active = 'Y'

    LEFT JOIN bl_3nf.ce_resellers r
      ON r.source_id = COALESCE(s.reseller_id, 'N/A')

    LEFT JOIN bl_3nf.ce_stores st
      ON st.store_key = COALESCE(s.store_id, 'N/A')

    ON CONFLICT (sales_order_id) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_sales_orders', v_rows, 'CE_SALES_ORDERS loaded');

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_sales_orders', 0, SQLERRM);
        RAISE;
END;
$$;

-- 12) FACT-LIKE: CE_TARGETS (ONLY date FK)
--    From AW: target, target_date


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_targets()
LANGUAGE plpgsql
AS $$
DECLARE v_rows INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT
            s.target_date::date AS target_dt,
            s.target::numeric(19,4) AS target_amt,
            'SA_ADVENTUREWORKS'::varchar AS source_system,
            'SRC_SALES'::varchar AS source_entity,
            ('TGT|'||COALESCE(s.target_date::varchar,'n.a.')||'|'||COALESCE(s.target::varchar,'n.a.')) AS source_id
        FROM sa_adventureworks.src_sales s
        WHERE s.target_date IS NOT NULL
          AND s.target IS NOT NULL
    )
    INSERT INTO bl_3nf.ce_targets
    (target_surr_id, target, date_surr_id,
     insert_dt, update_dt, source_system, source_entity, source_id)
    SELECT
        nextval('bl_3nf.seq_ce_targets'),
        s.target_amt,
        COALESCE(d.date_surr_id, -1),
        now(), now(),
        s.source_system, s.source_entity, s.source_id
    FROM src s
    LEFT JOIN bl_3nf.ce_dates d
      ON d.full_date_label = s.target_dt
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_targets t
        WHERE t.source_id = s.source_id
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_targets', v_rows, 'CE_TARGETS loaded');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_load_ce_targets', 0, SQLERRM);
        RAISE;
END;
$$;


-- 13) MASTER RUNNER (correct order)


CREATE OR REPLACE PROCEDURE bl_cl.prc_run_load_bl_3nf()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL bl_cl.prc_load_defaults_3nf();

    CALL bl_cl.prc_load_ce_seasons();
    CALL bl_cl.prc_load_ce_dates();

    CALL bl_cl.prc_load_ce_geography();

    CALL bl_cl.prc_load_ce_product_categories();
    CALL bl_cl.prc_load_ce_product_subcategories();
    CALL bl_cl.prc_load_ce_products();

    CALL bl_cl.prc_load_ce_employees();
    CALL bl_cl.prc_load_ce_employee_territory();

    CALL bl_cl.prc_load_ce_resellers();
    CALL bl_cl.prc_load_ce_stores();

    CALL bl_cl.prc_load_ce_customers_scd2();

    CALL bl_cl.prc_load_ce_sales_orders();
    CALL bl_cl.prc_load_ce_targets();

    CALL bl_cl.prc_write_log('bl_cl.prc_run_load_bl_3nf', 0, 'BL_3NF load completed');
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.prc_write_log('bl_cl.prc_run_load_bl_3nf', 0, SQLERRM);
        RAISE;
END;
$$;


-- 14) QUICK TESTS

    CALL bl_cl.prc_load_defaults_3nf();

    CALL bl_cl.prc_load_ce_seasons();
    CALL bl_cl.prc_load_ce_dates();

    CALL bl_cl.prc_load_ce_geography();

    CALL bl_cl.prc_load_ce_product_categories();
    CALL bl_cl.prc_load_ce_product_subcategories();
    CALL bl_cl.prc_load_ce_products();

    CALL bl_cl.prc_load_ce_employees();
    CALL bl_cl.prc_load_ce_employee_territory();

    CALL bl_cl.prc_load_ce_resellers();
    CALL bl_cl.prc_load_ce_stores();

    CALL bl_cl.prc_load_ce_customers_scd2();

    CALL bl_cl.prc_load_ce_sales_orders();
    CALL bl_cl.prc_load_ce_targets();

-- Run full load
CALL bl_cl.prc_run_load_bl_3nf();

-- Target duplicates (should be 0)
SELECT source_id, COUNT(*) FROM bl_3nf.ce_targets GROUP BY source_id HAVING COUNT(*) > 1;

-- Sales duplicates (should be 0)
SELECT sales_order_id, COUNT(*) FROM bl_3nf.ce_sales_orders GROUP BY sales_order_id HAVING COUNT(*) > 1;

-- Latest logs
SELECT * FROM bl_cl.mta_etl_log ORDER BY log_id DESC;