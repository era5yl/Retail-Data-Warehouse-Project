-- 1) PARTITIONED FACT TABLE (MONTHLY RANGE by sales_dt)




-- 2) PARTITION CREATION PROCEDURE 
--    Creates partitions for month starts between p_month_from and p_month_to.

CREATE OR REPLACE PROCEDURE bl_cl.prc_ensure_part_fct_sales_orders
(
  p_month_from DATE,   -- any date inside first month, or month start
  p_month_to   DATE    -- any date inside last month, or month start
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc      VARCHAR(200) := 'bl_cl.prc_ensure_part_fct_sales_orders';
  v_m         DATE;
  v_next      DATE;
  v_part      TEXT;
  v_sql       TEXT;
  v_rows      INT := 0;
BEGIN
  IF p_month_from IS NULL OR p_month_to IS NULL THEN
    RAISE EXCEPTION 'p_month_from and p_month_to must be not null';
  END IF;

  -- normalize to month starts
  p_month_from := date_trunc('month', p_month_from)::date;
  p_month_to   := date_trunc('month', p_month_to)::date;

  IF p_month_from > p_month_to THEN
    RAISE EXCEPTION 'p_month_from (%) cannot be > p_month_to (%)', p_month_from, p_month_to;
  END IF;

  -- Cursor-style loop (FOR ... IN SELECT ...) satisfies cursor requirement in PL/pgSQL
  FOR v_m IN
    SELECT gs::date
    FROM generate_series(p_month_from, p_month_to, interval '1 month') gs
  LOOP
    v_next := (v_m + interval '1 month')::date;
    v_part := format('fct_sales_orders_%s', to_char(v_m,'YYYYMM'));

    v_sql := format(
      'CREATE TABLE IF NOT EXISTS bl_dm.%I
       PARTITION OF bl_dm.fct_sales_orders
       FOR VALUES FROM (%L) TO (%L);',
      v_part, v_m, v_next
    );

    -- EXECUTE IMMEDIATE
    EXECUTE v_sql;
    v_rows := v_rows + 1;
  END LOOP;

  CALL bl_cl.prc_write_log(v_proc, v_rows,
    'Ensured partitions for '||p_month_from||' .. '||p_month_to);

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;






-- Bootstrapping
CREATE OR REPLACE PROCEDURE bl_cl.prc_bootstrap_part_fct_sales_orders()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_bootstrap_part_fct_sales_orders';
  v_min  DATE;
  v_max  DATE;
BEGIN
  SELECT date_trunc('month', MIN(full_date))::date,
         date_trunc('month', MAX(full_date))::date
    INTO v_min, v_max
  FROM bl_dm.dim_dates
  WHERE date_surr_id <> -1;

  IF v_min IS NULL OR v_max IS NULL THEN
    RAISE EXCEPTION 'dim_dates has no real dates (only default?)';
  END IF;

  CALL bl_cl.prc_ensure_part_fct_sales_orders(v_min, v_max);
  CALL bl_cl.prc_write_log(v_proc, 1, 'Bootstrap partitions: '||v_min||'..'||v_max);

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;





/* 
   4) FACT LOAD PROCEDURE UPDATED FOR PARTITIONS
*/

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_fct_sales_orders()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   VARCHAR(200) := 'bl_cl.prc_load_fct_sales_orders';
  v_rows   INT := 0;
  v_min_dt DATE;
  v_max_dt DATE;
BEGIN

  SELECT
    date_trunc('month', MIN(dd.full_date))::date,
    date_trunc('month', MAX(dd.full_date))::date
  INTO v_min_dt, v_max_dt
  FROM bl_3nf.ce_sales_orders s
  LEFT JOIN bl_dm.dim_dates dd
         ON dd.date_3nf_id = COALESCE(s.date_surr_id, -1)
  WHERE NOT EXISTS (
    SELECT 1
    FROM bl_dm.fct_sales_orders f
    WHERE f.sales_order_id = s.sales_order_id
  );

  -- If nothing new, v_min_dt will be null
  IF v_min_dt IS NOT NULL AND v_max_dt IS NOT NULL THEN
    CALL bl_cl.prc_ensure_part_fct_sales_orders(v_min_dt, v_max_dt);
  END IF;

  INSERT INTO bl_dm.fct_sales_orders
  (sales_order_surr_id, sales_order_id, sales_dt,
   date_surr_id, customer_surr_id, product_surr_id, employee_surr_id,
   geography_surr_id, reseller_surr_id, store_surr_id,
   sales_amount, cost, profit, quantity, unit_price,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_fct_sales_orders'),
    s.sales_order_id,

    -- partition key: prefer actual date from dim_dates, else default
    COALESCE(dd.full_date, DATE '1900-01-01') AS sales_dt,

    COALESCE(dd.date_surr_id, -1),
    COALESCE(dc.customer_surr_id, -1),
    COALESCE(dp.product_surr_id, -1),
    COALESCE(de.employee_surr_id, -1),
    COALESCE(dg.geography_surr_id, -1),
    COALESCE(dr.reseller_surr_id, -1),
    COALESCE(ds.store_surr_id, -1),

    s.sales_amount,
    s.cost,
    (COALESCE(s.sales_amount,0) - COALESCE(s.cost,0)) AS profit,
    s.quantity,
    s.unit_price,

    now(), now(),
    'BL_3NF','CE_SALES_ORDERS',
    COALESCE(s.source_id, s.sales_order_id)
  FROM bl_3nf.ce_sales_orders s
  LEFT JOIN bl_dm.dim_dates     dd ON dd.date_3nf_id      = COALESCE(s.date_surr_id, -1)
  LEFT JOIN bl_dm.dim_customers dc ON dc.customer_3nf_id  = COALESCE(s.customer_surr_id, -1)
  LEFT JOIN bl_dm.dim_products  dp ON dp.product_3nf_id   = COALESCE(s.product_surr_id, -1)
  LEFT JOIN bl_dm.dim_employees de ON de.employee_3nf_id  = COALESCE(s.employee_surr_id, -1)
  LEFT JOIN bl_dm.dim_geography dg ON dg.geography_3nf_id = COALESCE(s.geography_surr_id, -1)
  LEFT JOIN bl_dm.dim_resellers dr ON dr.reseller_3nf_id  = COALESCE(s.reseller_surr_id, -1)
  LEFT JOIN bl_dm.dim_stores    ds ON ds.store_3nf_id     = COALESCE(s.store_surr_id, -1)
  WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.fct_sales_orders f
    WHERE f.sales_order_id = s.sales_order_id
  );

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'FCT_SALES_ORDERS inserted into partitions');

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/*
   DIM_GEOGRAPHY (SCD1 MERGE)
    */
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_geography_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_geography_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_geography
  (geography_surr_id, geography_3nf_id, city, state, country, geography_type,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_geography') AS geography_surr_id,
    g.geography_surr_id               AS geography_3nf_id,
    COALESCE(g.city,'n.a.'),
    COALESCE(g.state,'n.a.'),
    COALESCE(g.country,'n.a.'),
    COALESCE(g.geography_type,'n.a.'),
    now(), now(),
    'BL_3NF','CE_GEOGRAPHY',
    COALESCE(g.source_id, ('GEO|'||g.geography_surr_id::varchar))
  FROM bl_3nf.ce_geography g
  WHERE g.geography_surr_id <> -1

  ON CONFLICT (geography_3nf_id) DO UPDATE
    SET city           = EXCLUDED.city,
        state          = EXCLUDED.state,
        country        = EXCLUDED.country,
        geography_type = EXCLUDED.geography_type,
        update_dt      = now(),
        source_system  = EXCLUDED.source_system,
        source_entity  = EXCLUDED.source_entity,
        source_id      = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_GEOGRAPHY SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/* 
   DIM_DATES (SCD1 MERGE)
 */
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_dates_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_dates_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_dates
  (date_surr_id, date_3nf_id, full_date, quarter, season_3nf_id,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_dates') AS date_surr_id,
    d.date_surr_id                AS date_3nf_id,
    COALESCE(d.full_date_label, DATE '1900-01-01') AS full_date,
    COALESCE(d.quarter,'n.a.'),
    COALESCE(d.season_surr_id, -1) AS season_3nf_id,
    now(), now(),
    'BL_3NF','CE_DATES',
    COALESCE(d.source_id, ('DATE|'||d.date_surr_id::varchar))
  FROM bl_3nf.ce_dates d
  WHERE d.date_surr_id <> -1

  ON CONFLICT (date_3nf_id) DO UPDATE
    SET full_date     = EXCLUDED.full_date,
        quarter       = EXCLUDED.quarter,
        season_3nf_id = EXCLUDED.season_3nf_id,
        update_dt     = now(),
        source_system = EXCLUDED.source_system,
        source_entity = EXCLUDED.source_entity,
        source_id     = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_DATES SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/*
   DIM_PRODUCTS (SCD1 MERGE)
   NK: product_3nf_id
 */
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_products_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_products_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_products
  (product_surr_id, product_3nf_id,
   product_description, manufacturer, brand_name, class_name, style_name,
   size, weight, stock_type, status_product,
   product_subcategory_label, product_subcategory_description, product_category_description,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_products') AS product_surr_id,
    p.product_surr_id                AS product_3nf_id,
    COALESCE(p.product_description,'n.a.'),
    COALESCE(p.manufacturer,'n.a.'),
    COALESCE(p.brand_name,'n.a.'),
    COALESCE(p.class_name,'n.a.'),
    COALESCE(p.style_name,'n.a.'),
    COALESCE(p.size,'n.a.'),
    p.weight,
    COALESCE(p.stock_type,'n.a.'),
    COALESCE(p.status_product,'n.a.'),
    COALESCE(ps.product_subcategory_label,'n.a.'),
    COALESCE(ps.product_subcategory_description,'n.a.'),
    COALESCE(pc.product_category_description,'n.a.'),
    now(), now(),
    'BL_3NF','CE_PRODUCTS',
    COALESCE(p.source_id, ('PROD|'||p.product_surr_id::varchar))
  FROM bl_3nf.ce_products p
  LEFT JOIN bl_3nf.ce_product_subcategories ps
         ON ps.product_subcategory_surr_id = p.product_subcategory_surr_id
  LEFT JOIN bl_3nf.ce_product_categories pc
         ON pc.product_category_surr_id = ps.product_category_surr_id
  WHERE p.product_surr_id <> -1

  ON CONFLICT (product_3nf_id) DO UPDATE
    SET product_description             = EXCLUDED.product_description,
        manufacturer                    = EXCLUDED.manufacturer,
        brand_name                      = EXCLUDED.brand_name,
        class_name                      = EXCLUDED.class_name,
        style_name                      = EXCLUDED.style_name,
        size                            = EXCLUDED.size,
        weight                          = EXCLUDED.weight,
        stock_type                      = EXCLUDED.stock_type,
        status_product                  = EXCLUDED.status_product,
        product_subcategory_label       = EXCLUDED.product_subcategory_label,
        product_subcategory_description = EXCLUDED.product_subcategory_description,
        product_category_description    = EXCLUDED.product_category_description,
        update_dt                       = now(),
        source_system                   = EXCLUDED.source_system,
        source_entity                   = EXCLUDED.source_entity,
        source_id                       = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_PRODUCTS SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/*
   DIM_RESELLERS (SCD1 MERGE)
   NK: reseller_3nf_id
 */
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_resellers_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_resellers_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_resellers
  (reseller_surr_id, reseller_3nf_id,
   reseller_name, reseller_business_type, geography_surr_id,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_resellers') AS reseller_surr_id,
    r.reseller_surr_id                AS reseller_3nf_id,
    COALESCE(r.reseller_name,'n.a.'),
    COALESCE(r.reseller_business_type,'n.a.'),
    COALESCE(gd.geography_surr_id, -1),
    now(), now(),
    'BL_3NF','CE_RESELLERS',
    COALESCE(r.source_id, ('RESELLER|'||r.reseller_surr_id::varchar))
  FROM bl_3nf.ce_resellers r
  LEFT JOIN bl_dm.dim_geography gd
         ON gd.geography_3nf_id = COALESCE(r.geography_surr_id, -1)
  WHERE r.reseller_surr_id <> -1

  ON CONFLICT (reseller_3nf_id) DO UPDATE
    SET reseller_name          = EXCLUDED.reseller_name,
        reseller_business_type = EXCLUDED.reseller_business_type,
        geography_surr_id      = EXCLUDED.geography_surr_id,
        update_dt              = now(),
        source_system          = EXCLUDED.source_system,
        source_entity          = EXCLUDED.source_entity,
        source_id              = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_RESELLERS SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/* 
   DIM_EMPLOYEES (SCD1 MERGE)
   NK: employee_3nf_id
*/
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_employees_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_employees_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_employees
  (employee_surr_id, employee_3nf_id,
   employee_business_id, name, title, email,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_employees') AS employee_surr_id,
    e.employee_surr_id                AS employee_3nf_id,
    COALESCE(e.employee_business_id,'n.a.'),
    COALESCE(e.name,'n.a.'),
    COALESCE(e.title,'n.a.'),
    COALESCE(e.email,'n.a.'),
    now(), now(),
    'BL_3NF','CE_EMPLOYEES',
    COALESCE(e.source_id, ('EMP|'||e.employee_surr_id::varchar))
  FROM bl_3nf.ce_employees e
  WHERE e.employee_surr_id <> -1

  ON CONFLICT (employee_3nf_id) DO UPDATE
    SET employee_business_id = EXCLUDED.employee_business_id,
        name                = EXCLUDED.name,
        title               = EXCLUDED.title,
        email               = EXCLUDED.email,
        update_dt           = now(),
        source_system       = EXCLUDED.source_system,
        source_entity       = EXCLUDED.source_entity,
        source_id           = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_EMPLOYEES SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


/*
   DIM_STORES (SCD1 MERGE)
   NK: store_3nf_id
   */
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_stores_scd1_merge()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_stores_scd1_merge';
  v_rows INT := 0;
BEGIN
  INSERT INTO bl_dm.dim_stores
  (store_surr_id, store_3nf_id,
   store_key, store_status, store_parent_entity,
   entity_description, entity_type, entity_status,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_stores') AS store_surr_id,
    s.store_surr_id                AS store_3nf_id,
    COALESCE(s.store_key,'n.a.'),
    COALESCE(s.store_status,'n.a.'),
    COALESCE(s.store_parent_entity,'n.a.'),
    COALESCE(s.entity_description,'n.a.'),
    COALESCE(s.entity_type,'n.a.'),
    COALESCE(s.entity_status,'n.a.'),
    now(), now(),
    'BL_3NF','CE_STORES',
    COALESCE(s.source_id, ('STORE|'||s.store_surr_id::varchar))
  FROM bl_3nf.ce_stores s
  WHERE s.store_surr_id <> -1

  ON CONFLICT (store_3nf_id) DO UPDATE
    SET store_key           = EXCLUDED.store_key,
        store_status        = EXCLUDED.store_status,
        store_parent_entity = EXCLUDED.store_parent_entity,
        entity_description  = EXCLUDED.entity_description,
        entity_type         = EXCLUDED.entity_type,
        entity_status       = EXCLUDED.entity_status,
        update_dt           = now(),
        source_system       = EXCLUDED.source_system,
        source_entity       = EXCLUDED.source_entity,
        source_id           = EXCLUDED.source_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_STORES SCD1 MERGE done');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;






-- 1) CUSTOMER DIMENSION (SCD2 in DM)

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_customers_scd2_dm()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_dim_customers_scd2_dm';
  v_rows INT := 0;
  v_rc   INT := 0;
BEGIN
  -- 1) Close changed active rows
  UPDATE bl_dm.dim_customers_scd2_dm d
     SET end_dt    = c.start_dt,
         is_active = 'N',
         update_dt = now()
  FROM bl_3nf.ce_customers_scd2 c
  WHERE c.is_active = 'Y'
    AND d.is_active = 'Y'
    AND d.customer_3nf_id = c.customer_surr_id
    AND (
         COALESCE(d.customer_label,'n.a.') <> COALESCE(c.customer_label,'n.a.')
      OR COALESCE(d.customer_name,'n.a.')  <> COALESCE(c.customer_name,'n.a.')
      OR COALESCE(d.birth_date, DATE '1900-01-01') <> COALESCE(c.birth_date, DATE '1900-01-01')
      OR COALESCE(d.gender,'n.a.')         <> COALESCE(c.gender,'n.a.')
      OR COALESCE(d.education,'n.a.')      <> COALESCE(c.education,'n.a.')
      OR COALESCE(d.occupation,'n.a.')     <> COALESCE(c.occupation,'n.a.')
      OR COALESCE(d.yearly_income,-1)      <> COALESCE(c.yearly_income,-1)
      OR COALESCE(d.total_children,-1)     <> COALESCE(c.total_children,-1)
      OR COALESCE(d.number_children_at_home,-1) <> COALESCE(c.number_children_at_home,-1)
      OR COALESCE(d.house_owner_flag,'n.a.')<> COALESCE(c.house_owner_flag,'n.a.')
      OR COALESCE(d.number_cars_owned,-1)  <> COALESCE(c.number_cars_owned,-1)
    );

  GET DIAGNOSTICS v_rc = ROW_COUNT;
  v_rows := v_rows + v_rc;

  -- 2) Insert brand new customers OR new versions after close
  INSERT INTO bl_dm.dim_customers_scd2_dm
  (customer_surr_id, customer_3nf_id,
   customer_label, customer_name, birth_date, gender, education, occupation,
   yearly_income, total_children, number_children_at_home, house_owner_flag, number_cars_owned,
   start_dt, end_dt, is_active,
   insert_dt, update_dt, source_system, source_entity, source_id)
  SELECT
    nextval('bl_dm.seq_dim_customers') AS customer_surr_id,
    c.customer_surr_id                AS customer_3nf_id,

    COALESCE(c.customer_label,'n.a.'),
    COALESCE(c.customer_name,'n.a.'),
    c.birth_date,
    COALESCE(c.gender,'n.a.'),
    COALESCE(c.education,'n.a.'),
    COALESCE(c.occupation,'n.a.'),
    c.yearly_income,
    c.total_children,
    c.number_children_at_home,
    COALESCE(c.house_owner_flag,'n.a.'),
    c.number_cars_owned,

    COALESCE(c.start_dt, CURRENT_DATE)                 AS start_dt,
    COALESCE(c.end_dt,   DATE '9999-12-31')            AS end_dt,
    'Y'                                                AS is_active,

    now(), now(),
    'BL_3NF','CE_CUSTOMERS_SCD2',
    COALESCE(c.source_id, ('CUST|'||c.customer_surr_id::varchar||'|'||c.start_dt::varchar))
  FROM bl_3nf.ce_customers_scd2 c
  WHERE c.is_active = 'Y'
    AND c.customer_surr_id <> -1
    AND NOT EXISTS (
      SELECT 1
      FROM bl_dm.dim_customers_scd2_dm d
      WHERE d.customer_3nf_id = c.customer_surr_id
        AND d.is_active = 'Y'
        AND COALESCE(d.customer_label,'n.a.') = COALESCE(c.customer_label,'n.a.')
        AND COALESCE(d.customer_name,'n.a.')  = COALESCE(c.customer_name,'n.a.')
        AND COALESCE(d.birth_date, DATE '1900-01-01') = COALESCE(c.birth_date, DATE '1900-01-01')
        AND COALESCE(d.gender,'n.a.')         = COALESCE(c.gender,'n.a.')
        AND COALESCE(d.education,'n.a.')      = COALESCE(c.education,'n.a.')
        AND COALESCE(d.occupation,'n.a.')     = COALESCE(c.occupation,'n.a.')
        AND COALESCE(d.yearly_income,-1)      = COALESCE(c.yearly_income,-1)
        AND COALESCE(d.total_children,-1)     = COALESCE(c.total_children,-1)
        AND COALESCE(d.number_children_at_home,-1) = COALESCE(c.number_children_at_home,-1)
        AND COALESCE(d.house_owner_flag,'n.a.')= COALESCE(c.house_owner_flag,'n.a.')
        AND COALESCE(d.number_cars_owned,-1)  = COALESCE(c.number_cars_owned,-1)
    );

  GET DIAGNOSTICS v_rc = ROW_COUNT;
  v_rows := v_rows + v_rc;

  CALL bl_cl.prc_write_log(v_proc, v_rows, 'DIM_CUSTOMERS SCD2 DM load done');

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;




-- 2) DM RUNNER 


CREATE OR REPLACE PROCEDURE bl_cl.prc_run_load_bl_dm()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_run_load_bl_dm';
BEGIN
  CALL bl_cl.prc_write_log(v_proc, 0, 'DM load started');

  CALL bl_cl.prc_load_defaults_dm();

  -- SCD1 (MERGE/UPSERT) for all non-customer dims
  CALL bl_cl.prc_load_dim_geography_scd1_merge();
  CALL bl_cl.prc_load_dim_dates_scd1_merge();
  CALL bl_cl.prc_load_dim_products_scd1_merge();
  CALL bl_cl.prc_load_dim_resellers_scd1_merge();
  CALL bl_cl.prc_load_dim_employees_scd1_merge();
  CALL bl_cl.prc_load_dim_stores_scd1_merge();

  -- SCD2 customers in DM
  CALL bl_cl.prc_load_dim_customers_scd2_dm();

 
  CALL bl_cl.prc_load_fct_sales_orders();
  CALL bl_cl.prc_load_fct_targets();

  CALL bl_cl.prc_write_log(v_proc, 0, 'DM load finished successfully');
EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;


-- checking

  CALL bl_cl.prc_load_defaults_dm();

  -- SCD1 (MERGE/UPSERT) for all non-customer dims
  CALL bl_cl.prc_load_dim_geography_scd1_merge();
  CALL bl_cl.prc_load_dim_dates_scd1_merge();
  CALL bl_cl.prc_load_dim_products_scd1_merge();
  CALL bl_cl.prc_load_dim_resellers_scd1_merge();
  CALL bl_cl.prc_load_dim_employees_scd1_merge();
  CALL bl_cl.prc_load_dim_stores_scd1_merge();

  -- SCD2 customers in DM
  CALL bl_cl.prc_load_dim_customers_scd2_dm();
  
  CALL bl_cl.prc_load_fct_sales_orders();
  CALL bl_cl.prc_load_fct_targets();
  
  -- all
  call bl_cl.prc_run_load_bl_dm()

