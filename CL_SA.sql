/* 
   SA procedures (per source) with INPUT = source file link
 */

CREATE SCHEMA IF NOT EXISTS bl_cl;

-- 1) AdventureWorks: set EXT file + load EXT -> SRC

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_sa_adventureworks
(
  p_source_file  TEXT,            
  p_is_full      BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_sa_adventureworks';
  v_rows INT := 0;
  v_sql  TEXT;
BEGIN
  IF p_source_file IS NULL OR btrim(p_source_file) = '' THEN
    RAISE EXCEPTION 'p_source_file must be provided';
  END IF;

  CALL bl_cl.prc_write_log(v_proc, 0, 'SA AW start. file='||p_source_file||', full='||p_is_full);

  -- 1) "Source -> EXT": point foreign table to the provided file
  v_sql := format(
    'ALTER FOREIGN TABLE sa_adventureworks.ext_adventureworks_sales OPTIONS (SET filename %L);',
    p_source_file
  );
  EXECUTE v_sql;


  IF p_is_full THEN
    TRUNCATE TABLE sa_adventureworks.src_sales;
    CALL bl_cl.prc_write_log(v_proc, 0, 'AW SRC truncated (full reload)');
  END IF;

  -- 2) EXT -> SRC (typed + dedup + restartable incremental)
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
  FROM sa_adventureworks.ext_adventureworks_sales e
  WHERE nullif(trim(e.sales_order_number),'') IS NOT NULL
    AND nullif(e.sales_order_date,'') IS NOT NULL

    AND NOT EXISTS (
      SELECT 1
      FROM sa_adventureworks.src_sales s
      WHERE s.sales_order_number = trim(e.sales_order_number)
        AND s.sales_order_date   = to_date(e.sales_order_date, 'YYYY-MM-DD')
        AND COALESCE(s.product_key,  -999999) = COALESCE(nullif(e.product_key,'')::int, -999999)
        AND COALESCE(s.reseller_key, -999999) = COALESCE(nullif(e.reseller_key,'')::int, -999999)
        AND COALESCE(s.employee_id,  'n.a.')  = COALESCE(nullif(trim(e.employee_id),''), 'n.a.')
    );

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'SA AW EXT->SRC loaded');

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;




-- 2) Contoso: set EXT file + load EXT -> SRC (dedup by OnlineSalesKey)

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_sa_contoso_cb
(
  p_source_file  TEXT,        
  p_is_full      BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc VARCHAR(200) := 'bl_cl.prc_load_sa_contoso_cb';
  v_rows INT := 0;
  v_sql  TEXT;
BEGIN
  IF p_source_file IS NULL OR btrim(p_source_file) = '' THEN
    RAISE EXCEPTION 'p_source_file must be provided';
  END IF;

  CALL bl_cl.prc_write_log(v_proc, 0, 'SA Contoso start. file='||p_source_file||', full='||p_is_full);

  -- 1) "Source -> EXT": point foreign table to the provided file
  v_sql := format(
    'ALTER FOREIGN TABLE sa_contoso.ext_contoso_cb OPTIONS (SET filename %L);',
    p_source_file
  );
  EXECUTE v_sql;


  IF p_is_full THEN
    TRUNCATE TABLE sa_contoso.src_online_sales;
    CALL bl_cl.prc_write_log(v_proc, 0, 'Contoso SRC truncated (full reload)');
  END IF;

  -- 2) EXT -> SRC (typed + dedup + restartable incremental)
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

          'contoso_cb'                                      AS source_entity
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
      d.online_sales_key, d.sales_amount, d.sales_order_number, d.full_date,
      d.customer_label, d.quarter_label,
      d.birth_date, d.gender, d.education, d.occupation,
      d.status_store, d.entity_key, d.parent_entity_label, d.entity_description, d.entity_type, d.status_entity,
      d.geography_type, d.continent_name, d.city_name, d.state_province_name, d.region_country_name,
      d.product_description, d.manufacturer, d.brand_name, d.class_name, d.style_name, d.size_txt, d.weight_num,
      d.stock_type_name, d.unit_cost, d.unit_price, d.status_product,
      d.product_subcategory_label, d.product_subcategory_description, d.product_category_description,
      d.customer_name, d.europe_season, d.north_america_season, d.asia_season,
      d.yearly_income, d.total_children, d.number_children_at_home, d.house_owner_flag, d.number_cars_owned, d.date_first_purchase,
      d.source_entity
  FROM dedup d
  WHERE NOT EXISTS (
      SELECT 1
      FROM sa_contoso.src_online_sales s
      WHERE s.online_sales_key = d.online_sales_key
  );

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  CALL bl_cl.prc_write_log(v_proc, v_rows, 'SA Contoso EXT->SRC loaded');

EXCEPTION
  WHEN OTHERS THEN
    CALL bl_cl.prc_write_log(v_proc, 0, 'ERROR: '||SQLERRM);
    RAISE;
END;
$$;




-- Initial load:
CALL bl_cl.prc_load_sa_adventureworks('/tmp/adventureworks.csv', TRUE);
CALL bl_cl.prc_load_sa_contoso_cb('/tmp/contoso_cb.csv', TRUE);
--
-- Incremental demo (no truncate):
CALL bl_cl.prc_load_sa_adventureworks('/tmp/adventureworks_inc.csv', FALSE);
CALL bl_cl.prc_load_sa_contoso_cb('/tmp/contoso_cb_inc.csv', FALSE);
-- =========================================================