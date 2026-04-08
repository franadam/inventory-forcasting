CREATE TABLE IF NOT EXISTS bronze.locations (
    location_id             INT
    , location_code         VARCHAR(50)
    , location_name         VARCHAR(50)
    , location_type         VARCHAR(50)
    , address               VARCHAR(50)
    , city                  VARCHAR(50)
    , postal_code           VARCHAR(50)
    , region                VARCHAR(50)
    , storage_capacity_m3   DECIMAL(15,2)
    , is_active             BOOLEAN 
);

CREATE TABLE IF NOT EXISTS bronze.suppliers (
    supplier_id            INT
    , supplier_code        VARCHAR(50)
    , supplier_name        VARCHAR(50)
    , contact_name         VARCHAR(50)
    , email                VARCHAR(50)
    , country              VARCHAR(50)
    , avg_lead_time_days   DECIMAL(15,2)
    , reliability_score    DECIMAL(15,2)
    , is_active            BOOLEAN 
);

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id          INT
    , sku               VARCHAR(50)
    , product_name      VARCHAR(50)
    , category          VARCHAR(50)
    , subcategory       VARCHAR(50)
    , brand             VARCHAR(50)
    , unit_of_measure   VARCHAR(50)
    , unit_weight_kg    DECIMAL(15,2)
    , unit_price_eur    DECIMAL(15,2)
    , lead_time_days    INT
    , min_order_qty     DECIMAL(15,2)
    , is_active         BOOLEAN 
    , created_at        TIMESTAMP 
    , updated_at        TIMESTAMP 
);

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id         INT
    , customer_code     VARCHAR(50)
    , customer_name     VARCHAR(50)
    , customer_type     VARCHAR(50) 
    , address           VARCHAR(50)
    , city              VARCHAR(50)
    , postal_code       VARCHAR(50)
    , region            VARCHAR(50)
    , is_professional   BOOLEAN 
    , is_active         BOOLEAN 
);

CREATE TABLE IF NOT EXISTS bronze.product_suppliers (
    id                         INT
    , product_id               INT 
    , supplier_id              INT 
    , supplier_unit_cost_eur   DECIMAL(15,2)
    , supplier_lead_time_days  INT
    , is_preferred             BOOLEAN
);

CREATE TABLE IF NOT EXISTS bronze.inventory (
    inventory_id        INT
    , product_id        INT
    , location_id       INT
    , qty_on_hand       DECIMAL(15,2)
    , qty_reserved      DECIMAL(15,2)
    , min_stock_level   DECIMAL(15,2)
    , reorder_point     DECIMAL(15,2)
    , max_stock_level   DECIMAL(15,2)
    , last_upDATEd      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.sales_orders (
    order_id             INT
    , customer_id        INT
    , location_id        INT
    , order_ts           TIMESTAMP 
    , status             VARCHAR(50)
    , source             VARCHAR(50)
    , total_amount_eur   DECIMAL(15,2)
);

CREATE TABLE IF NOT EXISTS bronze.sales_order_lines (
    line_id              INT
    , order_id           INT 
    , product_id         INT 
    , qty_ordered        DECIMAL(15,2)
    , qty_fulfilled      DECIMAL(15,2)
    , unit_price_eur     DECIMAL(15,2)
    , line_total_eur     DECIMAL(15,2)
);

CREATE TABLE IF NOT EXISTS bronze.purchase_orders (
    po_id                 INT
    , supplier_id         INT
    , location_id         INT
    , created_at          TIMESTAMP
    , expected_delivery   DATE
    , actual_delivery     DATE
    , status              VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.purchase_order_lines (
    line_id               INT
    , po_id               INT
    , product_id          INT
    , qty_ordered         DECIMAL(15,2)
    , qty_received        DECIMAL(15,2)
    , unit_cost_eur       DECIMAL(15,2)
    , line_total_eur      DECIMAL(15,2)
);

CREATE TABLE IF NOT EXISTS bronze.inventory_movements (
    movement_id           INT
    , product_id          INT
    , location_id         INT
    , movement_type       VARCHAR(50) 
    , qty_delta           DECIMAL(15,2)
    , ref_order_id        INT
    , ref_order_type      VARCHAR(50)
    , movement_ts         TIMESTAMP
    , notes               VARCHAR(150)
);


