-- =============================================================================
-- OLTP SCHEMA DATA — Belgian Sanitary & Building Materials Distributor
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. SCHEMA CREATION
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS locations (
    location_id      SERIAL PRIMARY KEY,
    location_code    VARCHAR(10)  NOT NULL UNIQUE,
    location_name    VARCHAR(100) NOT NULL,
    location_type    VARCHAR(20)  NOT NULL CHECK (location_type IN ('sanicenter','regional_warehouse')),
    address          VARCHAR(200),
    city             VARCHAR(100),
    postal_code      VARCHAR(10),
    region           VARCHAR(50),
    storage_capacity_m3 NUMERIC(10,2),
    is_active        BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         SERIAL PRIMARY KEY,
    supplier_code       VARCHAR(10)  NOT NULL UNIQUE,
    supplier_name       VARCHAR(100) NOT NULL,
    contact_name        VARCHAR(100),
    email               VARCHAR(150),
    country             VARCHAR(50),
    avg_lead_time_days  NUMERIC(5,1),
    reliability_score   NUMERIC(3,2) CHECK (reliability_score BETWEEN 0 AND 1),
    is_active           BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS products (
    product_id       SERIAL PRIMARY KEY,
    sku              VARCHAR(20)  NOT NULL UNIQUE,
    product_name     VARCHAR(150) NOT NULL,
    category         VARCHAR(50),
    subcategory      VARCHAR(50),
    brand            VARCHAR(50),
    unit_of_measure  VARCHAR(20),
    unit_weight_kg   NUMERIC(8,3),
    unit_price_eur   NUMERIC(10,2),
    lead_time_days   INTEGER,
    min_order_qty    NUMERIC(8,2),
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    customer_code   VARCHAR(10)  NOT NULL UNIQUE,
    customer_name   VARCHAR(150) NOT NULL,
    customer_type   VARCHAR(30)  CHECK (customer_type IN ('plumber','contractor','architect','retailer','individual')),
    address         VARCHAR(200),
    city            VARCHAR(100),
    postal_code     VARCHAR(10),
    region          VARCHAR(50),
    is_professional BOOLEAN DEFAULT TRUE,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS product_suppliers (
    id                      SERIAL PRIMARY KEY,
    product_id              INTEGER NOT NULL REFERENCES products(product_id),
    supplier_id             INTEGER NOT NULL REFERENCES suppliers(supplier_id),
    supplier_unit_cost_eur  NUMERIC(10,2),
    supplier_lead_time_days INTEGER,
    is_preferred            BOOLEAN DEFAULT FALSE,
    UNIQUE (product_id, supplier_id)
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    SERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    location_id     INTEGER NOT NULL REFERENCES locations(location_id),
    qty_on_hand     NUMERIC(10,2) DEFAULT 0,
    qty_reserved    NUMERIC(10,2) DEFAULT 0,
    min_stock_level NUMERIC(10,2) DEFAULT 0,
    reorder_point   NUMERIC(10,2) DEFAULT 0,
    max_stock_level NUMERIC(10,2) DEFAULT 0,
    last_updated    TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_id, location_id)
);

CREATE TABLE IF NOT EXISTS sales_orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    location_id      INTEGER NOT NULL REFERENCES locations(location_id),
    order_ts         TIMESTAMP NOT NULL,
    status           VARCHAR(20) CHECK (status IN ('pending','fulfilled','partial','cancelled')),
    source           VARCHAR(20) CHECK (source IN ('counter','phone','online','erp_import')),
    total_amount_eur NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS sales_order_lines (
    line_id         SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES sales_orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    qty_ordered     NUMERIC(10,2),
    qty_fulfilled   NUMERIC(10,2),
    unit_price_eur  NUMERIC(10,2),
    line_total_eur  NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id              SERIAL PRIMARY KEY,
    supplier_id        INTEGER NOT NULL REFERENCES suppliers(supplier_id),
    location_id        INTEGER NOT NULL REFERENCES locations(location_id),
    created_at         TIMESTAMP NOT NULL,
    expected_delivery  DATE,
    actual_delivery    DATE,
    status             VARCHAR(20) CHECK (status IN ('draft','sent','confirmed','received','partial','cancelled')),
    total_cost_eur     NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS purchase_order_lines (
    line_id        SERIAL PRIMARY KEY,
    po_id          INTEGER NOT NULL REFERENCES purchase_orders(po_id),
    product_id     INTEGER NOT NULL REFERENCES products(product_id),
    qty_ordered    NUMERIC(10,2),
    qty_received   NUMERIC(10,2),
    unit_cost_eur  NUMERIC(10,2),
    line_total_eur NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS inventory_movements (
    movement_id    SERIAL PRIMARY KEY,
    product_id     INTEGER NOT NULL REFERENCES products(product_id),
    location_id    INTEGER NOT NULL REFERENCES locations(location_id),
    movement_type  VARCHAR(20) CHECK (movement_type IN ('sale','po_receipt','adjustment','transfer','initial_stock')),
    qty_delta      NUMERIC(10,2) NOT NULL,
    ref_order_id   INTEGER,
    ref_order_type VARCHAR(20),
    movement_ts    TIMESTAMP NOT NULL,
    notes          TEXT
);
