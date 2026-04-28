-- =========================================================
-- 1) DROP FOREIGN KEYS ON FACT TABLES
-- =========================================================

-- fact_sales_line
ALTER TABLE IF EXISTS gold.fact_sales_line
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_product,
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_customer,
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_location,
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_sales_channel,
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_sales_status,
DROP CONSTRAINT IF EXISTS fk_fact_sales_line_dim_date;

-- fact_inventory_snapshot
ALTER TABLE IF EXISTS gold.fact_inventory_snapshot
DROP CONSTRAINT IF EXISTS fk_fact_inventory_snapshot_dim_product,
DROP CONSTRAINT IF EXISTS fk_fact_inventory_snapshot_dim_location,
DROP CONSTRAINT IF EXISTS fk_fact_inventory_snapshot_dim_date;

-- fact_purchase_line
ALTER TABLE IF EXISTS gold.fact_purchase_line
DROP CONSTRAINT IF EXISTS fk_fact_purchase_dim_product,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_dim_location,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_dim_supplier,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_dim_purchase_status,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_expected_delivery_date,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_created_at_date,
DROP CONSTRAINT IF EXISTS fk_fact_purchase_actual_delivery_date;

-- =========================================================
-- 2) TRUNCATE TABLES
-- =========================================================
-- Facts first, then dimensions
-- RESTART IDENTITY resets GENERATED ALWAYS AS IDENTITY sequences

TRUNCATE TABLE gold.fact_sales_line,
gold.fact_inventory_snapshot,
gold.fact_purchase_line,
gold.dim_customer,
gold.dim_product,
gold.dim_location,
gold.dim_supplier,
gold.dim_date,
gold.dim_sales_channel,
gold.dim_sales_status,
gold.dim_purchase_status
RESTART IDENTITY;

-- =========================================================
-- 3) RECREATE FOREIGN KEYS
-- =========================================================

-- fact_sales_line
ALTER TABLE gold.fact_sales_line
ADD CONSTRAINT fk_fact_sales_line_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
ADD CONSTRAINT fk_fact_sales_line_dim_customer FOREIGN KEY (sk_dim_customer) REFERENCES gold.dim_customer (sk_dim_customer),
ADD CONSTRAINT fk_fact_sales_line_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
ADD CONSTRAINT fk_fact_sales_line_dim_sales_channel FOREIGN KEY (sk_dim_sales_channel) REFERENCES gold.dim_sales_channel (sk_dim_sales_channel),
ADD CONSTRAINT fk_fact_sales_line_dim_sales_status FOREIGN KEY (sk_dim_sales_status) REFERENCES gold.dim_sales_status (sk_dim_sales_status),
ADD CONSTRAINT fk_fact_sales_line_dim_date FOREIGN KEY (date_key) REFERENCES gold.dim_date (date_key);

-- fact_inventory_snapshot
ALTER TABLE gold.fact_inventory_snapshot
ADD CONSTRAINT fk_fact_inventory_snapshot_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
ADD CONSTRAINT fk_fact_inventory_snapshot_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
ADD CONSTRAINT fk_fact_inventory_snapshot_dim_date FOREIGN KEY (last_updated_date_key) REFERENCES gold.dim_date (date_key);

-- fact_purchase_line
ALTER TABLE gold.fact_purchase_line
ADD CONSTRAINT fk_fact_purchase_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
ADD CONSTRAINT fk_fact_purchase_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
ADD CONSTRAINT fk_fact_purchase_dim_supplier FOREIGN KEY (sk_dim_supplier) REFERENCES gold.dim_supplier (sk_dim_supplier),
ADD CONSTRAINT fk_fact_purchase_dim_purchase_status FOREIGN KEY (sk_dim_purchase_status) REFERENCES gold.dim_purchase_status (sk_dim_purchase_status),
ADD CONSTRAINT fk_fact_purchase_expected_delivery_date FOREIGN KEY (expected_delivery_date_key) REFERENCES gold.dim_date (date_key),
ADD CONSTRAINT fk_fact_purchase_created_at_date FOREIGN KEY (created_at_date_key) REFERENCES gold.dim_date (date_key),
ADD CONSTRAINT fk_fact_purchase_actual_delivery_date FOREIGN KEY (actual_delivery_date_key) REFERENCES gold.dim_date (date_key);

-- =========================================================
-- 4) DROP INDEXES
-- =========================================================

-- dimentions
DROP INDEX IF EXISTS gold.idx_dim_customer_id_source;

DROP INDEX IF EXISTS gold.idx_dim_product_id_source;

DROP INDEX IF EXISTS gold.idx_dim_location_id_source;

DROP INDEX IF EXISTS gold.idx_dim_supplier_id_source;

DROP INDEX IF EXISTS gold.idx_dim_date;

DROP INDEX IF EXISTS gold.idx_dim_sales_channel_source_name;

DROP INDEX IF EXISTS gold.idx_dim_sales_status_name;

DROP INDEX IF EXISTS gold.idx_dim_purchase_status_name;

-- fact_sales_line
DROP INDEX IF EXISTS gold.idx_fact_sales_line_sk_dim_customer;

DROP INDEX IF EXISTS gold.idx_fact_sales_line_sk_dim_location;

DROP INDEX IF EXISTS gold.idx_fact_sales_line_sk_dim_product;

DROP INDEX IF EXISTS gold.idx_fact_sales_line_sk_dim_sales_channel;

DROP INDEX IF EXISTS gold.idx_fact_sales_line_sk_dim_sales_status;

DROP INDEX IF EXISTS gold.idx_fact_sales_line_date_key;

-- fact_purchase_line
DROP INDEX IF EXISTS gold.idx_fact_purchase_line_sk_dim_location;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_sk_dim_product;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_sk_dim_purchase_status;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_sk_dim_supplier;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_expected_delivery_date_key;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_created_at_date_key;

DROP INDEX IF EXISTS gold.idx_fact_purchase_line_actual_delivery_date_key;

-- fact_inventory_snapshot
DROP INDEX IF EXISTS gold.idx_fact_inventory_snapshot_sk_dim_location;

DROP INDEX IF EXISTS gold.idx_fact_inventory_snapshot_sk_dim_product;

DROP INDEX IF EXISTS gold.idx_fact_inventory_snapshot_last_updated_date_key;

-- =========================================================
-- 5) RECREATE INDEXES
-- =========================================================

-- dimentions
CREATE INDEX idx_dim_customer_id_source ON gold.dim_customer (customer_id_source);

CREATE INDEX idx_dim_product_id_source ON gold.dim_product (product_id_source);

CREATE INDEX idx_dim_location_id_source ON gold.dim_location (location_id_source);

CREATE INDEX idx_dim_supplier_id_source ON gold.dim_supplier (supplier_id_source);

CREATE INDEX idx_dim_date ON gold.dim_date (full_date);

CREATE INDEX idx_dim_sales_channel_source_name ON gold.dim_sales_channel (source_name);

CREATE INDEX idx_dim_sales_status_name ON gold.dim_sales_status (status_name);

CREATE INDEX idx_dim_purchase_status_name ON gold.dim_purchase_status (status_name);

-- fact_sales_line
CREATE INDEX idx_fact_sales_line_sk_dim_customer ON gold.fact_sales_line (sk_dim_customer);

CREATE INDEX idx_fact_sales_line_sk_dim_location ON gold.fact_sales_line (sk_dim_location);

CREATE INDEX idx_fact_sales_line_sk_dim_product ON gold.fact_sales_line (sk_dim_product);

CREATE INDEX idx_fact_sales_line_sk_dim_sales_channel ON gold.fact_sales_line (sk_dim_sales_channel);

CREATE INDEX idx_fact_sales_line_sk_dim_sales_status ON gold.fact_sales_line (sk_dim_sales_status);

CREATE INDEX idx_fact_sales_line_date_key ON gold.fact_sales_line (date_key);

-- fact_purchase_line
CREATE INDEX idx_fact_purchase_line_sk_dim_location ON gold.fact_purchase_line (sk_dim_location);

CREATE INDEX idx_fact_purchase_line_sk_dim_product ON gold.fact_purchase_line (sk_dim_product);

CREATE INDEX idx_fact_purchase_line_sk_dim_purchase_status ON gold.fact_purchase_line (sk_dim_purchase_status);

CREATE INDEX idx_fact_purchase_line_sk_dim_supplier ON gold.fact_purchase_line (sk_dim_supplier);

CREATE INDEX idx_fact_purchase_line_expected_delivery_date_key ON gold.fact_purchase_line (expected_delivery_date_key);

CREATE INDEX idx_fact_purchase_line_created_at_date_key ON gold.fact_purchase_line (created_at_date_key);

CREATE INDEX idx_fact_purchase_line_actual_delivery_date_key ON gold.fact_purchase_line (actual_delivery_date_key);

-- fact_inventory_snapshot
CREATE INDEX idx_fact_inventory_snapshot_sk_dim_location ON gold.fact_inventory_snapshot (sk_dim_location);

CREATE INDEX idx_fact_inventory_snapshot_sk_dim_product ON gold.fact_inventory_snapshot (sk_dim_product);

CREATE INDEX idx_fact_inventory_snapshot_last_updated_date_key ON gold.fact_inventory_snapshot (last_updated_date_key);