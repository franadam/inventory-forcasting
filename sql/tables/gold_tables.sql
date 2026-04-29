CREATE TABLE IF NOT EXISTS gold.dim_customer (
    sk_dim_customer INT GENERATED ALWAYS AS IDENTITY,
    customer_id_source VARCHAR(50) NOT NULL,
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(50) NOT NULL,
    customer_status VARCHAR(50) NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    province VARCHAR(100),
    region VARCHAR(100),
    CONSTRAINT pk_dim_customer PRIMARY KEY (sk_dim_customer),
    CONSTRAINT uq_dim_customer_id_source UNIQUE (customer_id_source),
    CONSTRAINT uq_dim_customer_code UNIQUE (customer_code)
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    sk_dim_product INT GENERATED ALWAYS AS IDENTITY,
    product_id_source VARCHAR(50) NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    product_status VARCHAR(50),
    unit_of_measure VARCHAR(50),
    unit_weight_kg NUMERIC(15, 2),
    lead_time_days INT,
    min_order_qty NUMERIC(15, 2),
    CONSTRAINT pk_dim_product PRIMARY KEY (sk_dim_product),
    CONSTRAINT uq_dim_product_id_source UNIQUE (product_id_source),
    CONSTRAINT uq_dim_product_sku UNIQUE (sku)
);

CREATE TABLE IF NOT EXISTS gold.dim_location (
    sk_dim_location INT GENERATED ALWAYS AS IDENTITY,
    location_id_source VARCHAR(50) NOT NULL,
    location_code VARCHAR(50) NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    location_type VARCHAR(50) NOT NULL,
    location_status VARCHAR(50) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    province VARCHAR(100),
    region VARCHAR(100),
    storage_capacity_m3 NUMERIC(15, 2),
    CONSTRAINT pk_dim_location PRIMARY KEY (sk_dim_location),
    CONSTRAINT uq_dim_location_id_source UNIQUE (location_id_source),
    CONSTRAINT uq_dim_location_code UNIQUE (location_code)
);

CREATE TABLE IF NOT EXISTS gold.dim_supplier (
    sk_dim_supplier INT GENERATED ALWAYS AS IDENTITY,
    supplier_id_source VARCHAR(50) NOT NULL,
    supplier_code VARCHAR(50) NOT NULL,
    supplier_name VARCHAR(100) NOT NULL,
    supplier_status VARCHAR(50) NOT NULL,
    contact_name VARCHAR(100),
    country VARCHAR(100),
    email VARCHAR(255),
    avg_lead_time_days NUMERIC(15, 2),
    reliability_score NUMERIC(15, 2),
    CONSTRAINT pk_dim_supplier PRIMARY KEY (sk_dim_supplier),
    CONSTRAINT uq_dim_supplier_id_source UNIQUE (supplier_id_source),
    CONSTRAINT uq_dim_supplier_code UNIQUE (supplier_code)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_month INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_week INT NOT NULL,
    week_of_year INT NOT NULL,
    day_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date)
);

CREATE TABLE IF NOT EXISTS gold.dim_sales_channel (
    sk_dim_sales_channel INT GENERATED ALWAYS AS IDENTITY,
    source_code VARCHAR(50) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    CONSTRAINT pk_dim_sales_channel PRIMARY KEY (sk_dim_sales_channel),
    CONSTRAINT uq_dim_sales_channel_source_code UNIQUE (source_code)
);

CREATE TABLE IF NOT EXISTS gold.dim_sales_status (
    sk_dim_sales_status INT GENERATED ALWAYS AS IDENTITY,
    status_code VARCHAR(50) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    CONSTRAINT pk_dim_sales_status PRIMARY KEY (sk_dim_sales_status),
    CONSTRAINT uq_dim_sales_status_code UNIQUE (status_code)
);

CREATE TABLE IF NOT EXISTS gold.dim_purchase_status (
    sk_dim_purchase_status INT GENERATED ALWAYS AS IDENTITY,
    status_code VARCHAR(50) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    CONSTRAINT pk_dim_purchase_status PRIMARY KEY (sk_dim_purchase_status),
    CONSTRAINT uq_dim_purchase_status_code UNIQUE (status_code)
);

CREATE TABLE IF NOT EXISTS gold.fact_sales_line (
    sk_fact_sales_line INT GENERATED ALWAYS AS IDENTITY,
    sk_dim_product INT NOT NULL,
    sk_dim_location INT NOT NULL,
    sk_dim_customer INT NOT NULL,
    sk_dim_sales_channel INT,
    sk_dim_sales_status INT,
    date_key INT NOT NULL,
    sales_order_line_id_dd INT NOT NULL,
    sales_order_id_dd INT NOT NULL,
    qty_ordered NUMERIC(15, 2),
    qty_fulfilled NUMERIC(15, 2),
    unit_price_eur NUMERIC(15, 2),
    line_sales_amount_eur NUMERIC(15, 2),
    CONSTRAINT pk_fact_sales_line PRIMARY KEY (sk_fact_sales_line),
    CONSTRAINT fk_fact_sales_line_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
    CONSTRAINT fk_fact_sales_line_dim_customer FOREIGN KEY (sk_dim_customer) REFERENCES gold.dim_customer (sk_dim_customer),
    CONSTRAINT fk_fact_sales_line_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
    CONSTRAINT fk_fact_sales_line_dim_sales_channel FOREIGN KEY (sk_dim_sales_channel) REFERENCES gold.dim_sales_channel (sk_dim_sales_channel),
    CONSTRAINT fk_fact_sales_line_dim_sales_status FOREIGN KEY (sk_dim_sales_status) REFERENCES gold.dim_sales_status (sk_dim_sales_status),
    CONSTRAINT fk_fact_sales_line_dim_date FOREIGN KEY (date_key) REFERENCES gold.dim_date (date_key)
);

CREATE TABLE IF NOT EXISTS gold.fact_inventory_snapshot (
    sk_fact_inventory_snapshot INT GENERATED ALWAYS AS IDENTITY,
    sk_dim_product INT NOT NULL,
    sk_dim_location INT NOT NULL,
    last_updated_date_key INT NOT NULL,
    inventory_id_dd INT NOT NULL,
    qty_on_hand NUMERIC(15, 2),
    qty_reserved NUMERIC(15, 2),
    min_stock_level NUMERIC(15, 2),
    max_stock_level NUMERIC(15, 2),
    reorder_point NUMERIC(15, 2),
    available_stock NUMERIC(15, 2),
    is_ruptured BOOLEAN,
    is_below_min BOOLEAN,
    is_below_reorder BOOLEAN,
    CONSTRAINT pk_fact_inventory_snapshot PRIMARY KEY (sk_fact_inventory_snapshot),
    CONSTRAINT fk_fact_inventory_snapshot_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
    CONSTRAINT fk_fact_inventory_snapshot_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
    CONSTRAINT fk_fact_inventory_snapshot_dim_date FOREIGN KEY (last_updated_date_key) REFERENCES gold.dim_date (date_key)
);

CREATE TABLE IF NOT EXISTS gold.fact_purchase_line (
    sk_fact_purchase INT GENERATED ALWAYS AS IDENTITY,
    sk_dim_product INT NOT NULL,
    sk_dim_location INT NOT NULL,
    sk_dim_supplier INT NOT NULL,
    sk_dim_purchase_status INT NOT NULL,
    created_date_key INT NOT NULL,
    expected_date_key INT NOT NULL,
    actual_date_key INT,
    purchase_order_line_id_dd INT NOT NULL,
    purchase_order_id_dd INT NOT NULL,
    qty_ordered NUMERIC(15, 2),
    qty_received NUMERIC(15, 2),
    unit_cost_eur NUMERIC(15, 2),
    gross_purchase_amount NUMERIC(15, 2),
    CONSTRAINT pk_fact_purchase PRIMARY KEY (sk_fact_purchase),
    CONSTRAINT fk_fact_purchase_dim_product FOREIGN KEY (sk_dim_product) REFERENCES gold.dim_product (sk_dim_product),
    CONSTRAINT fk_fact_purchase_dim_location FOREIGN KEY (sk_dim_location) REFERENCES gold.dim_location (sk_dim_location),
    CONSTRAINT fk_fact_purchase_dim_supplier FOREIGN KEY (sk_dim_supplier) REFERENCES gold.dim_supplier (sk_dim_supplier),
    CONSTRAINT fk_fact_purchase_dim_purchase_status FOREIGN KEY (sk_dim_purchase_status) REFERENCES gold.dim_purchase_status (sk_dim_purchase_status),
    CONSTRAINT fk_fact_purchase_expected_delivery_date FOREIGN KEY (expected_date_key) REFERENCES gold.dim_date (date_key),
    CONSTRAINT fk_fact_purchase_created_at_date FOREIGN KEY (created_date_key) REFERENCES gold.dim_date (date_key),
    CONSTRAINT fk_fact_purchase_actual_delivery_date FOREIGN KEY (actual_date_key) REFERENCES gold.dim_date (date_key)
);

CREATE INDEX idx_dim_customer_id_source ON gold.dim_customer(customer_id_source);
CREATE INDEX idx_dim_product_id_source ON gold.dim_product(product_id_source);
CREATE INDEX idx_dim_location_id_source ON gold.dim_location(location_id_source);
CREATE INDEX idx_dim_supplier_id_source ON gold.dim_supplier(supplier_id_source);
CREATE INDEX idx_dim_date ON gold.dim_date(full_date);
CREATE INDEX idx_dim_sales_channel_source_name ON gold.dim_sales_channel(source_name);
CREATE INDEX idx_dim_sales_status_name ON gold.dim_sales_status(status_name);
CREATE INDEX idx_dim_purchase_status_name ON gold.dim_purchase_status(status_name);

CREATE INDEX idx_fact_sales_line_sk_dim_customer ON gold.fact_sales_line(sk_dim_customer);
CREATE INDEX idx_fact_sales_line_sk_dim_location ON gold.fact_sales_line(sk_dim_location);
CREATE INDEX idx_fact_sales_line_sk_dim_product ON gold.fact_sales_line(sk_dim_product);
CREATE INDEX idx_fact_sales_line_sk_dim_sales_channel ON gold.fact_sales_line(sk_dim_sales_channel);
CREATE INDEX idx_fact_sales_line_sk_dim_sales_status ON gold.fact_sales_line(sk_dim_sales_status);
CREATE INDEX idx_fact_sales_line_date_key ON gold.fact_sales_line(date_key);

CREATE INDEX idx_fact_purchase_line_sk_dim_location ON gold.fact_purchase_line(sk_dim_location);
CREATE INDEX idx_fact_purchase_line_sk_dim_product ON gold.fact_purchase_line(sk_dim_product);
CREATE INDEX idx_fact_purchase_line_sk_dim_purchase_status ON gold.fact_purchase_line(sk_dim_purchase_status);
CREATE INDEX idx_fact_purchase_line_sk_dim_supplier ON gold.fact_purchase_line(sk_dim_supplier);
CREATE INDEX idx_fact_purchase_line_expected_delivery_date_key ON gold.fact_purchase_line(expected_date_key);
CREATE INDEX idx_fact_purchase_line_created_at_date_key ON gold.fact_purchase_line(created_date_key);
CREATE INDEX idx_fact_purchase_line_actual_delivery_date_key ON gold.fact_purchase_line(actual_date_key);

CREATE INDEX idx_fact_inventory_snapshot_sk_dim_location ON gold.fact_inventory_snapshot(sk_dim_location);
CREATE INDEX idx_fact_inventory_snapshot_sk_dim_product ON gold.fact_inventory_snapshot(sk_dim_product);
CREATE INDEX idx_fact_inventory_snapshot_last_updated_date_key ON gold.fact_inventory_snapshot(last_updated_date_key);
