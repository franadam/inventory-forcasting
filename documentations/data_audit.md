# Data audit

## Global Grain Definition

Each table has its own grain (level of detail). Understanding this is critical before building any pipeline.

**Master data tables** → 1 row per entity (product, supplier, location…)  
**Transactional tables (headers)** → 1 row per business document  
**Transactional tables (lines)** → 1 row per product in a document  
**Log tables** → 1 row per event


## Table-Level Data Audit

### **locations**
**Business**: Physical sites (stores and warehouses)  
**Grain**: 1 row = 1 location  
**PK**: ``location_id``  
**Metrics**: `storage_capacity_m3`    
**Business rules**: 
- `location_type` ∈ ('sanicenter','regional_warehouse')
- Only `is_active` = TRUE locations should be used in analytics
- Used as a key dimension for stock and sales

### **suppliers**
**Business**: Supplier master data  
**Grain**: 1 row = 1 supplier  
**PK**: `supplier_id`  
**Metrics**:  
`avg_lead_time_days`, `reliability_score`  
**Business rules**:
- `reliability_score` ∈ [0,1]
- Represents aggregated performance, not transaction-level truth
- Used for supplier performance analytics (not for Purchase Order calculations)

### **products**
**Business**: Product catalog  
**Grain**: 1 row = 1 product (SKU)  
**PK**: `product_id`  
**Metrics**:
`unit_weight_kg`,
`unit_price_eur`,
`lead_time_days` (default fallback),
`min_order_qty`  
**Business rules**:
- `lead_time_days` = fallback if no supplier-specific lead time
- `unit_price_eur` is current price only (not historized)
`is_active` = FALSE products should not generate new transactions

### **customers**
**Business**: Customer master data  
**Grain**: 1 row = 1 customer  
**PK**: `customer_id`  
**Metrics**: none  
**Business rules**:
- `customer_type` ∈ ('plumber','contractor','architect','retailer','individual')
- `is_professional` helps segmentation
- Used for demand segmentation

### **product_suppliers**
**Business**: Supplier-product relationship (sourcing)  
**Grain**: 1 row = 1 (product, supplier)  
**PK**: id  
**FK**: `product_id`, `supplier_id`  
**Metrics**:
`supplier_unit_cost_eur`,
`supplier_lead_time_days`  
**Business rules**:
- Overrides products.lead_time_days
`is_preferred` = TRUE → default supplier for replenishment
- A product can have multiple suppliers
- Used for sourcing and procurement logic

### **inventory**
**Business**: Current stock state  
**Grain**: 1 row = 1 (product, location)  
**PK**: `inventory_id`  
**FK**: `product_id`, `location_id`  
**Metrics**:  
`qty_on_hand`,
`qty_reserved`,
`min_stock_level`,
`reorder_point`,
`max_stock_level`  
**Business rules**:
- No history stored here
- `available_stock` = `qty_on_hand` - `qty_reserved`
- Must be reconstructed over time using inventory_movements table
- UNIQUE(product_id, location_id) enforced  

### **inventory_movements**
**Business**: Stock change audit log (source of truth for history)  
**Grain**: 1 row = 1 stock movement event  
**PK**: `movement_id`  
**FK**: `product_id`, `location_id`    
**Metrics**:  
`qty_delta` (positive = inbound, negative = outbound), `movement_ts`  
**Business rules**:
- `movement_type` ∈ ('sale','po_receipt','adjustment','transfer','initial_stock')
- This is the only reliable source to rebuild stock history  
- `ref_order_id` is optional (not all movements are linked to orders)
- Used for:
  - stock reconstruction
  - real-time streaming (Kafka)
  - auditability  
- Daily stock must be reconstructed using cumulative sum of `qty_delta`
  
### **sales_orders**
**Business**: Sales order header
**Grain**: 1 row = 1 sales order
**PK**: `order_id`
**FK**: `customer_id`, `location_id`
**Metrics**: `total_amount_eur`
**Business rules**:
- `status` ∈ ('pending','fulfilled','partial','cancelled')
- Only fulfilled and partial should contribute to demand
- `order_ts` = demand timestamp
- Header-level data only (no product detail)

### **sales_order_lines**
**Business**: Product-level sales detail  
**Grain**: 1 row = 1 product in 1 order  
**PK**: line_id  
**FK**: order_id, product_id  
**Metrics**:  
`qty_ordered`,
`qty_fulfilled`,
`unit_price_eur`,
`line_total_eur`  
**Business rules**:
- Demand definition:
  - gross demand = `qty_ordered`
  - served demand = `qty_fulfilled`
- `qty_fulfilled` ≤ `qty_ordered`
- `line_total_eur` = `qty_ordered × unit_price_eur` (should be validated)
- Must be joined with sales_orders table  for full context
- Cancelled orders should be excluded
- Partial orders indicate stock issues

### **purchase_orders**
**Business**: Purchase order header  
**Grain**: 1 row = 1 purchase order  
**PK**: `po_id`  
**FK**: `supplier_id`, `location_id`  
**Metrics**:
`total_cost_eur`,
`expected_delivery`,
`actual_delivery`  
**Business rules**:
- `status` ∈ ('draft','sent','confirmed','received','partial','cancelled')
- Lead time calculation:
`actual_lead_time = actual_delivery - created_at`
- Used to evaluate supplier performance

### **purchase_order_lines**
**Business**: Product-level purchase detail  
**Grain**: 1 row = 1 product in 1 purchase order  
**PK**: `line_id`  
**FK**: `po_id`, `product_id`  
**Metrics**:
`qty_ordered`,
`qty_received`,
`unit_cost_eur`,
`line_total_eur`  
**Business rules**:
- `qty_received` ≤ `qty_ordered`
- Used to track partial deliveries
- Must be joined with purchase_orders

## Data Quality Critical Checks

- `qty_fulfilled ≤ qty_ordered`
- `qty_received ≤ qty_ordered`
- no duplicate (`product_id, location_id`) in inventory table
- referential integrity between headers and lines
- no negative stock anomalies (unless justified)
- timestamps consistency:
`actual_delivery ≥ created_at`
- valid status values