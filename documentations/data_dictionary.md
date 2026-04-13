# Data Dictionary 

# 1.  Locations

Represents physical sites such as stores or warehouses.

| Field                 | Description                                                    |
| --------------------- | -------------------------------------------------------------- |
| `location_id`         | Unique identifier of the location                              |
| `location_code`       | Business code used to identify the location (e.g., store code) |
| `location_name`       | Human-readable name of the location                            |
| `location_type`       | Type of location: `sanicenter` (store) or `regional_warehouse` |
| `address`             | Street address                                                 |
| `city`                | City where the location is situated                            |
| `postal_code`         | Postal/ZIP code                                                |
| `region`              | Province  (not the region)                   |
| `storage_capacity_m3` | Maximum storage capacity in cubic meters                       |
| `is_active`           | Indicates whether the location is currently operational        |

---

# 2. Suppliers

Represents companies providing products.

| Field                | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `supplier_id`        | Unique identifier of the supplier                     |
| `supplier_code`      | Business identifier                                   |
| `supplier_name`      | Supplier company name                                 |
| `contact_name`       | Main contact person                                   |
| `email`              | Contact email address                                 |
| `country`            | Country of the supplier                               |
| `avg_lead_time_days` | Average delivery time in days                         |
| `reliability_score`  | Score between 0 and 1 indicating delivery reliability |
| `is_active`          | Whether the supplier is currently used                |

---

# 3. Products

Represents items sold and stocked.

| Field             | Description                                     |
| ----------------- | ----------------------------------------------- |
| `product_id`      | Unique identifier of the product                |
| `sku`             | Stock Keeping Unit (unique business identifier) |
| `product_name`    | Name/description of the product                 |
| `category`        | High-level category (e.g., Pipes, Heating)      |
| `subcategory`     | More detailed classification                    |
| `brand`           | Manufacturer or brand                           |
| `unit_of_measure` | Unit used for sales (e.g., unit, piece, kg)     |
| `unit_weight_kg`  | Weight of one unit in kilograms                 |
| `unit_price_eur`  | Selling price per unit (EUR)                    |
| `lead_time_days`  | Internal estimated procurement delay            |
| `min_order_qty`   | Minimum quantity that can be ordered            |
| `is_active`       | Indicates if the product is still sold          |
| `created_at`      | Creation timestamp                              |
| `updated_at`      | Last update timestamp                           |

---

# 4. Customers

Represents buyers (B2B and B2C).

| Field             | Description                                      |
| ----------------- | ------------------------------------------------ |
| `customer_id`     | Unique identifier                                |
| `customer_code`   | Business identifier                              |
| `customer_name`   | Name of the customer                             |
| `customer_type`   | Type of customer (`plumber`, `contractor`, etc.) |
| `address`         | Customer address                                 |
| `city`            | City                                             |
| `postal_code`     | Postal code                                      |
| `region`          | Province  (not the region)                                         |
| `is_professional` | Whether the customer is a professional           |
| `is_active`       | Whether the customer is still active             |

---

# 5. Product Suppliers

Defines which supplier provides which product.

| Field                     | Description                                   |
| ------------------------- | --------------------------------------------- |
| `id`                      | Unique identifier                             |
| `product_id`              | Reference to product                          |
| `supplier_id`             | Reference to supplier                         |
| `supplier_unit_cost_eur`  | Purchase cost per unit                        |
| `supplier_lead_time_days` | Delivery delay for this supplier-product pair |
| `is_preferred`            | Indicates if this is the preferred supplier   |

---

# 6. Inventory (Current Stock State)

Represents the current stock level of each product at each location.

| Field             | Description                                      |
| ----------------- | ------------------------------------------------ |
| `inventory_id`    | Unique identifier                                |
| `product_id`      | Product reference                                |
| `location_id`     | Location reference                               |
| `qty_on_hand`     | Current physical stock available in the location |
| `qty_reserved`    | Quantity already reserved for customer orders    |
| `min_stock_level` | Minimum acceptable stock level                   |
| `reorder_point`   | Stock threshold triggering replenishment         |
| `max_stock_level` | Maximum desired stock level                      |
| `last_updated`    | Last update timestamp                            |

### Key Business Concepts

* **Available stock = qty_on_hand - qty_reserved**
* If stock falls below `reorder_point`, a purchase order should be triggered

---

# 7. Sales Orders

Represents customer orders.

| Field              | Description                                                   |
| ------------------ | ------------------------------------------------------------- |
| `order_id`         | Unique order identifier                                       |
| `customer_id`      | Customer placing the order                                    |
| `location_id`      | Location fulfilling the order                                 |
| `order_ts`         | Order timestamp                                               |
| `status`           | Order status (`pending`, `fulfilled`, `partial`, `cancelled`) |
| `source`           | Order origin (`counter`, `online`, etc.)                      |
| `total_amount_eur` | Total order value                                             |

---

# 8. Sales Order Lines

Represents individual products within an order.

| Field            | Description                        |
| ---------------- | ---------------------------------- |
| `line_id`        | Unique identifier                  |
| `order_id`       | Parent order                       |
| `product_id`     | Ordered product                    |
| `qty_ordered`    | Quantity requested by the customer |
| `qty_fulfilled`  | Quantity actually delivered        |
| `unit_price_eur` | Selling price per unit             |
| `line_total_eur` | Total line amount                  |

### Key Business Concepts

* **Demand = qty_ordered**
* **Served demand = qty_fulfilled**
* Gap between the two indicates **stock issues or shortages**

---

# 9. Purchase Orders

Represents orders placed to suppliers.

| Field               | Description                                |
| ------------------- | ------------------------------------------ |
| `po_id`             | Unique identifier                          |
| `supplier_id`       | Supplier                                   |
| `location_id`       | Receiving location                         |
| `created_at`        | Creation timestamp                         |
| `expected_delivery` | Planned delivery date                      |
| `actual_delivery`   | Real delivery date                         |
| `status`            | Status (`draft`, `sent`, `received`, etc.) |
| `total_cost_eur`    | Total purchase cost                        |

---

# 10. Purchase Order Lines

Represents products ordered from suppliers.

| Field            | Description                |
| ---------------- | -------------------------- |
| `line_id`        | Unique identifier          |
| `po_id`          | Parent purchase order      |
| `product_id`     | Product                    |
| `qty_ordered`    | Quantity ordered           |
| `qty_received`   | Quantity actually received |
| `unit_cost_eur`  | Cost per unit              |
| `line_total_eur` | Total cost                 |

### Key Business Concepts

* Difference between ordered and received quantities indicates **supplier performance issues**

---

# 11. Inventory Movements

Tracks all stock changes over time.

| Field            | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `movement_id`    | Unique identifier                                           |
| `product_id`     | Product                                                     |
| `location_id`    | Location                                                    |
| `movement_type`  | Type of movement (`sale`, `po_receipt`, `adjustment`, etc.) |
| `qty_delta`      | Quantity change (positive or negative)                      |
| `ref_order_id`   | Related order ID                                            |
| `ref_order_type` | Type of reference (sales or purchase)                       |
| `movement_ts`    | Timestamp of the movement                                   |
| `notes`          | Additional information                                      |

### Key Business Concepts

* **Positive qty_delta** → stock increase (purchase, adjustment)
* **Negative qty_delta** → stock decrease (sale)
* This table is the **source of truth for stock history**
