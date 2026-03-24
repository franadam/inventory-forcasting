# Business Logic
## Demand definition
Demand is derived from:
- **gross demand**: `sales_order_lines.qty_ordered`  
- **served demand**: `sales_order_lines.qty_fulfilled`   

- Cancelled orders should be excluded
- Partial orders indicate stock issues

## Stock reconstruction
- Source of truth: `inventory_movements`
- Daily stock must be reconstructed using cumulative sum of `qty_delta`
- inventory table = latest snapshot only

## Lead time logic

Hierarchy:

1. `product_suppliers.supplier_lead_time_days` -> **priority**
2. `products.lead_time_days` -> **fallback**
3. **Actual lead time**:
`purchase_orders.actual_delivery - created_at`

Used for:

- reorder point calculation
- supplier performance KPIs

## Pricing logic
- `products.unit_price_eur` = **current price**
- `sales_order_lines.unit_price_eur` = **historical price** (source of truth for analytics)