# Testdata

## Table Customers
- customer_id (int, pk)
- first_name (string)
- last_name (string)
- email
- phone
- registration_date (datetime, auto add now)
- address (string)

## Table Orders
- order_id (int, pk)
- customer_id (int, fk)
- order_date (datetime, auto add now)
- total_amount (decimal)
- status (string)
- payment_method

## Table Order_Items
- order_item_id (int, pk)
- order_id (int, fk)
- product_id (int, fk)
- quantity: int
- unit_price (decimal)
- total_price (decimal)

## Table Categories
- category_id (int, pk)
- category_name (string)
- description (text)
- parent_category_id (int, fk)

## Table Products
- product_id (int, pk)
- product_name (string)
- description (text)
- price (decimal)
- category (string)
- brand (string)
- stock_qty (int)
- is_active

## Table Product_Associations
- association_id (int, pk)
- product_a_id (int, fk)
- product_b_id (int, fk)
- frequence_count (int)
- last_calculated (datetime)

# Generate orders and items first
python generate_testdata.py --rows 5000 --tables customers products orders order_items

# Then update associations based on actual order patterns
python generate_testdata.py --update-associations

python analyze_associations.py