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

# Postman Test Data for DDRS API

## Prerequisites
- Ensure your Django server is running on `http://localhost:8000`
- Have your authentication token ready
- Set these Postman environment variables:
  - `base_url`: `http://localhost:8000`
  - `auth_token`: Your Django Token

## Step 1: Create the PostgreSQL Datastore

**Method:** `POST`  
**URL:** `{{base_url}}/api/datastores/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Test Data PostgreSQL",
    "type": "sql",
    "system": "postgres",
    "description": "PostgreSQL database containing test e-commerce data with customers, products, orders, and relationships. Used for testing recommendation algorithms with realistic retail data patterns.",
    "server": "localhost",
    "port": 5433,
    "username": "test",
    "password": "test",
    "password_confirm": "test",
    "is_active": true,
    "max_connections": 100,
    "avg_response_time_ms": 45.0,
    "storage_capacity_gb": 50.0
}
```

---

## Step 2: Create Datasets (Execute in Order)

### 2.1 Categories Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Product Categories",
    "short_description": "Hierarchical product categorization system with parent-child relationships. Supports multi-level category organization for product classification.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "low",
    "access_patterns": "read_heavy",
    "query_complexity": "low",
    "properties": [
        "category_id",
        "category_name", 
        "description",
        "parent_category_id",
        "created_at",
        "updated_at"
    ],
    "sample_data": [
        [1, "Electronics", "Electronic devices and accessories", null, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        [2, "Clothing", "Apparel and fashion items", null, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        [5, "Smartphones", "Mobile phones and accessories", 1, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]
    ],
    "estimated_size_gb": 0.1,
    "avg_query_time_ms": 15.0,
    "queries_per_day": 500.0,
    "queries": [
        {
            "name": "Get Category Hierarchy",
            "query_text": "SELECT c1.*, c2.category_name as parent_name FROM Categories c1 LEFT JOIN Categories c2 ON c1.parent_category_id = c2.category_id",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 12.0,
            "description": "Retrieve category hierarchy with parent category names"
        },
        {
            "name": "Find Root Categories",
            "query_text": "SELECT * FROM Categories WHERE parent_category_id IS NULL ORDER BY category_name",
            "query_type": "select",
            "frequency": "medium",
            "avg_execution_time_ms": 8.0,
            "description": "Get top-level categories for navigation menus"
        }
    ]
}
```

### 2.2 Customers Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Customer Data",
    "short_description": "Customer information including personal details, contact information, and registration data. Core entity for order management and customer analytics.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "medium",
    "access_patterns": "read_write_heavy",
    "query_complexity": "medium",
    "properties": [
        "customer_id",
        "first_name",
        "last_name", 
        "email",
        "phone",
        "registration_date",
        "address"
    ],
    "sample_data": [
        [1, "John", "Doe", "john.doe@email.com", "+1234567890", "2024-01-15T10:30:00Z", "123 Main St, City, State"],
        [2, "Jane", "Smith", "jane.smith@email.com", "+1234567891", "2024-02-20T14:15:00Z", "456 Oak Ave, City, State"],
        [3, "Bob", "Johnson", "bob.johnson@email.com", "+1234567892", "2024-03-10T09:45:00Z", "789 Pine Rd, City, State"]
    ],
    "estimated_size_gb": 2.5,
    "avg_query_time_ms": 25.0,
    "queries_per_day": 3000.0,
    "queries": [
        {
            "name": "Customer Lookup by Email",
            "query_text": "SELECT * FROM Customers WHERE email = ?",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 18.0,
            "description": "Find customer by email for authentication and profile access"
        },
        {
            "name": "Customer Registration",
            "query_text": "INSERT INTO Customers (first_name, last_name, email, phone, address) VALUES (?, ?, ?, ?, ?)",
            "query_type": "insert",
            "frequency": "medium",
            "avg_execution_time_ms": 35.0,
            "description": "Register new customer account"
        },
        {
            "name": "Update Customer Profile",
            "query_text": "UPDATE Customers SET first_name = ?, last_name = ?, phone = ?, address = ? WHERE customer_id = ?",
            "query_type": "update",
            "frequency": "low",
            "avg_execution_time_ms": 22.0,
            "description": "Update customer profile information"
        }
    ]
}
```

### 2.3 Products Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Product Catalog",
    "short_description": "Complete product catalog with pricing, inventory, and categorization. Central dataset for e-commerce operations with brand and stock management.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "medium",
    "access_patterns": "read_heavy",
    "query_complexity": "medium",
    "properties": [
        "product_id",
        "product_name",
        "description",
        "price",
        "category_id",
        "brand",
        "stock_qty",
        "is_active",
        "created_at",
        "updated_at"
    ],
    "sample_data": [
        [1, "iPhone 15 Pro", "Latest Apple smartphone", 1199.99, 5, "Apple", 50, true, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        [2, "Samsung Galaxy S24", "Samsung flagship phone", 999.99, 5, "Samsung", 30, true, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        [3, "MacBook Air M3", "Apple laptop with M3 chip", 1299.99, 6, "Apple", 25, true, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]
    ],
    "estimated_size_gb": 8.0,
    "avg_query_time_ms": 35.0,
    "queries_per_day": 8000.0,
    "queries": [
        {
            "name": "Product Search by Category",
            "query_text": "SELECT p.*, c.category_name FROM Products p JOIN Categories c ON p.category_id = c.category_id WHERE p.category_id = ? AND p.is_active = true ORDER BY p.product_name",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 28.0,
            "description": "Browse products by category with category name"
        },
        {
            "name": "Product Search by Brand",
            "query_text": "SELECT * FROM Products WHERE brand = ? AND is_active = true ORDER BY price",
            "query_type": "select",
            "frequency": "medium",
            "avg_execution_time_ms": 22.0,
            "description": "Find products by specific brand"
        },
        {
            "name": "Update Stock Quantity",
            "query_text": "UPDATE Products SET stock_qty = stock_qty - ? WHERE product_id = ? AND stock_qty >= ?",
            "query_type": "update",
            "frequency": "high",
            "avg_execution_time_ms": 15.0,
            "description": "Decrease stock after order placement"
        },
        {
            "name": "Low Stock Alert",
            "query_text": "SELECT product_id, product_name, stock_qty FROM Products WHERE stock_qty < 10 AND is_active = true",
            "query_type": "select",
            "frequency": "low",
            "avg_execution_time_ms": 45.0,
            "description": "Monitor products with low inventory"
        }
    ],
    "relationships": [
        {
            "to_dataset": 1,
            "relationship_type": "foreign_key",
            "strength": 9,
            "description": "Products belong to categories via category_id foreign key",
            "is_active": true
        }
    ]
}
```

### 2.4 Orders Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Order Management",
    "short_description": "Order transactions with customer relationships, payment information, and order status tracking. Core transactional dataset for e-commerce operations.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "high",
    "access_patterns": "read_write_heavy",
    "query_complexity": "medium",
    "properties": [
        "order_id",
        "customer_id",
        "order_date",
        "total_amount",
        "status",
        "payment_method"
    ],
    "sample_data": [
        [1, 1, "2025-01-15T10:30:00Z", 1229.98, "completed", "credit_card"],
        [2, 2, "2025-01-16T14:20:00Z", 79.99, "pending", "paypal"],
        [3, 3, "2025-01-17T09:15:00Z", 2199.98, "shipped", "credit_card"]
    ],
    "estimated_size_gb": 15.0,
    "avg_query_time_ms": 40.0,
    "queries_per_day": 5000.0,
    "queries": [
        {
            "name": "Customer Order History",
            "query_text": "SELECT * FROM Orders WHERE customer_id = ? ORDER BY order_date DESC",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 25.0,
            "description": "Get order history for specific customer"
        },
        {
            "name": "Create New Order",
            "query_text": "INSERT INTO Orders (customer_id, total_amount, status, payment_method) VALUES (?, ?, ?, ?)",
            "query_type": "insert",
            "frequency": "high",
            "avg_execution_time_ms": 30.0,
            "description": "Create new order record"
        },
        {
            "name": "Update Order Status",
            "query_text": "UPDATE Orders SET status = ? WHERE order_id = ?",
            "query_type": "update",
            "frequency": "medium",
            "avg_execution_time_ms": 18.0,
            "description": "Update order fulfillment status"
        },
        {
            "name": "Daily Sales Report",
            "query_text": "SELECT status, COUNT(*) as order_count, SUM(total_amount) as total_sales FROM Orders WHERE DATE(order_date) = CURRENT_DATE GROUP BY status",
            "query_type": "aggregate",
            "frequency": "low",
            "avg_execution_time_ms": 85.0,
            "description": "Generate daily sales summary by status"
        }
    ],
    "relationships": [
        {
            "to_dataset": 2,
            "relationship_type": "foreign_key",
            "strength": 10,
            "description": "Orders belong to customers via customer_id foreign key",
            "is_active": true
        }
    ]
}
```

### 2.5 Order Items Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Order Line Items",
    "short_description": "Detailed line items for each order including product references, quantities, and pricing. Junction table connecting orders to products with transaction details.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "high",
    "access_patterns": "read_write_heavy",
    "query_complexity": "high",
    "properties": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_price"
    ],
    "sample_data": [
        [1, 1, 1, 1, 1199.99, 1199.99],
        [2, 1, 5, 1, 29.99, 29.99],
        [3, 2, 6, 1, 79.99, 79.99],
        [4, 3, 1, 1, 1199.99, 1199.99],
        [5, 3, 3, 1, 1299.99, 1299.99]
    ],
    "estimated_size_gb": 25.0,
    "avg_query_time_ms": 50.0,
    "queries_per_day": 7000.0,
    "queries": [
        {
            "name": "Get Order Details",
            "query_text": "SELECT oi.*, p.product_name, p.brand FROM Order_Items oi JOIN Products p ON oi.product_id = p.product_id WHERE oi.order_id = ?",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 45.0,
            "description": "Get detailed order items with product information"
        },
        {
            "name": "Add Item to Order",
            "query_text": "INSERT INTO Order_Items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
            "query_type": "insert",
            "frequency": "high",
            "avg_execution_time_ms": 25.0,
            "description": "Add product to existing order"
        },
        {
            "name": "Product Sales Analysis",
            "query_text": "SELECT p.product_name, SUM(oi.quantity) as total_sold, SUM(oi.total_price) as revenue FROM Order_Items oi JOIN Products p ON oi.product_id = p.product_id GROUP BY p.product_id, p.product_name ORDER BY total_sold DESC",
            "query_type": "aggregate",
            "frequency": "low",
            "avg_execution_time_ms": 120.0,
            "description": "Analyze product sales performance"
        },
        {
            "name": "Customer Purchase Pattern",
            "query_text": "SELECT p.brand, COUNT(DISTINCT oi.order_id) as order_count FROM Order_Items oi JOIN Products p ON oi.product_id = p.product_id JOIN Orders o ON oi.order_id = o.order_id WHERE o.customer_id = ? GROUP BY p.brand",
            "query_type": "aggregate",
            "frequency": "medium",
            "avg_execution_time_ms": 75.0,
            "description": "Analyze customer brand preferences"
        }
    ],
    "relationships": [
        {
            "to_dataset": 4,
            "relationship_type": "foreign_key",
            "strength": 10,
            "description": "Order items belong to orders via order_id foreign key",
            "is_active": true
        },
        {
            "to_dataset": 3,
            "relationship_type": "foreign_key",
            "strength": 9,
            "description": "Order items reference products via product_id foreign key",
            "is_active": true
        }
    ]
}
```

### 2.6 Product Associations Dataset

**Method:** `POST`  
**URL:** `{{base_url}}/api/datasets/`  
**Headers:**
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```

**Body:**
```json
{
    "name": "Product Associations",
    "short_description": "Machine learning-driven product association data tracking frequently bought together patterns. Used for recommendation algorithms and cross-selling analytics.",
    "current_datastore": 1,
    "data_structure": "structured",
    "growth_rate": "medium",
    "access_patterns": "analytical",
    "query_complexity": "high",
    "properties": [
        "association_id",
        "product_a_id",
        "product_b_id",
        "frequency_count",
        "last_calculated"
    ],
    "sample_data": [
        [1, 1, 3, 15, "2025-01-10T00:00:00Z"],
        [2, 1, 5, 8, "2025-01-10T00:00:00Z"],
        [3, 3, 1, 15, "2025-01-10T00:00:00Z"]
    ],
    "estimated_size_gb": 5.0,
    "avg_query_time_ms": 65.0,
    "queries_per_day": 1200.0,
    "queries": [
        {
            "name": "Get Product Recommendations",
            "query_text": "SELECT pa.product_b_id, p.product_name, pa.frequency_count FROM Product_Associations pa JOIN Products p ON pa.product_b_id = p.product_id WHERE pa.product_a_id = ? ORDER BY pa.frequency_count DESC LIMIT 5",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 55.0,
            "description": "Get recommended products based on association strength"
        },
        {
            "name": "Update Association Frequency",
            "query_text": "UPDATE Product_Associations SET frequency_count = frequency_count + 1, last_calculated = CURRENT_TIMESTAMP WHERE product_a_id = ? AND product_b_id = ?",
            "query_type": "update",
            "frequency": "medium",
            "avg_execution_time_ms": 20.0,
            "description": "Increment association frequency when products bought together"
        },
        {
            "name": "Recalculate Associations",
            "query_text": "INSERT INTO Product_Associations (product_a_id, product_b_id, frequency_count, last_calculated) SELECT CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END, CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END, COUNT(*), CURRENT_TIMESTAMP FROM Order_Items oi1 JOIN Order_Items oi2 ON oi1.order_id = oi2.order_id WHERE oi1.product_id != oi2.product_id GROUP BY 1, 2 HAVING COUNT(*) >= 2 ON CONFLICT (product_a_id, product_b_id) DO UPDATE SET frequency_count = EXCLUDED.frequency_count, last_calculated = EXCLUDED.last_calculated",
            "query_type": "complex",
            "frequency": "low",
            "avg_execution_time_ms": 250.0,
            "description": "Batch recalculate all product associations from order history"
        },
        {
            "name": "Top Product Pairs",
            "query_text": "SELECT p1.product_name as product_a, p2.product_name as product_b, pa.frequency_count FROM Product_Associations pa JOIN Products p1 ON pa.product_a_id = p1.product_id JOIN Products p2 ON pa.product_b_id = p2.product_id ORDER BY pa.frequency_count DESC LIMIT 10",
            "query_type": "aggregate",
            "frequency": "low",
            "avg_execution_time_ms": 95.0,
            "description": "Find most frequently associated product pairs"
        }
    ],
    "relationships": [
        {
            "to_dataset": 3,
            "relationship_type": "foreign_key",
            "strength": 8,
            "description": "Product associations reference products via product_a_id foreign key",
            "is_active": true
        },
        {
            "to_dataset": 3,
            "relationship_type": "foreign_key",
            "strength": 8,
            "description": "Product associations reference products via product_b_id foreign key",
            "is_active": true
        },
        {
            "to_dataset": 5,
            "relationship_type": "dependency",
            "strength": 7,
            "description": "Product associations calculated from order items data patterns",
            "is_active": true
        }
    ]
}
```

---

## Execution Order Summary

1. **Create Datastore** (Step 1)
2. **Create Categories Dataset** (Step 2.1)
3. **Create Customers Dataset** (Step 2.2)
4. **Create Products Dataset** (Step 2.3) - References Categories
5. **Create Orders Dataset** (Step 2.4) - References Customers
6. **Create Order Items Dataset** (Step 2.5) - References Orders and Products
7. **Create Product Associations Dataset** (Step 2.6) - References Products and Order Items

## Notes

- **Dataset IDs**: Update the `current_datastore` field in each dataset to match the ID returned from the datastore creation (Step 1)
- **Relationship IDs**: Update the `to_dataset` values in relationships to match the actual dataset IDs returned from creation
- **Foreign Key Dependencies**: The creation order is important due to foreign key relationships
- **Data Realism**: All sample data reflects realistic e-commerce scenarios
- **Query Patterns**: Each dataset includes realistic queries that would actually be performed on that table
- **Performance Metrics**: Estimated sizes and query times are based on typical e-commerce workloads

## Verification

After creating all datasets, you can verify the relationships by calling:
- `GET {{base_url}}/api/datasets/` - List all datasets
- `GET {{base_url}}/api/datasets/{id}/` - Get detailed view with relationships
- `GET {{base_url}}/api/relationships/` - List all relationships