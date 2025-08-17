"""
Querie Set Postgres
"""



POSTGRES_QUERIES = {
    "favorite_products": {
        "description": "Most popular products by total quantity sold from completed/shipped orders",
        "dataset_reference": "dataset 10",
        "database": "postgresql",
        "sql": """
        SELECT p.product_id, p.product_name, p.brand, c.category_name, 
               SUM(oi.quantity) as total_quantity_sold,
               COUNT(DISTINCT oi.order_id) as number_of_orders,
               SUM(oi.total_price) as total_revenue
        FROM Products p
        JOIN Categories c ON p.category_id = c.category_id
        JOIN Order_Items oi ON p.product_id = oi.product_id
        JOIN Orders o ON oi.order_id = o.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY p.product_id, p.product_name, p.brand, c.category_name
        ORDER BY total_quantity_sold DESC;
        """
    },
    
    "favorite_categories": {
        "description": "Most popular product categories by total quantity sold",
        "dataset_reference": "dataset 8",
        "database": "postgresql",
        "sql": """
        SELECT
            c.category_id,
            c.category_name,
            c.description,
            SUM(oi.quantity) as total_quantity_sold,
            COUNT(DISTINCT oi.order_id) as number_of_orders,
            COUNT(DISTINCT p.product_id) as number_of_products,
            SUM(oi.total_price) as total_revenue,
            ROUND(AVG(oi.unit_price), 2) as avg_unit_price
        FROM Categories c
        JOIN Products p ON c.category_id = p.category_id
        JOIN Order_Items oi ON p.product_id = oi.product_id
        JOIN Orders o ON oi.order_id = o.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY c.category_id, c.category_name, c.description
        ORDER BY total_quantity_sold DESC;
        """
    },
    
    "customer_product_patterns": {
        "description": "Customer purchasing patterns - how often each customer buys each product",
        "dataset_reference": "dataset 10",
        "database": "postgresql",
        "sql": """
        SELECT
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) as customer_name,
            c.email,
            p.product_id,
            p.product_name,
            p.brand,
            cat.category_name,
            SUM(oi.quantity) as total_quantity_purchased,
            COUNT(DISTINCT o.order_id) as number_of_orders,
            SUM(oi.total_price) as total_spent_on_product,
            ROUND(AVG(oi.unit_price), 2) as avg_unit_price,
            MIN(o.order_date) as first_purchase_date,
            MAX(o.order_date) as last_purchase_date
        FROM Customers c
        JOIN Orders o ON c.customer_id = o.customer_id
        JOIN Order_Items oi ON o.order_id = oi.order_id
        JOIN Products p ON oi.product_id = p.product_id
        JOIN Categories cat ON p.category_id = cat.category_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY
            c.customer_id, c.first_name, c.last_name, c.email,
            p.product_id, p.product_name, p.brand, cat.category_name
        ORDER BY total_quantity_purchased DESC, customer_name ASC, p.product_name ASC;
        """
    },
    
    "product_associations_postgresql": {
        "description": "Products frequently bought together - PostgreSQL version",
        "dataset_reference": "dataset 13",
        "database": "postgresql",
        "sql": """
        SELECT
            pa1.product_name as product_a,
            pa1.brand as brand_a,
            cat1.category_name as category_a,
            pa2.product_name as product_b,
            pa2.brand as brand_b,
            cat2.category_name as category_b,
            pa.frequency_count,
            pa.last_calculated
        FROM Product_Associations pa
        JOIN Products pa1 ON pa.product_a_id = pa1.product_id
        JOIN Products pa2 ON pa.product_b_id = pa2.product_id
        JOIN Categories cat1 ON pa1.category_id = cat1.category_id
        JOIN Categories cat2 ON pa2.category_id = cat2.category_id
        ORDER BY pa.frequency_count DESC;
        """
    }
}




def get_postgresql_queries():
    """Return all PostgreSQL queries"""
    return POSTGRES_QUERIES

