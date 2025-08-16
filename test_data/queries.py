"""
enhanced_queries.py
Enhanced query definitions with both PostgreSQL and Neo4j support for performance comparison
"""

from neo4j import GraphDatabase
import psycopg2

# PostgreSQL Query definitions (existing)
POSTGRESQL_QUERIES = {
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
    
    # PostgreSQL version of product associations
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

# Neo4j Query definitions (focused on direct comparison)
NEO4J_QUERIES = {
    "product_associations_neo4j": {
        "description": "Products frequently bought together - Neo4j graph version",
        "dataset_reference": "dataset 13",
        "database": "neo4j",
        "cypher": """
        MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
        MATCH (p1)-[:BELONGS_TO]->(c1:Category)
        MATCH (p2)-[:BELONGS_TO]->(c2:Category)
        RETURN p1.product_name as product_a,
               p1.brand as brand_a,
               c1.category_name as category_a,
               p2.product_name as product_b,
               p2.brand as brand_b,
               c2.category_name as category_b,
               r.frequency_count as frequency_count,
               r.last_calculated as last_calculated
        ORDER BY r.frequency_count DESC
        """
    }
}

# Performance comparison queries (simplified to focus on core comparison)
PERFORMANCE_COMPARISON_QUERIES = {
    "association_frequency_analysis": {
        "postgresql": {
            "description": "Association frequency analysis - PostgreSQL version",
            "sql": """
            SELECT 
                CASE 
                    WHEN frequency_count >= 10 THEN '10+'
                    WHEN frequency_count >= 5 THEN '5-9'
                    WHEN frequency_count >= 3 THEN '3-4'
                    ELSE '1-2'
                END as frequency_range,
                COUNT(*) as association_count,
                AVG(frequency_count) as avg_frequency,
                MIN(frequency_count) as min_frequency,
                MAX(frequency_count) as max_frequency
            FROM Product_Associations
            GROUP BY 
                CASE 
                    WHEN frequency_count >= 10 THEN '10+'
                    WHEN frequency_count >= 5 THEN '5-9'
                    WHEN frequency_count >= 3 THEN '3-4'
                    ELSE '1-2'
                END
            ORDER BY min_frequency DESC;
            """
        },
        "neo4j": {
            "description": "Association frequency analysis - Neo4j version",
            "cypher": """
            MATCH ()-[r:BOUGHT_TOGETHER]->()
            WITH r.frequency_count as freq,
                 CASE 
                     WHEN r.frequency_count >= 10 THEN '10+'
                     WHEN r.frequency_count >= 5 THEN '5-9'
                     WHEN r.frequency_count >= 3 THEN '3-4'
                     ELSE '1-2'
                 END as frequency_range
            RETURN frequency_range,
                   count(*) as association_count,
                   avg(freq) as avg_frequency,
                   min(freq) as min_frequency,
                   max(freq) as max_frequency
            ORDER BY min_frequency DESC
            """
        }
    }
}

# Combined query configuration
QUERY_CONFIG = {
    "display_limit": 5,
    "sample_data_limit": 3,
    "export_filename_template": "enhanced_analytics_results_{timestamp}.json",
    "timestamp_format": "%Y%m%d_%H%M%S",
    "neo4j_batch_size": 1000,
    "performance_comparison_enabled": True
}

def get_postgresql_queries():
    """Return all PostgreSQL queries"""
    return POSTGRESQL_QUERIES

def get_neo4j_queries():
    """Return all Neo4j queries"""
    return NEO4J_QUERIES

def get_performance_comparison_queries():
    """Return performance comparison queries"""
    return PERFORMANCE_COMPARISON_QUERIES

def get_all_queries():
    """Return all queries combined"""
    all_queries = {}
    all_queries.update(POSTGRESQL_QUERIES)
    all_queries.update(NEO4J_QUERIES)
    
    # Add performance comparison queries
    for query_name, query_data in PERFORMANCE_COMPARISON_QUERIES.items():
        # Add PostgreSQL version
        if 'postgresql' in query_data:
            pg_query_name = f"{query_name}_postgresql"
            all_queries[pg_query_name] = {
                **query_data['postgresql'],
                'database': 'postgresql',
                'dataset_reference': 'dataset 13',
                'comparison_group': query_name
            }
        
        # Add Neo4j version
        if 'neo4j' in query_data:
            neo4j_query_name = f"{query_name}_neo4j"
            all_queries[neo4j_query_name] = {
                **query_data['neo4j'],
                'database': 'neo4j',
                'dataset_reference': 'dataset 13',
                'comparison_group': query_name
            }
    
    return all_queries

def get_query_list():
    """Return list of available query names"""
    return list(get_all_queries().keys())

def get_query(query_name):
    """Get a specific query by name"""
    return get_all_queries().get(query_name)

def get_queries_by_database(database_type):
    """Get queries filtered by database type"""
    all_queries = get_all_queries()
    return {name: query for name, query in all_queries.items() 
            if query.get('database') == database_type}

def get_comparison_query_pairs():
    """Get query pairs for performance comparison (simplified)"""
    comparison_pairs = []
    
    # Main product associations comparison
    comparison_pairs.append({
        'comparison_name': 'Product Associations',
        'postgresql_query': 'product_associations_postgresql',
        'neo4j_query': 'product_associations_neo4j',
        'description': 'Direct comparison: PostgreSQL JOINs vs Neo4j graph traversal'
    })
    
    # Frequency analysis comparison
    comparison_pairs.append({
        'comparison_name': 'Association Frequency Analysis',
        'postgresql_query': 'association_frequency_analysis_postgresql',
        'neo4j_query': 'association_frequency_analysis_neo4j',
        'description': 'Compare aggregation performance between databases'
    })
    
    return comparison_pairs