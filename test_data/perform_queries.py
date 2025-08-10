#!/usr/bin/env python3
"""
Product Analytics Query Performance Script
Executes meaningful analytics queries to analyze product buying patterns
and measures query execution times for performance analysis
"""

import psycopg2
import time
from datetime import datetime, timedelta
from decouple import config
from tabulate import tabulate
import json
from decimal import Decimal


class ProductAnalytics:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.query_results = {}
        self.performance_metrics = {}
    
    def connect(self):
        """Connect to the database"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print(f"✅ Connected to database: {self.db_config['host']}:{self.db_config['port']}")
            return True
        except psycopg2.Error as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query_name, query_sql, description=""):
        """Execute a query and measure performance"""
        print(f"\n🔍 Executing: {query_name}")
        if description:
            print(f"   Description: {description}")
        
        try:
            cursor = self.connection.cursor()
            
            # Measure execution time
            start_time = time.time()
            cursor.execute(query_sql)
            results = cursor.fetchall()
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            
            # Store results and metrics
            self.query_results[query_name] = {
                'results': results,
                'columns': column_names,
                'row_count': len(results),
                'description': description
            }
            
            self.performance_metrics[query_name] = {
                'execution_time': execution_time,
                'row_count': len(results),
                'timestamp': datetime.now()
            }
            
            print(f"   ⏱️  Execution time: {execution_time:.4f}s")
            print(f"   📊 Rows returned: {len(results):,}")
            
            return results, column_names, execution_time
            
        except psycopg2.Error as e:
            print(f"   ❌ Query failed: {e}")
            return None, None, None
    
    def display_results(self, query_name, limit=10):
        """Display query results in a formatted table"""
        if query_name not in self.query_results:
            print(f"❌ No results found for query: {query_name}")
            return
        
        result_data = self.query_results[query_name]
        results = result_data['results']
        columns = result_data['columns']
        
        print(f"\n📊 Results for {query_name}:")
        print(f"   Total rows: {len(results):,}")
        
        if not results:
            print("   No data found")
            return
        
        # Show limited results
        display_results = results[:limit]
        if len(results) > limit:
            print(f"   Showing first {limit} rows:")
        
        print(tabulate(display_results, headers=columns, tablefmt="grid", floatfmt=".2f"))
        
        if len(results) > limit:
            print(f"   ... and {len(results) - limit} more rows")
    
    def run_product_cooccurrence_analysis(self):
        """Analyze which products are frequently bought together"""
        
        # Query 1: Most frequent product pairs in same orders
        query1 = """
        WITH product_pairs AS (
            SELECT 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END as product_a_id,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END as product_b_id,
                COUNT(DISTINCT oi1.order_id) as times_bought_together,
                SUM(oi1.quantity * oi2.quantity) as total_quantity_pairs,
                AVG(oi1.unit_price + oi2.unit_price) as avg_combined_price
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            WHERE oi1.product_id != oi2.product_id
            GROUP BY 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END
            HAVING COUNT(DISTINCT oi1.order_id) >= 3
        )
        SELECT 
            p1.product_name as product_a,
            p2.product_name as product_b,
            p1.brand as brand_a,
            p2.brand as brand_b,
            pp.times_bought_together,
            pp.total_quantity_pairs,
            pp.avg_combined_price,
            ROUND((pp.times_bought_together::decimal / 
                  (SELECT COUNT(DISTINCT order_id) FROM orders) * 100), 2) as percentage_of_orders
        FROM product_pairs pp
        JOIN products p1 ON pp.product_a_id = p1.product_id
        JOIN products p2 ON pp.product_b_id = p2.product_id
        ORDER BY pp.times_bought_together DESC, pp.avg_combined_price DESC
        LIMIT 20
        """
        
        self.execute_query(
            "frequent_product_pairs",
            query1,
            "Products most frequently bought together in the same order"
        )
    
    def run_market_basket_analysis(self):
        """Advanced market basket analysis with support and confidence metrics"""
        
        # Query 2: Market basket analysis with support, confidence, and lift
        query2 = """
        WITH order_stats AS (
            SELECT COUNT(DISTINCT order_id) as total_orders FROM orders
        ),
        product_support AS (
            SELECT 
                product_id,
                COUNT(DISTINCT order_id) as product_orders,
                COUNT(DISTINCT order_id)::decimal / (SELECT total_orders FROM order_stats) as support
            FROM order_items
            GROUP BY product_id
        ),
        product_pairs AS (
            SELECT 
                oi1.product_id as product_a,
                oi2.product_id as product_b,
                COUNT(DISTINCT oi1.order_id) as pair_orders
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            WHERE oi1.product_id < oi2.product_id
            GROUP BY oi1.product_id, oi2.product_id
            HAVING COUNT(DISTINCT oi1.order_id) >= 2
        )
        SELECT 
            p1.product_name as product_a,
            p2.product_name as product_b,
            pp.pair_orders,
            ROUND(pp.pair_orders::decimal / (SELECT total_orders FROM order_stats) * 100, 2) as support_percent,
            ROUND(pp.pair_orders::decimal / ps1.product_orders * 100, 2) as confidence_a_to_b,
            ROUND(pp.pair_orders::decimal / ps2.product_orders * 100, 2) as confidence_b_to_a,
            ROUND(
                (pp.pair_orders::decimal / (SELECT total_orders FROM order_stats)) / 
                (ps1.support * ps2.support), 2
            ) as lift
        FROM product_pairs pp
        JOIN products p1 ON pp.product_a = p1.product_id
        JOIN products p2 ON pp.product_b = p2.product_id
        JOIN product_support ps1 ON pp.product_a = ps1.product_id
        JOIN product_support ps2 ON pp.product_b = ps2.product_id
        WHERE pp.pair_orders >= 3
        ORDER BY lift DESC, support_percent DESC
        LIMIT 15
        """
        
        self.execute_query(
            "market_basket_analysis",
            query2,
            "Market basket analysis with support, confidence, and lift metrics"
        )
    
    def run_category_cross_selling_analysis(self):
        """Analyze cross-selling patterns between product categories"""
        
        # Query 3: Category cross-selling analysis
        query3 = """
        WITH category_pairs AS (
            SELECT 
                c1.category_name as category_a,
                c2.category_name as category_b,
                COUNT(DISTINCT oi1.order_id) as orders_with_both,
                SUM(oi1.quantity + oi2.quantity) as total_items,
                AVG(oi1.unit_price + oi2.unit_price) as avg_combined_price,
                COUNT(*) as total_combinations
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            JOIN categories c1 ON p1.category_id = c1.category_id
            JOIN categories c2 ON p2.category_id = c2.category_id
            WHERE c1.category_id != c2.category_id
            GROUP BY c1.category_name, c2.category_name
            HAVING COUNT(DISTINCT oi1.order_id) >= 2
        )
        SELECT 
            category_a,
            category_b,
            orders_with_both,
            total_items,
            ROUND(avg_combined_price, 2) as avg_combined_price,
            total_combinations,
            ROUND(orders_with_both::decimal / 
                  (SELECT COUNT(DISTINCT order_id) FROM orders) * 100, 2) as cross_sell_rate
        FROM category_pairs
        ORDER BY orders_with_both DESC, cross_sell_rate DESC
        LIMIT 20
        """
        
        self.execute_query(
            "category_cross_selling",
            query3,
            "Cross-selling patterns between different product categories"
        )
    
    def run_seasonal_product_patterns(self):
        """Analyze seasonal buying patterns and product combinations"""
        
        # Query 4: Seasonal patterns in product combinations
        query4 = """
        WITH monthly_pairs AS (
            SELECT 
                DATE_TRUNC('month', o.order_date) as order_month,
                p1.product_name as product_a,
                p2.product_name as product_b,
                COUNT(DISTINCT o.order_id) as monthly_combinations,
                SUM(oi1.quantity + oi2.quantity) as monthly_quantity,
                AVG(oi1.unit_price + oi2.unit_price) as avg_monthly_price
            FROM orders o
            JOIN order_items oi1 ON o.order_id = oi1.order_id
            JOIN order_items oi2 ON o.order_id = oi2.order_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            WHERE oi1.product_id < oi2.product_id
                AND o.order_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY DATE_TRUNC('month', o.order_date), p1.product_name, p2.product_name
            HAVING COUNT(DISTINCT o.order_id) >= 2
        )
        SELECT 
            TO_CHAR(order_month, 'YYYY-MM') as month,
            product_a,
            product_b,
            monthly_combinations,
            monthly_quantity,
            ROUND(avg_monthly_price, 2) as avg_price,
            RANK() OVER (PARTITION BY order_month ORDER BY monthly_combinations DESC) as rank_in_month
        FROM monthly_pairs
        WHERE RANK() OVER (PARTITION BY order_month ORDER BY monthly_combinations DESC) <= 3
        ORDER BY order_month DESC, monthly_combinations DESC
        """
        
        self.execute_query(
            "seasonal_patterns",
            query4,
            "Top 3 product combinations per month over the last year"
        )
    
    def run_customer_segment_analysis(self):
        """Analyze product combinations by customer segments"""
        
        # Query 5: Customer segment analysis
        query5 = """
        WITH customer_segments AS (
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name as customer_name,
                COUNT(DISTINCT o.order_id) as total_orders,
                SUM(o.total_amount) as total_spent,
                AVG(o.total_amount) as avg_order_value,
                CASE 
                    WHEN SUM(o.total_amount) >= 2000 THEN 'High Value'
                    WHEN SUM(o.total_amount) >= 500 THEN 'Medium Value'
                    ELSE 'Low Value'
                END as customer_segment
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name
        ),
        segment_product_pairs AS (
            SELECT 
                cs.customer_segment,
                p1.product_name as product_a,
                p2.product_name as product_b,
                COUNT(DISTINCT oi1.order_id) as combinations_in_segment,
                AVG(oi1.unit_price + oi2.unit_price) as avg_combined_price
            FROM customer_segments cs
            JOIN orders o ON cs.customer_id = o.customer_id
            JOIN order_items oi1 ON o.order_id = oi1.order_id
            JOIN order_items oi2 ON o.order_id = oi2.order_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            WHERE oi1.product_id < oi2.product_id
            GROUP BY cs.customer_segment, p1.product_name, p2.product_name
            HAVING COUNT(DISTINCT oi1.order_id) >= 2
        )
        SELECT 
            customer_segment,
            product_a,
            product_b,
            combinations_in_segment,
            ROUND(avg_combined_price, 2) as avg_combined_price,
            RANK() OVER (PARTITION BY customer_segment ORDER BY combinations_in_segment DESC) as rank_in_segment
        FROM segment_product_pairs
        WHERE RANK() OVER (PARTITION BY customer_segment ORDER BY combinations_in_segment DESC) <= 5
        ORDER BY customer_segment, combinations_in_segment DESC
        """
        
        self.execute_query(
            "customer_segment_analysis",
            query5,
            "Top 5 product combinations by customer value segments"
        )
    
    def run_brand_affinity_analysis(self):
        """Analyze brand affinity and cross-brand purchasing"""
        
        # Query 6: Brand affinity analysis
        query6 = """
        WITH brand_combinations AS (
            SELECT 
                p1.brand as brand_a,
                p2.brand as brand_b,
                COUNT(DISTINCT oi1.order_id) as orders_with_both_brands,
                COUNT(*) as total_item_combinations,
                AVG(oi1.unit_price + oi2.unit_price) as avg_combined_price,
                SUM(oi1.quantity + oi2.quantity) as total_quantities
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            WHERE p1.brand != p2.brand
                AND p1.brand IS NOT NULL 
                AND p2.brand IS NOT NULL
            GROUP BY p1.brand, p2.brand
            HAVING COUNT(DISTINCT oi1.order_id) >= 2
        ),
        brand_stats AS (
            SELECT 
                brand_a,
                brand_b,
                orders_with_both_brands,
                total_item_combinations,
                ROUND(avg_combined_price, 2) as avg_combined_price,
                total_quantities,
                ROUND(orders_with_both_brands::decimal / 
                      (SELECT COUNT(DISTINCT order_id) FROM orders) * 100, 2) as cross_brand_rate
            FROM brand_combinations
        )
        SELECT 
            brand_a,
            brand_b,
            orders_with_both_brands,
            total_item_combinations,
            avg_combined_price,
            total_quantities,
            cross_brand_rate,
            CASE 
                WHEN cross_brand_rate >= 5 THEN 'High Affinity'
                WHEN cross_brand_rate >= 2 THEN 'Medium Affinity'
                ELSE 'Low Affinity'
            END as affinity_level
        FROM brand_stats
        ORDER BY orders_with_both_brands DESC, cross_brand_rate DESC
        LIMIT 15
        """
        
        self.execute_query(
            "brand_affinity_analysis",
            query6,
            "Cross-brand purchasing patterns and brand affinity analysis"
        )
    
    def run_high_value_combinations(self):
        """Find the most valuable product combinations"""
        
        # Query 7: High-value product combinations
        query7 = """
        WITH valuable_combinations AS (
            SELECT 
                p1.product_name as product_a,
                p2.product_name as product_b,
                p1.price + p2.price as theoretical_combined_price,
                COUNT(DISTINCT oi1.order_id) as times_bought_together,
                SUM(oi1.unit_price + oi2.unit_price) as total_revenue,
                AVG(oi1.unit_price + oi2.unit_price) as avg_actual_combined_price,
                SUM(oi1.quantity + oi2.quantity) as total_units_sold,
                MIN(oi1.unit_price + oi2.unit_price) as min_combined_price,
                MAX(oi1.unit_price + oi2.unit_price) as max_combined_price
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            WHERE oi1.product_id < oi2.product_id
            GROUP BY p1.product_name, p2.product_name, p1.price, p2.price
            HAVING COUNT(DISTINCT oi1.order_id) >= 2
        )
        SELECT 
            product_a,
            product_b,
            times_bought_together,
            ROUND(total_revenue, 2) as total_revenue,
            ROUND(avg_actual_combined_price, 2) as avg_combined_price,
            total_units_sold,
            ROUND(min_combined_price, 2) as min_price,
            ROUND(max_combined_price, 2) as max_price,
            ROUND(total_revenue / times_bought_together, 2) as revenue_per_combination
        FROM valuable_combinations
        ORDER BY total_revenue DESC, times_bought_together DESC
        LIMIT 15
        """
        
        self.execute_query(
            "high_value_combinations",
            query7,
            "Most valuable product combinations by total revenue"
        )
    
    def run_all_analytics(self):
        """Run all analytics queries"""
        print("🚀 Starting Comprehensive Product Analytics\n")
        print("=" * 60)
        
        # Run all analytics
        self.run_product_cooccurrence_analysis()
        self.run_market_basket_analysis()
        self.run_category_cross_selling_analysis()
        self.run_seasonal_product_patterns()
        self.run_customer_segment_analysis()
        self.run_brand_affinity_analysis()
        self.run_high_value_combinations()
        
        # Display results
        print("\n" + "=" * 60)
        print("📊 ANALYTICS RESULTS")
        print("=" * 60)
        
        for query_name in self.query_results.keys():
            self.display_results(query_name, limit=8)
        
        # Performance summary
        self.display_performance_summary()
    
    def display_performance_summary(self):
        """Display performance metrics for all queries"""
        print("\n" + "=" * 60)
        print("⚡ PERFORMANCE SUMMARY")
        print("=" * 60)
        
        performance_data = []
        total_time = 0
        
        for query_name, metrics in self.performance_metrics.items():
            execution_time = metrics['execution_time']
            row_count = metrics['row_count']
            total_time += execution_time
            
            performance_data.append([
                query_name.replace('_', ' ').title(),
                f"{execution_time:.4f}s",
                f"{row_count:,}",
                f"{row_count/execution_time:.0f}" if execution_time > 0 else "N/A"
            ])
        
        headers = ["Query", "Execution Time", "Rows", "Rows/Second"]
        print(tabulate(performance_data, headers=headers, tablefmt="grid"))
        
        print(f"\n📈 Total execution time: {total_time:.4f}s")
        print(f"📊 Average query time: {total_time/len(self.performance_metrics):.4f}s")
        
        # Find slowest and fastest queries
        if self.performance_metrics:
            slowest = max(self.performance_metrics.items(), key=lambda x: x[1]['execution_time'])
            fastest = min(self.performance_metrics.items(), key=lambda x: x[1]['execution_time'])
            
            print(f"🐌 Slowest query: {slowest[0]} ({slowest[1]['execution_time']:.4f}s)")
            print(f"🚀 Fastest query: {fastest[0]} ({fastest[1]['execution_time']:.4f}s)")
    
    def export_results_to_json(self, filename="analytics_results.json"):
        """Export all results to JSON file"""
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'database_config': {k: v for k, v in self.db_config.items() if k != 'password'},
            'performance_metrics': {
                k: {
                    'execution_time': v['execution_time'],
                    'row_count': v['row_count'],
                    'timestamp': v['timestamp'].isoformat()
                } for k, v in self.performance_metrics.items()
            },
            'query_summaries': {
                k: {
                    'description': v['description'],
                    'row_count': v['row_count'],
                    'columns': v['columns']
                } for k, v in self.query_results.items()
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            print(f"📄 Results exported to {filename}")
        except Exception as e:
            print(f"❌ Failed to export results: {e}")


def load_environment():
    """Load environment variables"""
    return {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': int(config('DB_PORT', 5433)),
        'connect_timeout': int(config('DB_CONNECT_TIMEOUT', 10))
    }


def main():
    """Main function"""
    print("📊 Product Analytics & Performance Testing")
    print("=" * 50)
    
    # Load database configuration
    db_config = load_environment()
    
    # Initialize analytics
    analytics = ProductAnalytics(db_config)
    
    if not analytics.connect():
        return
    
    try:
        # Run all analytics
        analytics.run_all_analytics()
        
        # Export results
        analytics.export_results_to_json()
        
        print("\n🎉 Analytics completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Analytics interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        analytics.disconnect()


if __name__ == "__main__":
    main()