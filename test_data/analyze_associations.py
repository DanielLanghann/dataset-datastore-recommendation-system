"""
Product Association Analyzer
Analyzes and updates product associations based on actual order patterns
"""

import os
import psycopg2
from decouple import config
from tabulate import tabulate

def load_environment():
 
    
    return {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': config('DB_PORT', default = 5433, cast = int),
        'connect_timeout': config('DB_CONNECT_TIMEOUT', default=10, cast=int)
    }

def analyze_current_associations(connection):
    """Analyze current product associations"""
    cursor = connection.cursor()
    
    print("📊 Current Product Associations Analysis\n")
    
    # Get basic stats
    cursor.execute("SELECT COUNT(*) FROM product_associations")
    total_associations = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(frequency_count), MIN(frequency_count), MAX(frequency_count) FROM product_associations")
    avg_freq, min_freq, max_freq = cursor.fetchone()
    
    print(f"Total Associations: {total_associations}")
    print(f"Average Frequency: {avg_freq:.2f}")
    print(f"Min Frequency: {min_freq}")
    print(f"Max Frequency: {max_freq}\n")
    
    # Top associations
    cursor.execute("""
        SELECT 
            p1.product_name as product_a,
            p2.product_name as product_b,
            pa.frequency_count,
            pa.last_calculated::date
        FROM product_associations pa
        JOIN products p1 ON pa.product_a_id = p1.product_id
        JOIN products p2 ON pa.product_b_id = p2.product_id
        ORDER BY pa.frequency_count DESC
        LIMIT 10
    """)
    
    top_associations = cursor.fetchall()
    if top_associations:
        print("🔥 Top 10 Product Associations:")
        headers = ["Product A", "Product B", "Frequency", "Last Updated"]
        print(tabulate(top_associations, headers=headers, tablefmt="grid"))
    
    cursor.close()

def analyze_order_patterns(connection):
    """Analyze actual buying patterns from orders"""
    cursor = connection.cursor()
    
    print("\n📈 Order Pattern Analysis\n")
    
    # Products frequently bought together (but not in associations table)
    cursor.execute("""
        WITH order_pairs AS (
            SELECT 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END as product_a_id,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END as product_b_id,
                COUNT(*) as actual_frequency
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            WHERE oi1.product_id != oi2.product_id
            GROUP BY 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END
            HAVING COUNT(*) >= 2
        )
        SELECT 
            p1.product_name as product_a,
            p2.product_name as product_b,
            op.actual_frequency,
            COALESCE(pa.frequency_count, 0) as recorded_frequency,
            CASE 
                WHEN pa.association_id IS NULL THEN 'Missing'
                WHEN pa.frequency_count != op.actual_frequency THEN 'Outdated'
                ELSE 'Current'
            END as status
        FROM order_pairs op
        JOIN products p1 ON op.product_a_id = p1.product_id
        JOIN products p2 ON op.product_b_id = p2.product_id
        LEFT JOIN product_associations pa ON op.product_a_id = pa.product_a_id AND op.product_b_id = pa.product_b_id
        ORDER BY op.actual_frequency DESC
        LIMIT 15
    """)
    
    order_patterns = cursor.fetchall()
    if order_patterns:
        print("🛒 Actual vs Recorded Buying Patterns:")
        headers = ["Product A", "Product B", "Actual Freq", "Recorded Freq", "Status"]
        print(tabulate(order_patterns, headers=headers, tablefmt="grid"))
    
    cursor.close()

def update_associations_from_orders(connection):
    """Update product associations based on actual order data"""
    cursor = connection.cursor()
    
    print("\n🔄 Updating associations from order data...\n")
    
    try:
        # Insert/update associations based on actual order patterns
        cursor.execute("""
            INSERT INTO product_associations (product_a_id, product_b_id, frequency_count, last_calculated)
            SELECT 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END as product_a_id,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END as product_b_id,
                COUNT(*) as frequency_count,
                CURRENT_TIMESTAMP as last_calculated
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            WHERE oi1.product_id != oi2.product_id
            GROUP BY 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END
            HAVING COUNT(*) >= 2
            ON CONFLICT (product_a_id, product_b_id) 
            DO UPDATE SET 
                frequency_count = EXCLUDED.frequency_count,
                last_calculated = EXCLUDED.last_calculated
        """)
        
        rows_affected = cursor.rowcount
        connection.commit()
        
        print(f"✅ Updated {rows_affected} product associations")
        
        # Remove associations with frequency < 2 (not meaningful)
        cursor.execute("DELETE FROM product_associations WHERE frequency_count < 2")
        deleted_rows = cursor.rowcount
        connection.commit()
        
        if deleted_rows > 0:
            print(f"🗑️  Removed {deleted_rows} associations with frequency < 2")
        
    except psycopg2.Error as e:
        print(f"❌ Error updating associations: {e}")
        connection.rollback()
    
    cursor.close()

def find_missing_associations(connection, min_frequency=3):
    """Find product pairs that should have associations but don't"""
    cursor = connection.cursor()
    
    print(f"\n🔍 Finding missing associations (min frequency: {min_frequency})...\n")
    
    cursor.execute("""
        WITH order_pairs AS (
            SELECT 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END as product_a_id,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END as product_b_id,
                COUNT(*) as frequency
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            WHERE oi1.product_id != oi2.product_id
            GROUP BY 
                CASE WHEN oi1.product_id < oi2.product_id THEN oi1.product_id ELSE oi2.product_id END,
                CASE WHEN oi1.product_id < oi2.product_id THEN oi2.product_id ELSE oi1.product_id END
            HAVING COUNT(*) >= %s
        )
        SELECT 
            p1.product_name as product_a,
            p2.product_name as product_b,
            op.frequency
        FROM order_pairs op
        JOIN products p1 ON op.product_a_id = p1.product_id
        JOIN products p2 ON op.product_b_id = p2.product_id
        LEFT JOIN product_associations pa ON op.product_a_id = pa.product_a_id AND op.product_b_id = pa.product_b_id
        WHERE pa.association_id IS NULL
        ORDER BY op.frequency DESC
    """, (min_frequency,))
    
    missing_associations = cursor.fetchall()
    if missing_associations:
        print("❌ Missing Associations (should be added):")
        headers = ["Product A", "Product B", "Frequency"]
        print(tabulate(missing_associations, headers=headers, tablefmt="grid"))
    else:
        print("✅ No missing associations found!")
    
    cursor.close()

def main():
    """Main function"""
    print("🔍 Product Association Analyzer\n")
    
    # Load database configuration
    db_config = load_environment()
    
    try:
        connection = psycopg2.connect(**db_config)
        print(f"✅ Connected to database: {db_config['host']}:{db_config['port']}\n")
        
        # Run analysis
        analyze_current_associations(connection)
        analyze_order_patterns(connection)
        find_missing_associations(connection)
        
        # Ask user if they want to update
        response = input("\n💡 Would you like to update associations based on order data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            update_associations_from_orders(connection)
            print("\n📊 Updated Analysis:")
            analyze_current_associations(connection)
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    main()