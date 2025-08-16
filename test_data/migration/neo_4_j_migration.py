"""
Neo4j Migration Script for Product Associations
Migrates product association data from PostgreSQL to Neo4j for performance comparison
"""

import sys
import time
import psycopg2
from neo4j import GraphDatabase
from decouple import config
import argparse

class Neo4jMigrator:
    def __init__(self, postgres_config, neo4j_config):
        self.postgres_config = postgres_config
        self.neo4j_config = neo4j_config
        self.pg_connection = None
        self.neo4j_driver = None
        
    def connect_databases(self):
        """Connect to both PostgreSQL and Neo4j"""
        try:
            # Connect to PostgreSQL
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
            
            # Connect to Neo4j
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            
            # Test Neo4j connection
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 'Connection successful' as message")
                print(f"✅ Connected to Neo4j: {self.neo4j_config['uri']}")
                
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_databases(self):
        """Close database connections"""
        if self.pg_connection:
            self.pg_connection.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
    
    def setup_neo4j_schema(self):
        """Create Neo4j schema for product associations"""
        print("🔧 Setting up Neo4j schema...")
        
        schema_queries = [
            # Create constraints for unique product IDs
            "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
            
            # Create constraints for unique category IDs
            "CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE",
            
            # Create indexes for performance
            "CREATE INDEX product_name_index IF NOT EXISTS FOR (p:Product) ON (p.product_name)",
            "CREATE INDEX product_brand_index IF NOT EXISTS FOR (p:Product) ON (p.brand)",
            "CREATE INDEX category_name_index IF NOT EXISTS FOR (c:Category) ON (c.category_name)",
            "CREATE INDEX association_frequency_index IF NOT EXISTS FOR ()-[r:BOUGHT_TOGETHER]-() ON (r.frequency_count)",
        ]
        
        try:
            with self.neo4j_driver.session() as session:
                for query in schema_queries:
                    try:
                        session.run(query)
                        print(f"   ✅ Executed: {query.split()[1:4]} constraint/index")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            print(f"   ℹ️  Already exists: {query.split()[1:4]} constraint/index")
                        else:
                            print(f"   ⚠️  Warning: {e}")
            
            print("✅ Neo4j schema setup completed")
            return True
            
        except Exception as e:
            print(f"❌ Error setting up Neo4j schema: {e}")
            return False
    
    def migrate_categories(self):
        """Migrate categories to Neo4j"""
        print("📦 Migrating categories to Neo4j...")
        
        try:
            # Fetch categories from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT category_id, category_name, description, created_at, updated_at
                FROM categories
                ORDER BY category_id
            """)
            categories = cursor.fetchall()
            cursor.close()
            
            if not categories:
                print("   ⚠️  No categories found in PostgreSQL")
                return True
            
            # Insert categories into Neo4j
            with self.neo4j_driver.session() as session:
                # Clear existing categories
                session.run("MATCH (c:Category) DETACH DELETE c")
                
                # Insert new categories
                for category in categories:
                    category_id, name, description, created_at, updated_at = category
                    
                    session.run("""
                        CREATE (c:Category {
                            category_id: $category_id,
                            category_name: $name,
                            description: $description,
                            created_at: $created_at,
                            updated_at: $updated_at
                        })
                    """, {
                        'category_id': category_id,
                        'name': name,
                        'description': description,
                        'created_at': created_at.isoformat() if created_at else None,
                        'updated_at': updated_at.isoformat() if updated_at else None
                    })
            
            print(f"   ✅ Migrated {len(categories)} categories")
            return True
            
        except Exception as e:
            print(f"   ❌ Error migrating categories: {e}")
            return False
    
    def migrate_products(self):
        """Migrate products to Neo4j"""
        print("📦 Migrating products to Neo4j...")
        
        try:
            # Fetch products from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT p.product_id, p.product_name, p.description, p.price, 
                       p.category_id, p.brand, p.stock_qty, p.is_active,
                       p.created_at, p.updated_at, c.category_name
                FROM products p
                JOIN categories c ON p.category_id = c.category_id
                WHERE p.is_active = true
                ORDER BY p.product_id
            """)
            products = cursor.fetchall()
            cursor.close()
            
            if not products:
                print("   ⚠️  No products found in PostgreSQL")
                return True
            
            # Insert products into Neo4j in batches
            batch_size = 1000
            total_batches = (len(products) + batch_size - 1) // batch_size
            
            with self.neo4j_driver.session() as session:
                # Clear existing products
                session.run("MATCH (p:Product) DETACH DELETE p")
                
                for i in range(0, len(products), batch_size):
                    batch = products[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    # Create products in batch
                    for product in batch:
                        (product_id, name, description, price, category_id, brand, 
                         stock_qty, is_active, created_at, updated_at, category_name) = product
                        
                        session.run("""
                            MERGE (p:Product {product_id: $product_id})
                            SET p.product_name = $name,
                                p.description = $description,
                                p.price = $price,
                                p.category_id = $category_id,
                                p.brand = $brand,
                                p.stock_qty = $stock_qty,
                                p.is_active = $is_active,
                                p.created_at = $created_at,
                                p.updated_at = $updated_at,
                                p.category_name = $category_name
                        """, {
                            'product_id': product_id,
                            'name': name,
                            'description': description,
                            'price': float(price) if price else 0.0,
                            'category_id': category_id,
                            'brand': brand,
                            'stock_qty': stock_qty,
                            'is_active': is_active,
                            'created_at': created_at.isoformat() if created_at else None,
                            'updated_at': updated_at.isoformat() if updated_at else None,
                            'category_name': category_name
                        })
                    
                    # Create relationships between products and categories
                    session.run("""
                        MATCH (p:Product), (c:Category)
                        WHERE p.category_id = c.category_id
                        AND p.product_id IN [product['product_id'] for product in $batch]
                        MERGE (p)-[:BELONGS_TO]->(c)
                    """, {'batch': [{'product_id': p[0]} for p in batch]})
                    
                    print(f"   📦 Batch {batch_num}/{total_batches} completed ({len(batch)} products)")
            
            print(f"   ✅ Migrated {len(products)} products with category relationships")
            return True
            
        except Exception as e:
            print(f"   ❌ Error migrating products: {e}")
            return False
    
    def migrate_product_associations(self):
        """Migrate product associations to Neo4j as relationships"""
        print("🔗 Migrating product associations to Neo4j...")
        
        try:
            # Fetch product associations from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT pa.association_id, pa.product_a_id, pa.product_b_id, 
                       pa.frequency_count, pa.last_calculated,
                       p1.product_name as product_a_name, p1.brand as product_a_brand,
                       p2.product_name as product_b_name, p2.brand as product_b_brand
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                WHERE p1.is_active = true AND p2.is_active = true
                ORDER BY pa.frequency_count DESC
            """)
            associations = cursor.fetchall()
            cursor.close()
            
            if not associations:
                print("   ⚠️  No product associations found in PostgreSQL")
                return True
            
            # Insert associations into Neo4j as relationships
            batch_size = 500
            total_batches = (len(associations) + batch_size - 1) // batch_size
            
            with self.neo4j_driver.session() as session:
                # Clear existing associations
                session.run("MATCH ()-[r:BOUGHT_TOGETHER]-() DELETE r")
                
                for i in range(0, len(associations), batch_size):
                    batch = associations[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    for association in batch:
                        (assoc_id, product_a_id, product_b_id, frequency_count, 
                         last_calculated, product_a_name, product_a_brand,
                         product_b_name, product_b_brand) = association
                        
                        session.run("""
                            MATCH (p1:Product {product_id: $product_a_id})
                            MATCH (p2:Product {product_id: $product_b_id})
                            CREATE (p1)-[r:BOUGHT_TOGETHER {
                                association_id: $assoc_id,
                                frequency_count: $frequency_count,
                                last_calculated: $last_calculated,
                                strength: $frequency_count
                            }]->(p2)
                        """, {
                            'product_a_id': product_a_id,
                            'product_b_id': product_b_id,
                            'assoc_id': assoc_id,
                            'frequency_count': frequency_count,
                            'last_calculated': last_calculated.isoformat() if last_calculated else None
                        })
                    
                    print(f"   🔗 Batch {batch_num}/{total_batches} completed ({len(batch)} associations)")
            
            print(f"   ✅ Migrated {len(associations)} product associations as relationships")
            return True
            
        except Exception as e:
            print(f"   ❌ Error migrating product associations: {e}")
            return False
    
    def verify_migration(self):
        """Verify the migration was successful"""
        print("🔍 Verifying migration...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Count nodes and relationships
                category_count = session.run("MATCH (c:Category) RETURN count(c) as count").single()['count']
                product_count = session.run("MATCH (p:Product) RETURN count(p) as count").single()['count']
                belongs_to_count = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count").single()['count']
                bought_together_count = session.run("MATCH ()-[r:BOUGHT_TOGETHER]->() RETURN count(r) as count").single()['count']
                
                print(f"   📊 Neo4j Data Summary:")
                print(f"      Categories: {category_count:,}")
                print(f"      Products: {product_count:,}")
                print(f"      Product-Category relationships: {belongs_to_count:,}")
                print(f"      Product-Association relationships: {bought_together_count:,}")
                
                # Test a sample query
                sample_result = session.run("""
                    MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
                    RETURN p1.product_name, p2.product_name, r.frequency_count
                    ORDER BY r.frequency_count DESC
                    LIMIT 3
                """)
                
                print(f"   🔍 Sample associations:")
                for record in sample_result:
                    print(f"      {record['p1.product_name']} ↔ {record['p2.product_name']} (freq: {record['r.frequency_count']})")
                
                if product_count > 0 and bought_together_count > 0:
                    print("   ✅ Migration verification successful")
                    return True
                else:
                    print("   ❌ Migration verification failed - missing data")
                    return False
                
        except Exception as e:
            print(f"   ❌ Error verifying migration: {e}")
            return False
    
    def run_performance_test(self):
        """Run a quick performance test"""
        print("⚡ Running performance test...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Test query performance
                start_time = time.time()
                
                result = session.run("""
                    MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
                    WHERE r.frequency_count >= 3
                    RETURN p1.product_name as product_a,
                           p1.brand as brand_a,
                           p2.product_name as product_b,
                           p2.brand as brand_b,
                           r.frequency_count
                    ORDER BY r.frequency_count DESC
                    LIMIT 100
                """)
                
                records = list(result)
                end_time = time.time()
                
                execution_time_ms = (end_time - start_time) * 1000
                
                print(f"   ⚡ Neo4j Query Performance:")
                print(f"      Query time: {execution_time_ms:.2f}ms")
                print(f"      Results returned: {len(records)}")
                print(f"      Records per second: {len(records) / (execution_time_ms / 1000):.0f}")
                
                if records:
                    print(f"   🔍 Top association: {records[0]['product_a']} ↔ {records[0]['product_b']} (freq: {records[0]['r.frequency_count']})")
                
                return True
                
        except Exception as e:
            print(f"   ❌ Error running performance test: {e}")
            return False


def load_environment():
    """Load database configurations"""
    postgres_config = {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': int(config('DB_PORT', 5433)),
        'connect_timeout': int(config('DB_CONNECT_TIMEOUT', 10))
    }
    
    neo4j_config = {
        'uri': config('NEO4J_URI', 'bolt://localhost:7687'),
        'user': config('NEO4J_USER', 'neo4j'),
        'password': config('NEO4J_PASSWORD', config('NEO4J_PASSWORD', 'neo4j_root_password'))
    }
    
    return postgres_config, neo4j_config


def main():
    parser = argparse.ArgumentParser(description='Migrate product associations from PostgreSQL to Neo4j')
    parser.add_argument('--verify-only', action='store_true', help='Only verify existing migration')
    parser.add_argument('--test-only', action='store_true', help='Only run performance test')
    parser.add_argument('--skip-verification', action='store_true', help='Skip migration verification')
    
    args = parser.parse_args()
    
    print("🚀 Neo4j Migration Tool for Product Associations")
    print("=" * 60)
    
    # Load configurations
    postgres_config, neo4j_config = load_environment()
    
    # Initialize migrator
    migrator = Neo4jMigrator(postgres_config, neo4j_config)
    
    if not migrator.connect_databases():
        sys.exit(1)
    
    try:
        if args.verify_only:
            migrator.verify_migration()
        elif args.test_only:
            migrator.run_performance_test()
        else:
            # Full migration process
            success = True
            
            success &= migrator.setup_neo4j_schema()
            success &= migrator.migrate_categories()
            success &= migrator.migrate_products()
            success &= migrator.migrate_product_associations()
            
            if success and not args.skip_verification:
                success &= migrator.verify_migration()
                
            if success:
                migrator.run_performance_test()
                print("\n🎉 Migration completed successfully!")
                print("💡 Neo4j is now ready for product association queries")
                print("💡 Use --verify-only to check migration status later")
                print("💡 Use --test-only to run performance tests")
            else:
                print("\n❌ Migration failed or incomplete")
                sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n⏹️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        migrator.disconnect_databases()


if __name__ == "__main__":
    main()