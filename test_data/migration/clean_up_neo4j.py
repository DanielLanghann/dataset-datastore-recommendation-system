"""
Clean Start Neo4j Migration Script
Complete clean import in correct order with proper error handling
"""

import sys
import time
import psycopg2
from neo4j import GraphDatabase
from decouple import config
import argparse

class CleanMigrator:
    def __init__(self, postgres_config, neo4j_config):
        self.postgres_config = postgres_config
        self.neo4j_config = neo4j_config
        self.pg_connection = None
        self.neo4j_driver = None
        
    def connect_databases(self):
        """Connect to both databases"""
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
            
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 'Connection successful' as message")
                print(f"✅ Connected to Neo4j: {self.neo4j_config['uri']}")
                
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_databases(self):
        if self.pg_connection:
            self.pg_connection.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
    
    def clean_neo4j(self):
        """Completely clean Neo4j database"""
        print("🧹 Cleaning Neo4j database...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Count existing data
                count_result = session.run("""
                    MATCH (n) 
                    RETURN count(n) as total_nodes
                """)
                total_nodes = count_result.single()['total_nodes']
                
                if total_nodes > 0:
                    print(f"   Found {total_nodes:,} existing nodes")
                    
                    # Delete everything
                    session.run("MATCH (n) DETACH DELETE n")
                    
                    # Verify deletion
                    verify_result = session.run("MATCH (n) RETURN count(n) as remaining")
                    remaining = verify_result.single()['remaining']
                    
                    if remaining == 0:
                        print("   ✅ Database cleaned successfully")
                    else:
                        print(f"   ⚠️  {remaining} nodes still remain")
                        return False
                else:
                    print("   ✅ Database already clean")
                
                return True
                
        except Exception as e:
            print(f"   ❌ Error cleaning database: {e}")
            return False
    
    def setup_schema(self):
        """Create Neo4j schema"""
        print("🔧 Setting up Neo4j schema...")
        
        schema_queries = [
            "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
            "CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE",
            "CREATE INDEX product_name_index IF NOT EXISTS FOR (p:Product) ON (p.product_name)",
            "CREATE INDEX product_brand_index IF NOT EXISTS FOR (p:Product) ON (p.brand)",
            "CREATE INDEX category_name_index IF NOT EXISTS FOR (c:Category) ON (c.category_name)",
            "CREATE INDEX association_frequency_index IF NOT EXISTS FOR ()-[r:BOUGHT_TOGETHER]-() ON (r.frequency_count)",
        ]
        
        try:
            with self.neo4j_driver.session() as session:
                for query in schema_queries:
                    session.run(query)
                    constraint_type = query.split()[1]
                    print(f"   ✅ Created {constraint_type}")
            
            print("✅ Schema setup completed")
            return True
            
        except Exception as e:
            print(f"❌ Error setting up schema: {e}")
            return False
    
    def import_categories(self):
        """Import categories from PostgreSQL"""
        print("📁 Importing categories...")
        
        try:
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT category_id, category_name, description, created_at, updated_at
                FROM categories
                ORDER BY category_id
            """)
            categories = cursor.fetchall()
            cursor.close()
            
            if not categories:
                print("   ⚠️  No categories found")
                return True
            
            with self.neo4j_driver.session() as session:
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
            
            print(f"   ✅ Imported {len(categories)} categories")
            return True
            
        except Exception as e:
            print(f"   ❌ Error importing categories: {e}")
            return False
    
    def import_products(self):
        """Import ALL products from PostgreSQL - FIXED VERSION"""
        print("📦 Importing products...")
        
        try:
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
                print("   ⚠️  No products found")
                return True
            
            print(f"   Found {len(products):,} products to import")
            
            batch_size = 1000
            total_batches = (len(products) + batch_size - 1) // batch_size
            successful_imports = 0
            
            with self.neo4j_driver.session() as session:
                for i in range(0, len(products), batch_size):
                    batch = products[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    try:
                        for product in batch:
                            (product_id, name, description, price, category_id, brand, 
                             stock_qty, is_active, created_at, updated_at, category_name) = product
                            
                            session.run("""
                                CREATE (p:Product {
                                    product_id: $product_id,
                                    product_name: $name,
                                    description: $description,
                                    price: $price,
                                    category_id: $category_id,
                                    brand: $brand,
                                    stock_qty: $stock_qty,
                                    is_active: $is_active,
                                    created_at: $created_at,
                                    updated_at: $updated_at,
                                    category_name: $category_name
                                })
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
                            
                            successful_imports += 1
                        
                        print(f"   📦 Batch {batch_num}/{total_batches} completed ({len(batch):,} products)")
                        
                    except Exception as batch_error:
                        print(f"   ⚠️  Error in batch {batch_num}: {batch_error}")
                        continue
                
                # Create product-category relationships - FIXED VERSION
                print("   🔗 Creating product-category relationships...")
                relationship_result = session.run("""
                    MATCH (p:Product), (c:Category)
                    WHERE p.category_id = c.category_id
                    CREATE (p)-[:BELONGS_TO]->(c)
                    RETURN count(*) as relationships_created
                """)
                
                relationships_count = relationship_result.single()['relationships_created']
                print(f"   ✅ Created {relationships_count:,} product-category relationships")
            
            print(f"   ✅ Successfully imported {successful_imports:,} products")
            return True
            
        except Exception as e:
            print(f"   ❌ Error importing products: {e}")
            return False
    
    def import_associations(self):
        """Import product associations - PROPER VERSION using MATCH"""
        print("🔗 Importing product associations...")
        
        try:
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT pa.association_id, pa.product_a_id, pa.product_b_id, 
                       pa.frequency_count, pa.last_calculated
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                WHERE p1.is_active = true AND p2.is_active = true
                ORDER BY pa.frequency_count DESC
            """)
            associations = cursor.fetchall()
            cursor.close()
            
            if not associations:
                print("   ⚠️  No product associations found")
                return True
            
            print(f"   Found {len(associations):,} associations to import")
            
            batch_size = 500
            total_batches = (len(associations) + batch_size - 1) // batch_size
            successful_associations = 0
            failed_associations = 0
            
            with self.neo4j_driver.session() as session:
                for i in range(0, len(associations), batch_size):
                    batch = associations[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    for association in batch:
                        (assoc_id, product_a_id, product_b_id, frequency_count, last_calculated) = association
                        
                        try:
                            # Use MATCH - this will only create relationships if BOTH products exist
                            result = session.run("""
                                MATCH (p1:Product {product_id: $product_a_id})
                                MATCH (p2:Product {product_id: $product_b_id})
                                CREATE (p1)-[r:BOUGHT_TOGETHER {
                                    association_id: $assoc_id,
                                    frequency_count: $frequency_count,
                                    last_calculated: $last_calculated,
                                    strength: $frequency_count
                                }]->(p2)
                                RETURN r
                            """, {
                                'product_a_id': product_a_id,
                                'product_b_id': product_b_id,
                                'assoc_id': assoc_id,
                                'frequency_count': frequency_count,
                                'last_calculated': last_calculated.isoformat() if last_calculated else None
                            })
                            
                            if result.single():
                                successful_associations += 1
                            else:
                                failed_associations += 1
                                
                        except Exception:
                            failed_associations += 1
                    
                    print(f"   🔗 Batch {batch_num}/{total_batches} completed ({len(batch)} associations)")
            
            print(f"   ✅ Created {successful_associations:,} associations")
            if failed_associations > 0:
                print(f"   ⚠️  {failed_associations:,} associations failed (products not found)")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error importing associations: {e}")
            return False
    
    def verify_import(self):
        """Verify the complete import"""
        print("🔍 Verifying import...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Count everything
                result = session.run("""
                    MATCH (c:Category)
                    OPTIONAL MATCH (p:Product)
                    OPTIONAL MATCH (p)-[:BELONGS_TO]->(cat:Category)
                    OPTIONAL MATCH ()-[a:BOUGHT_TOGETHER]-()
                    RETURN 
                      count(DISTINCT c) as categories,
                      count(DISTINCT p) as products,
                      count(DISTINCT cat) as products_with_categories,
                      count(DISTINCT a) as associations
                """)
                
                stats = result.single()
                
                print(f"   📊 Final counts:")
                print(f"      Categories: {stats['categories']:,}")
                print(f"      Products: {stats['products']:,}")
                print(f"      Products with categories: {stats['products_with_categories']:,}")
                print(f"      Associations: {stats['associations']:,}")
                
                # Check for issues
                issues = []
                if stats['products'] != stats['products_with_categories']:
                    issues.append(f"{stats['products'] - stats['products_with_categories']} products missing category relationships")
                
                if not issues:
                    print("   ✅ Import verification successful - no issues found")
                    return True
                else:
                    print("   ⚠️  Issues found:")
                    for issue in issues:
                        print(f"      • {issue}")
                    return False
                
        except Exception as e:
            print(f"   ❌ Error verifying import: {e}")
            return False


def load_environment():
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
        'password': config('NEO4J_PASSWORD', 'neo4j_root_password')
    }
    
    return postgres_config, neo4j_config


def main():
    parser = argparse.ArgumentParser(description='Clean Neo4j migration from PostgreSQL')
    parser.add_argument('--skip-clean', action='store_true', help='Skip cleaning existing data')
    
    args = parser.parse_args()
    
    print("🚀 Clean Neo4j Migration Tool")
    print("=" * 50)
    
    postgres_config, neo4j_config = load_environment()
    migrator = CleanMigrator(postgres_config, neo4j_config)
    
    if not migrator.connect_databases():
        sys.exit(1)
    
    try:
        success = True
        start_time = time.time()
        
        # Clean database
        if not args.skip_clean:
            success &= migrator.clean_neo4j()
        
        if success:
            success &= migrator.setup_schema()
            success &= migrator.import_categories()
            success &= migrator.import_products()
            success &= migrator.import_associations()
            success &= migrator.verify_import()
        
        end_time = time.time()
        total_time = (end_time - start_time) / 60
        
        if success:
            print(f"\n🎉 Clean migration completed successfully!")
            print(f"⏱️  Total time: {total_time:.1f} minutes")
            print("💡 Neo4j is ready for product association queries")
        else:
            print(f"\n❌ Migration failed or incomplete")
            
    except KeyboardInterrupt:
        print("\n⏹️  Migration interrupted")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        migrator.disconnect_databases()


if __name__ == "__main__":
    main()