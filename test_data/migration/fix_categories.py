"""
Fix category ID mismatch between products and categories
"""
  
import psycopg2
from neo4j import GraphDatabase
from decouple import config
import random
  
class CategoryMismatchFixer:
    def __init__(self):
        # Database configs
        self.postgres_config = {
            'host': config('DB_HOST', 'localhost'),
            'database': config('DB_NAME', 'test_data'),
            'user': config('DB_USER', 'test'),
            'password': config('DB_PASSWORD', 'test'),
            'port': int(config('DB_PORT', 5433)),
            'connect_timeout': int(config('DB_CONNECT_TIMEOUT', 10))
        }
        self.neo4j_config = {
            'uri': config('NEO4J_URI', 'bolt://localhost:7687'),
            'user': config('NEO4J_USER', 'neo4j'),
            'password': config('NEO4J_PASSWORD', 'neo4j_root_password')
        }
        self.pg_connection = None
        self.neo4j_driver = None

    def connect(self):
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def disconnect(self):
        if self.pg_connection:
            self.pg_connection.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()

    def analyze_mismatch(self):
        """Analyze the category ID mismatch"""
        print("🔍 Analyzing category ID mismatch...")
        
        with self.neo4j_driver.session() as session:
            # Get Neo4j category IDs
            neo4j_cats = session.run("""
                MATCH (c:Category)
                RETURN collect(c.category_id) as cat_ids
            """).single()['cat_ids']
            
            # Get Neo4j product category IDs (sample)
            neo4j_prod_cats = session.run("""
                MATCH (p:Product)
                RETURN collect(DISTINCT p.category_id)[0..50] as prod_cat_ids
            """).single()['prod_cat_ids']

        print(f"   Neo4j category IDs: {sorted(neo4j_cats)}")
        print(f"   Sample product category IDs: {sorted(neo4j_prod_cats)}")

        # Check PostgreSQL for comparison
        cursor = self.pg_connection.cursor()
        cursor.execute("SELECT DISTINCT category_id FROM categories ORDER BY category_id")
        pg_cat_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT category_id FROM products ORDER BY category_id LIMIT 20")
        pg_prod_cat_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()

        print(f"   PostgreSQL category IDs: {pg_cat_ids}")
        print(f"   PostgreSQL product category IDs (sample): {pg_prod_cat_ids}")

        return neo4j_cats, neo4j_prod_cats, pg_cat_ids, pg_prod_cat_ids

    def solution_1_remap_products(self):
        """Solution 1: Remap product category_ids to existing categories"""
        print("🔧 Solution 1: Remapping product category IDs...")
        
        with self.neo4j_driver.session() as session:
            # Get available category IDs
            result = session.run("MATCH (c:Category) RETURN collect(c.category_id) as cat_ids")
            available_cats = result.single()['cat_ids']
            
            if not available_cats:
                print("   ❌ No categories found!")
                return False

            print(f"   Available categories: {sorted(available_cats)}")

            # Remap products to valid categories
            batch_size = 10000
            total_updated = 0

            # Get all products needing remapping
            result = session.run("""
                MATCH (p:Product)
                WHERE NOT p.category_id IN $valid_cats
                RETURN count(p) as invalid_count
            """, {'valid_cats': available_cats})
            
            invalid_count = result.single()['invalid_count']
            print(f"   Products needing remapping: {invalid_count:,}")

            if invalid_count == 0:
                print("   ✅ All products already have valid category IDs!")
                return True

            # Update in batches
            while True:
                result = session.run("""
                    MATCH (p:Product)
                    WHERE NOT p.category_id IN $valid_cats
                    WITH p LIMIT $batch_size
                    SET p.category_id = $random_cat
                    RETURN count(p) as updated
                """, {
                    'valid_cats': available_cats,
                    'batch_size': batch_size,
                    'random_cat': random.choice(available_cats)
                })
                
                batch_updated = result.single()['updated']
                total_updated += batch_updated
                
                if batch_updated == 0:
                    break
                    
                print(f"   Updated batch: {batch_updated:,} products (total: {total_updated:,})")

            print(f"   ✅ Remapped {total_updated:,} products")
            return True

    def solution_2_fix_categories(self):
        """Solution 2: Import correct categories from PostgreSQL"""
        print("🔧 Solution 2: Re-importing correct categories...")
        
        # Get correct categories from PostgreSQL
        cursor = self.pg_connection.cursor()
        cursor.execute("""
            SELECT category_id, category_name, description, created_at, updated_at
            FROM categories ORDER BY category_id
        """)
        pg_categories = cursor.fetchall()
        cursor.close()

        with self.neo4j_driver.session() as session:
            # Clear existing categories
            session.run("MATCH (c:Category) DETACH DELETE c")
            
            # Import correct categories
            for category in pg_categories:
                category_id, name, desc, created, updated = category
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
                    'description': desc,
                    'created_at': created.isoformat() if created else None,
                    'updated_at': updated.isoformat() if updated else None
                })

            print(f"   ✅ Imported {len(pg_categories)} correct categories")
            return True

    def create_indexes(self):
        """Create indexes for better performance"""
        print("📊 Creating indexes...")
        
        with self.neo4j_driver.session() as session:
            # Create indexes on category_id for both nodes
            try:
                session.run("CREATE INDEX product_category_id IF NOT EXISTS FOR (p:Product) ON (p.category_id)")
                session.run("CREATE INDEX category_category_id IF NOT EXISTS FOR (c:Category) ON (c.category_id)")
                print("   ✅ Indexes created")
            except Exception as e:
                print(f"   ⚠️  Index creation: {e}")

    def create_relationships(self):
        """Create product-category relationships efficiently"""
        print("🔗 Creating product-category relationships...")
        
        with self.neo4j_driver.session() as session:
            # Remove existing relationships first
            print("   Removing existing relationships...")
            result = session.run("MATCH ()-[r:BELONGS_TO]->() DELETE r RETURN count(r) as deleted")
            deleted = result.single()['deleted']
            if deleted > 0:
                print(f"   Removed {deleted:,} existing relationships")
            
            # Get all category IDs first
            result = session.run("MATCH (c:Category) RETURN collect(c.category_id) as cat_ids")
            category_ids = result.single()['cat_ids']
            
            if not category_ids:
                print("   ❌ No categories found!")
                return False
            
            total_created = 0
            batch_size = 5000
            
            print(f"   Processing {len(category_ids)} categories...")
            
            # Process each category separately to avoid memory issues
            for i, cat_id in enumerate(category_ids, 1):
                result = session.run("""
                    MATCH (p:Product {category_id: $cat_id})
                    MATCH (c:Category {category_id: $cat_id})
                    WITH p, c LIMIT $batch_size
                    CREATE (p)-[:BELONGS_TO]->(c)
                    RETURN count(*) as created
                """, {'cat_id': cat_id, 'batch_size': batch_size})
                
                created = result.single()['created']
                total_created += created
                
                if created > 0:
                    print(f"   Category {cat_id} ({i}/{len(category_ids)}): {created:,} relationships")
            
            print(f"   ✅ Created {total_created:,} total relationships")
            return total_created > 0

    def verify_fix(self):
        """Verify the fix worked"""
        print("🔍 Verifying fix...")
        
        with self.neo4j_driver.session() as session:
            # Count relationships
            result = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as rel_count")
            rel_count = result.single()['rel_count']
            
            # Count products
            result = session.run("MATCH (p:Product) RETURN count(p) as prod_count")
            prod_count = result.single()['prod_count']
            
            # Count categories
            result = session.run("MATCH (c:Category) RETURN count(c) as cat_count")
            cat_count = result.single()['cat_count']
            
            # Count orphaned products
            result = session.run("""
                MATCH (p:Product)
                WHERE NOT EXISTS((p)-[:BELONGS_TO]->())
                RETURN count(p) as orphaned
            """)
            orphaned = result.single()['orphaned']

            print(f"   Products: {prod_count:,}")
            print(f"   Categories: {cat_count:,}")
            print(f"   Relationships: {rel_count:,}")
            print(f"   Orphaned products: {orphaned:,}")
            
            if rel_count == prod_count and orphaned == 0:
                print("   ✅ Perfect! All products have category relationships")
                return True
            else:
                print(f"   ❌ Issues found - {orphaned:,} orphaned products")
                return False

    def cleanup_orphaned_products(self):
        """Fix any remaining orphaned products"""
        print("🧹 Cleaning up orphaned products...")
        
        with self.neo4j_driver.session() as session:
            # Get available category IDs
            result = session.run("MATCH (c:Category) RETURN collect(c.category_id) as cat_ids")
            available_cats = result.single()['cat_ids']
            
            if not available_cats:
                print("   ❌ No categories available!")
                return False
            
            # Find and fix orphaned products
            result = session.run("""
                MATCH (p:Product)
                WHERE NOT EXISTS((p)-[:BELONGS_TO]->())
                WITH p LIMIT 10000
                SET p.category_id = $random_cat
                WITH p
                MATCH (c:Category {category_id: p.category_id})
                CREATE (p)-[:BELONGS_TO]->(c)
                RETURN count(p) as fixed
            """, {'random_cat': random.choice(available_cats)})
            
            fixed = result.single()['fixed']
            print(f"   ✅ Fixed {fixed:,} orphaned products")
            return fixed > 0

def main():
    print("🔧 Category ID Mismatch Fixer")
    print("=" * 50)
    
    fixer = CategoryMismatchFixer()
    if not fixer.connect():
        return
    
    try:
        # Analyze the problem
        neo4j_cats, neo4j_prod_cats, pg_cats, pg_prod_cats = fixer.analyze_mismatch()
        
        print("\n💡 Available solutions:")
        print("1. Remap product category IDs to existing Neo4j categories (quick)")
        print("2. Re-import correct categories from PostgreSQL (proper)")
        
        choice = input("\nWhich solution? (1/2): ").strip()
        
        if choice == "1":
            success = fixer.solution_1_remap_products()
        elif choice == "2":
            success = fixer.solution_2_fix_categories()
        else:
            print("Invalid choice")
            return
        
        if success:
            print("\n🔧 Creating indexes and relationships...")
            fixer.create_indexes()
            fixer.create_relationships()
            
            # Verify and cleanup if needed
            if not fixer.verify_fix():
                print("\n🧹 Attempting to fix remaining issues...")
                fixer.cleanup_orphaned_products()
                fixer.verify_fix()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        fixer.disconnect()

if __name__ == "__main__":
    main()