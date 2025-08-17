"""
Improved Neo4j Migration Script for Product Associations
Migrates product association data from PostgreSQL to Neo4j with better error handling and validation
"""

import sys
import time
import json
import psycopg2
from neo4j import GraphDatabase
from decouple import config
import argparse
from datetime import datetime


class ImprovedNeo4jMigrator:
    def __init__(self, postgres_config, neo4j_config):
        self.postgres_config = postgres_config
        self.neo4j_config = neo4j_config
        self.pg_connection = None
        self.neo4j_driver = None
        self.migration_log = []
        
    def log_message(self, message, level='INFO'):
        """Log messages for debugging"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        self.migration_log.append(log_entry)
        if level == 'ERROR':
            print(f"❌ {message}")
        elif level == 'WARNING':
            print(f"⚠️  {message}")
        else:
            print(f"ℹ️  {message}")
        
    def connect_databases(self):
        """Connect to both PostgreSQL and Neo4j with improved error handling"""
        try:
            # Connect to PostgreSQL
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            self.log_message(f"Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
            
            # Test PostgreSQL connection
            cursor = self.pg_connection.cursor()
            cursor.execute("SELECT version()")
            pg_version = cursor.fetchone()[0]
            cursor.close()
            self.log_message(f"PostgreSQL version: {pg_version}")
            
            # Connect to Neo4j
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            
            # Test Neo4j connection with detailed info
            with self.neo4j_driver.session() as session:
                result = session.run("CALL dbms.components() YIELD name, versions, edition")
                neo4j_info = result.single()
                self.log_message(f"Connected to Neo4j: {self.neo4j_config['uri']}")
                self.log_message(f"Neo4j version: {neo4j_info['versions'][0]} ({neo4j_info['edition']} edition)")
                
            return True
            
        except psycopg2.Error as e:
            self.log_message(f"PostgreSQL connection failed: {e}", 'ERROR')
            return False
        except Exception as e:
            self.log_message(f"Neo4j connection failed: {e}", 'ERROR')
            return False
    
    def disconnect_databases(self):
        """Close database connections"""
        if self.pg_connection:
            self.pg_connection.close()
            self.log_message("Disconnected from PostgreSQL")
        if self.neo4j_driver:
            self.neo4j_driver.close()
            self.log_message("Disconnected from Neo4j")
    
    def clear_neo4j_database(self):
        """Completely clear Neo4j database"""
        print("🗑️  Clearing existing Neo4j data...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Get initial counts
                result = session.run("MATCH (n) RETURN count(n) as node_count")
                initial_nodes = result.single()['node_count']
                
                result = session.run("MATCH ()-[r]->() RETURN count(r) as rel_count")
                initial_rels = result.single()['rel_count']
                
                self.log_message(f"Found {initial_nodes} nodes and {initial_rels} relationships to delete")
                
                if initial_nodes > 0 or initial_rels > 0:
                    # Delete all relationships first
                    session.run("MATCH ()-[r]->() DELETE r")
                    self.log_message("Deleted all relationships")
                    
                    # Delete all nodes
                    session.run("MATCH (n) DELETE n")
                    self.log_message("Deleted all nodes")
                    
                    # Verify deletion
                    result = session.run("MATCH (n) RETURN count(n) as node_count")
                    remaining_nodes = result.single()['node_count']
                    
                    if remaining_nodes == 0:
                        print("✅ Neo4j database cleared successfully")
                        self.log_message("Neo4j database cleared successfully")
                    else:
                        self.log_message(f"Warning: {remaining_nodes} nodes still remain", 'WARNING')
                else:
                    print("✅ Neo4j database was already empty")
                    self.log_message("Neo4j database was already empty")
                
                return True
                
        except Exception as e:
            self.log_message(f"Error clearing Neo4j database: {e}", 'ERROR')
            return False
    
    def setup_neo4j_constraints_and_indexes(self):
        """Create Neo4j constraints and indexes"""
        print("🔧 Setting up Neo4j constraints and indexes...")
        
        # Define constraints and indexes
        schema_commands = [
            # Constraints for uniqueness
            ("CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE", "Product ID constraint"),
            ("CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE", "Category ID constraint"),
            
            # Indexes for performance
            ("CREATE INDEX product_name_index IF NOT EXISTS FOR (p:Product) ON (p.product_name)", "Product name index"),
            ("CREATE INDEX product_brand_index IF NOT EXISTS FOR (p:Product) ON (p.brand)", "Product brand index"),
            ("CREATE INDEX category_name_index IF NOT EXISTS FOR (c:Category) ON (c.category_name)", "Category name index"),
            ("CREATE INDEX association_frequency_index IF NOT EXISTS FOR ()-[r:BOUGHT_TOGETHER]-() ON (r.frequency_count)", "Association frequency index"),
        ]
        
        try:
            with self.neo4j_driver.session() as session:
                success_count = 0
                for command, description in schema_commands:
                    try:
                        session.run(command)
                        print(f"   ✅ {description}")
                        self.log_message(f"Created {description}")
                        success_count += 1
                    except Exception as e:
                        if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                            print(f"   ℹ️  {description} already exists")
                            self.log_message(f"{description} already exists")
                            success_count += 1
                        else:
                            self.log_message(f"Error creating {description}: {e}", 'WARNING')
                            print(f"   ⚠️  Error creating {description}: {e}")
                
                print(f"✅ Schema setup completed ({success_count}/{len(schema_commands)} items)")
                return success_count == len(schema_commands)
                
        except Exception as e:
            self.log_message(f"Error setting up Neo4j schema: {e}", 'ERROR')
            return False
    
    def test_single_category_import(self):
        """Test importing a single category to validate the process"""
        print("🧪 Testing single category import...")
        
        try:
            # Get one category from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT category_id, category_name, description, created_at, updated_at
                FROM categories
                ORDER BY category_id
                LIMIT 1
            """)
            category = cursor.fetchone()
            cursor.close()
            
            if not category:
                self.log_message("No categories found in PostgreSQL", 'ERROR')
                return False
            
            category_id, name, description, created_at, updated_at = category
            self.log_message(f"Testing with category: ID={category_id}, Name='{name}'")
            
            # Insert into Neo4j
            with self.neo4j_driver.session() as session:
                result = session.run("""
                    CREATE (c:Category {
                        category_id: $category_id,
                        category_name: $name,
                        description: $description,
                        created_at: $created_at,
                        updated_at: $updated_at
                    })
                    RETURN c.category_id as imported_id, c.category_name as imported_name
                """, {
                    'category_id': category_id,
                    'name': name,
                    'description': description,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                })
                
                imported = result.single()
                
                if imported and imported['imported_id'] == category_id:
                    print(f"   ✅ Successfully imported category: {imported['imported_name']} (ID: {imported['imported_id']})")
                    self.log_message(f"Test category import successful: {imported['imported_name']}")
                    return True
                else:
                    self.log_message("Test category import failed - data mismatch", 'ERROR')
                    return False
                    
        except Exception as e:
            self.log_message(f"Error testing category import: {e}", 'ERROR')
            return False
    
    def test_single_product_import(self):
        """Test importing a single product to validate the process"""
        print("🧪 Testing single product import...")
        
        try:
            # Get one product from PostgreSQL with its category
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT p.product_id, p.product_name, p.description, p.price, 
                       p.category_id, p.brand, p.stock_qty, p.is_active,
                       p.created_at, p.updated_at, c.category_name
                FROM products p
                JOIN categories c ON p.category_id = c.category_id
                WHERE p.is_active = true
                ORDER BY p.product_id
                LIMIT 1
            """)
            product = cursor.fetchone()
            cursor.close()
            
            if not product:
                self.log_message("No products found in PostgreSQL", 'ERROR')
                return False
            
            (product_id, name, description, price, category_id, brand, 
             stock_qty, is_active, created_at, updated_at, category_name) = product
            
            self.log_message(f"Testing with product: ID={product_id}, Name='{name}', Category={category_name}")
            
            # Insert into Neo4j
            with self.neo4j_driver.session() as session:
                result = session.run("""
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
                    RETURN p.product_id as imported_id, p.product_name as imported_name, p.category_id as imported_category_id
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
                
                imported = result.single()
                
                if (imported and 
                    imported['imported_id'] == product_id and 
                    imported['imported_category_id'] == category_id):
                    print(f"   ✅ Successfully imported product: {imported['imported_name']} (ID: {imported['imported_id']}, Category ID: {imported['imported_category_id']})")
                    self.log_message(f"Test product import successful: {imported['imported_name']}")
                    return True
                else:
                    self.log_message("Test product import failed - data mismatch", 'ERROR')
                    return False
                    
        except Exception as e:
            self.log_message(f"Error testing product import: {e}", 'ERROR')
            return False
    
    def migrate_all_categories(self):
        """Migrate all categories to Neo4j"""
        print("📦 Migrating all categories to Neo4j...")
        
        try:
            # Fetch all categories from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT category_id, category_name, description, created_at, updated_at
                FROM categories
                ORDER BY category_id
            """)
            categories = cursor.fetchall()
            cursor.close()
            
            if not categories:
                self.log_message("No categories found in PostgreSQL", 'WARNING')
                return True
            
            self.log_message(f"Found {len(categories)} categories to migrate")
            
            # Remove the test category first
            with self.neo4j_driver.session() as session:
                session.run("MATCH (c:Category) DELETE c")
                
                # Insert all categories
                success_count = 0
                for category in categories:
                    category_id, name, description, created_at, updated_at = category
                    
                    try:
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
                        success_count += 1
                        
                    except Exception as e:
                        self.log_message(f"Error importing category {name} (ID: {category_id}): {e}", 'ERROR')
                
                # Verify import
                result = session.run("MATCH (c:Category) RETURN count(c) as count")
                imported_count = result.single()['count']
                
                if imported_count == len(categories):
                    print(f"   ✅ Successfully migrated all {imported_count} categories")
                    self.log_message(f"Successfully migrated all {imported_count} categories")
                    return True
                else:
                    self.log_message(f"Category migration incomplete: {imported_count}/{len(categories)}", 'ERROR')
                    return False
                    
        except Exception as e:
            self.log_message(f"Error migrating categories: {e}", 'ERROR')
            return False
    
    def migrate_all_products(self):
        """Migrate all products to Neo4j"""
        print("📦 Migrating all products to Neo4j...")
        
        try:
            # Fetch all active products from PostgreSQL
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT p.product_id, p.product_name, p.description, p.price, 
                       p.category_id, p.brand, p.stock_qty, p.is_active,
                       p.created_at, p.updated_at, c.category_name
                FROM products p
                JOIN categories c ON p.category_id = c.category_id
                ORDER BY p.product_id
            """)
            products = cursor.fetchall()
            cursor.close()
            
            if not products:
                self.log_message("No products found in PostgreSQL", 'WARNING')
                return True
            
            self.log_message(f"Found {len(products)} products to migrate")
            
            # Process in batches for better performance
            batch_size = 1000
            total_batches = (len(products) + batch_size - 1) // batch_size
            
            with self.neo4j_driver.session() as session:
                # Remove test products first
                session.run("MATCH (p:Product) DELETE p")
                
                success_count = 0
                for i in range(0, len(products), batch_size):
                    batch = products[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    batch_success = 0
                    for product in batch:
                        (product_id, name, description, price, category_id, brand, 
                         stock_qty, is_active, created_at, updated_at, category_name) = product
                        
                        try:
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
                            batch_success += 1
                            
                        except Exception as e:
                            self.log_message(f"Error importing product {name} (ID: {product_id}): {e}", 'ERROR')
                    
                    success_count += batch_success
                    print(f"   📦 Batch {batch_num}/{total_batches} completed ({batch_success}/{len(batch)} products)")
                
                # Create relationships between products and categories
                print("   🔗 Creating product-category relationships...")
                relationship_result = session.run("""
                    MATCH (p:Product), (c:Category)
                    WHERE p.category_id = c.category_id
                    CREATE (p)-[:BELONGS_TO]->(c)
                    RETURN count(*) as relationships_created
                """)
                relationships_created = relationship_result.single()['relationships_created']
                
                # Verify import
                result = session.run("MATCH (p:Product) RETURN count(p) as count")
                imported_count = result.single()['count']
                
                if imported_count == len(products):
                    print(f"   ✅ Successfully migrated all {imported_count} products with {relationships_created} category relationships")
                    self.log_message(f"Successfully migrated all {imported_count} products")
                    return True
                else:
                    self.log_message(f"Product migration incomplete: {imported_count}/{len(products)}", 'ERROR')
                    return False
                    
        except Exception as e:
            self.log_message(f"Error migrating products: {e}", 'ERROR')
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
                ORDER BY pa.frequency_count DESC
            """)
            associations = cursor.fetchall()
            cursor.close()
            
            if not associations:
                self.log_message("No product associations found in PostgreSQL", 'WARNING')
                return True
            
            self.log_message(f"Found {len(associations)} product associations to migrate")
            
            # Process in batches
            batch_size = 500
            total_batches = (len(associations) + batch_size - 1) // batch_size
            
            with self.neo4j_driver.session() as session:
                success_count = 0
                for i in range(0, len(associations), batch_size):
                    batch = associations[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    batch_success = 0
                    for association in batch:
                        (assoc_id, product_a_id, product_b_id, frequency_count, 
                         last_calculated, product_a_name, product_a_brand,
                         product_b_name, product_b_brand) = association
                        
                        try:
                            result = session.run("""
                                MATCH (p1:Product {product_id: $product_a_id})
                                MATCH (p2:Product {product_id: $product_b_id})
                                CREATE (p1)-[r:BOUGHT_TOGETHER {
                                    association_id: $assoc_id,
                                    frequency_count: $frequency_count,
                                    last_calculated: $last_calculated,
                                    strength: $frequency_count
                                }]->(p2)
                                RETURN r.association_id as created_id
                            """, {
                                'product_a_id': product_a_id,
                                'product_b_id': product_b_id,
                                'assoc_id': assoc_id,
                                'frequency_count': frequency_count,
                                'last_calculated': last_calculated.isoformat() if last_calculated else None
                            })
                            
                            if result.single():
                                batch_success += 1
                            
                        except Exception as e:
                            self.log_message(f"Error importing association {assoc_id} ({product_a_name} ↔ {product_b_name}): {e}", 'ERROR')
                    
                    success_count += batch_success
                    print(f"   🔗 Batch {batch_num}/{total_batches} completed ({batch_success}/{len(batch)} associations)")
                
                # Verify import
                result = session.run("MATCH ()-[r:BOUGHT_TOGETHER]->() RETURN count(r) as count")
                imported_count = result.single()['count']
                
                if imported_count == len(associations):
                    print(f"   ✅ Successfully migrated all {imported_count} product associations")
                    self.log_message(f"Successfully migrated all {imported_count} product associations")
                    return True
                else:
                    self.log_message(f"Association migration incomplete: {imported_count}/{len(associations)}", 'ERROR')
                    return False
                    
        except Exception as e:
            self.log_message(f"Error migrating product associations: {e}", 'ERROR')
            return False
    
    def verify_complete_migration(self):
        """Verify the complete migration was successful"""
        print("🔍 Verifying complete migration...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Count nodes and relationships
                category_count = session.run("MATCH (c:Category) RETURN count(c) as count").single()['count']
                product_count = session.run("MATCH (p:Product) RETURN count(p) as count").single()['count']
                belongs_to_count = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count").single()['count']
                bought_together_count = session.run("MATCH ()-[r:BOUGHT_TOGETHER]->() RETURN count(r) as count").single()['count']
                
                print(f"   📊 Neo4j Migration Results:")
                print(f"      Categories: {category_count:,}")
                print(f"      Products: {product_count:,}")
                print(f"      Product-Category relationships: {belongs_to_count:,}")
                print(f"      Product-Association relationships: {bought_together_count:,}")
                
                # Verify data integrity
                integrity_checks = []
                
                # Check if all products have category relationships
                orphaned_products = session.run("""
                    MATCH (p:Product)
                    WHERE NOT (p)-[:BELONGS_TO]->(:Category)
                    RETURN count(p) as count
                """).single()['count']
                
                if orphaned_products == 0:
                    integrity_checks.append("✅ All products have category relationships")
                else:
                    integrity_checks.append(f"❌ {orphaned_products} products without category relationships")
                
                # Check if association references exist (simplified check)
                missing_association_count = session.run("""
                    MATCH ()-[r:BOUGHT_TOGETHER]->()
                    RETURN count(r) as total_associations
                """).single()['total_associations']
                
                # Verify that associations point to actual products
                valid_associations = session.run("""
                    MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
                    RETURN count(r) as valid_count
                """).single()['valid_count']
                
                if valid_associations == missing_association_count:
                    integrity_checks.append("✅ All associations reference existing products")
                else:
                    missing_refs = missing_association_count - valid_associations
                    integrity_checks.append(f"❌ {missing_refs} associations with missing product references")
                
                # Show sample data
                sample_result = session.run("""
                    MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
                    RETURN p1.product_name, p2.product_name, r.frequency_count
                    ORDER BY r.frequency_count DESC
                    LIMIT 3
                """)
                
                print(f"   🔍 Data integrity checks:")
                for check in integrity_checks:
                    print(f"      {check}")
                
                print(f"   🔍 Sample associations:")
                sample_count = 0
                for record in sample_result:
                    print(f"      {record['p1.product_name']} ↔ {record['p2.product_name']} (freq: {record['r.frequency_count']})")
                    sample_count += 1
                
                if sample_count == 0:
                    print("      No associations found")
                
                # Overall verification
                success = (category_count > 0 and 
                          product_count > 0 and 
                          belongs_to_count == product_count and
                          orphaned_products == 0 and
                          valid_associations == missing_association_count)
                
                if success:
                    print("   ✅ Migration verification successful")
                    self.log_message("Complete migration verification successful")
                    return True
                else:
                    print("   ❌ Migration verification failed")
                    self.log_message("Migration verification failed", 'ERROR')
                    return False
                
        except Exception as e:
            self.log_message(f"Error verifying migration: {e}", 'ERROR')
            return False
    
    def run_performance_test(self):
        """Run performance tests on the migrated data"""
        print("⚡ Running performance test...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Test 1: Simple association query
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
                test1_time = (time.time() - start_time) * 1000
                
                # Test 2: Category-based association query
                start_time = time.time()
                result = session.run("""
                    MATCH (p1:Product)-[:BELONGS_TO]->(c1:Category)
                    MATCH (p1)-[r:BOUGHT_TOGETHER]->(p2:Product)-[:BELONGS_TO]->(c2:Category)
                    WHERE c1.category_name = 'Electronics'
                    RETURN p1.product_name, p2.product_name, c2.category_name, r.frequency_count
                    ORDER BY r.frequency_count DESC
                    LIMIT 50
                """)
                category_records = list(result)
                test2_time = (time.time() - start_time) * 1000
                
                print(f"   ⚡ Neo4j Query Performance:")
                print(f"      Test 1 - Association query: {test1_time:.2f}ms ({len(records)} results)")
                print(f"      Test 2 - Category association query: {test2_time:.2f}ms ({len(category_records)} results)")
                
                if records:
                    print(f"      Top association: {records[0]['product_a']} ↔ {records[0]['product_b']} (freq: {records[0]['r.frequency_count']})")
                
                self.log_message(f"Performance test completed: {test1_time:.2f}ms, {test2_time:.2f}ms")
                return True
                
        except Exception as e:
            self.log_message(f"Error running performance test: {e}", 'ERROR')
            return False
    
    def get_postgresql_summary(self):
        """Get summary of PostgreSQL data for comparison"""
        try:
            cursor = self.pg_connection.cursor()
            
            # Get counts from PostgreSQL
            cursor.execute("SELECT COUNT(*) FROM categories")
            pg_categories = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM products")
            pg_products = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM product_associations")
            pg_associations = cursor.fetchone()[0]
            
            cursor.close()
            
            return {
                'categories': pg_categories,
                'products': pg_products,
                'associations': pg_associations
            }
            
        except Exception as e:
            self.log_message(f"Error getting PostgreSQL summary: {e}", 'ERROR')
            return None
    
    def save_migration_log(self):
        """Save migration log to a file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"neo4j_migration_log_{timestamp}.txt"
            
            with open(log_filename, 'w') as f:
                f.write("Neo4j Migration Log\n")
                f.write("=" * 50 + "\n")
                f.write(f"Migration completed at: {datetime.now().isoformat()}\n\n")
                
                for log_entry in self.migration_log:
                    f.write(log_entry + "\n")
            
            print(f"📝 Migration log saved to: {log_filename}")
            return log_filename
            
        except Exception as e:
            print(f"⚠️  Could not save migration log: {e}")
            return None


def load_environment():
    """Load database configurations from environment"""
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
    """Main function with improved step-by-step migration process"""
    parser = argparse.ArgumentParser(
        description='Improved Neo4j Migration Tool for Product Associations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python improved_neo4j_migration.py                    # Full migration
  python improved_neo4j_migration.py --test-only        # Test connections only
  python improved_neo4j_migration.py --verify-only      # Verify existing migration
  python improved_neo4j_migration.py --skip-tests       # Skip single item tests
  python improved_neo4j_migration.py --no-clear         # Don't clear existing data
        '''
    )
    
    parser.add_argument('--test-only', action='store_true', 
                       help='Only test database connections')
    parser.add_argument('--verify-only', action='store_true', 
                       help='Only verify existing migration')
    parser.add_argument('--skip-tests', action='store_true', 
                       help='Skip single item import tests')
    parser.add_argument('--no-clear', action='store_true', 
                       help='Do not clear existing Neo4j data')
    parser.add_argument('--performance-only', action='store_true', 
                       help='Only run performance tests')
    
    args = parser.parse_args()
    
    print("🚀 Improved Neo4j Migration Tool for Product Associations")
    print("=" * 65)
    
    # Load configurations
    try:
        postgres_config, neo4j_config = load_environment()
        print(f"📡 PostgreSQL: {postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}")
        print(f"📡 Neo4j: {neo4j_config['uri']}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Initialize migrator
    migrator = ImprovedNeo4jMigrator(postgres_config, neo4j_config)
    
    # Connect to databases
    if not migrator.connect_databases():
        print("❌ Database connection failed")
        sys.exit(1)
    
    try:
        # Handle specific operation modes
        if args.test_only:
            print("✅ Database connections successful")
            return
        
        if args.verify_only:
            if migrator.verify_complete_migration():
                print("\n🎉 Migration verification successful!")
            else:
                print("\n❌ Migration verification failed")
                sys.exit(1)
            return
        
        if args.performance_only:
            if migrator.run_performance_test():
                print("\n🎉 Performance test completed!")
            else:
                print("\n❌ Performance test failed")
                sys.exit(1)
            return
        
        # Full migration process
        print("\n🔄 Starting step-by-step migration process...")
        
        migration_steps = []
        
        # Step 1: Clear existing data (optional)
        if not args.no_clear:
            step_success = migrator.clear_neo4j_database()
            migration_steps.append(("Clear Neo4j Database", step_success))
            if not step_success:
                print("❌ Failed to clear Neo4j database")
                sys.exit(1)
        
        # Step 2: Setup schema
        step_success = migrator.setup_neo4j_constraints_and_indexes()
        migration_steps.append(("Setup Schema", step_success))
        if not step_success:
            print("❌ Failed to setup Neo4j schema")
            sys.exit(1)
        
        # Step 3: Test single imports (optional)
        if not args.skip_tests:
            print("\n🧪 Testing single item imports...")
            
            step_success = migrator.test_single_category_import()
            migration_steps.append(("Test Category Import", step_success))
            if not step_success:
                print("❌ Single category import test failed")
                sys.exit(1)
            
            step_success = migrator.test_single_product_import()
            migration_steps.append(("Test Product Import", step_success))
            if not step_success:
                print("❌ Single product import test failed")
                sys.exit(1)
            
            print("✅ Single import tests passed, proceeding with full migration")
        
        # Step 4: Migrate all categories
        print("\n📦 Full data migration...")
        step_success = migrator.migrate_all_categories()
        migration_steps.append(("Migrate Categories", step_success))
        if not step_success:
            print("❌ Category migration failed")
            sys.exit(1)
        
        # Step 5: Migrate all products
        step_success = migrator.migrate_all_products()
        migration_steps.append(("Migrate Products", step_success))
        if not step_success:
            print("❌ Product migration failed")
            sys.exit(1)
        
        # Step 6: Migrate product associations
        step_success = migrator.migrate_product_associations()
        migration_steps.append(("Migrate Associations", step_success))
        if not step_success:
            print("❌ Association migration failed")
            sys.exit(1)
        
        # Step 7: Verify migration
        step_success = migrator.verify_complete_migration()
        migration_steps.append(("Verify Migration", step_success))
        if not step_success:
            print("❌ Migration verification failed")
            sys.exit(1)
        
        # Step 8: Performance test
        step_success = migrator.run_performance_test()
        migration_steps.append(("Performance Test", step_success))
        
        # Summary
        print("\n" + "=" * 65)
        print("📋 Migration Summary:")
        print("=" * 65)
        
        success_count = 0
        for step_name, success in migration_steps:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{step_name:25}: {status}")
            if success:
                success_count += 1
        
        print("=" * 65)
        print(f"Overall Success Rate: {success_count}/{len(migration_steps)} ({100*success_count/len(migration_steps):.1f}%)")
        
        if success_count == len(migration_steps):
            print("\n🎉 Migration completed successfully!")
            print("💡 Neo4j is now ready for product association queries")
            print("💡 Key features available:")
            print("   • Product-Category relationships via BELONGS_TO")
            print("   • Product associations via BOUGHT_TOGETHER")
            print("   • Full text search on product/category names")
            print("   • Frequency-based association strength")
            print("\n💡 Example queries to try:")
            print("   • Find products bought with a specific item")
            print("   • Discover cross-category purchase patterns") 
            print("   • Analyze brand affinity in associations")
        else:
            print(f"\n⚠️  Migration completed with {len(migration_steps) - success_count} issues")
            if success_count >= len(migration_steps) - 1:
                print("💡 Core migration successful, only performance test failed")
            else:
                print("❌ Significant migration issues detected")
                sys.exit(1)
        
        # Save migration log
        log_file = migrator.save_migration_log()
        if log_file:
            print(f"📝 Detailed log available in: {log_file}")
                
    except KeyboardInterrupt:
        print("\n⏹️  Migration interrupted by user")
        migrator.log_message("Migration interrupted by user", 'ERROR')
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        migrator.log_message(f"Unexpected error: {e}", 'ERROR')
        sys.exit(1)
    finally:
        migrator.disconnect_databases()


if __name__ == "__main__":
    main()