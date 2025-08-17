"""
Test Data Generator Script
Generates realistic test data for the database with proper foreign key relationships
"""

import os
import sys
import json
import random
import psutil
import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
from datetime import datetime, timedelta
from decimal import Decimal
import argparse
from decouple import config

# Initialize Faker
fake = Faker()

class TestDataGenerator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.existing_data = {}
        
        # Execution tracking
        self.execution_start_time = datetime.now()
        self.execution_log = []
        self.error_count = 0
        self.warning_count = 0
        self.total_operations = 0
        self.successful_operations = 0
        self.failed_operations = 0
        self.records_created = {}  # Track records created per table
        
        # Reset Faker's unique provider to start fresh
        fake.unique.clear()
        
        # Data pools for realistic generation
        self.brands = [
            'Apple', 'Samsung', 'Google', 'Microsoft', 'Sony', 'LG', 'HP', 'Dell', 
            'Lenovo', 'Asus', 'Nike', 'Adidas', 'Zara', 'H&M', 'Uniqlo', 'Generic'
        ]
        
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        self.order_statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']
        
        # Product name templates by category
        self.product_templates = {
            'smartphones': [
                'Pro Max', 'Ultra', 'Plus', 'Mini', 'Standard', 'Lite', 'Edge', 
                'Note', 'Galaxy', 'Pixel', 'iPhone'
            ],
            'laptops': [
                'ThinkPad', 'MacBook', 'XPS', 'Surface', 'Pavilion', 'Inspiron',
                'ZenBook', 'VivoBook', 'Gaming', 'Business'
            ],
            'clothing': [
                'T-Shirt', 'Shirt', 'Jeans', 'Dress', 'Sweater', 'Jacket', 
                'Hoodie', 'Pants', 'Skirt', 'Blouse'
            ],
            'books': [
                'The Art of', 'Introduction to', 'Advanced', 'Complete Guide to',
                'Mastering', 'Understanding', 'Principles of'
            ]
        }

    def log_message(self, message, level='INFO'):
        """Log messages for execution tracking"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        self.execution_log.append(log_entry)
        
        if level == 'ERROR':
            self.error_count += 1
        elif level == 'WARNING':
            self.warning_count += 1

    def connect(self):
        """Create database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.log_message(f"Connected to database: {self.db_config['host']}:{self.db_config['port']}")
            print(f"✅ Connected to database: {self.db_config['host']}:{self.db_config['port']}")
            return True
        except psycopg2.Error as e:
            error_msg = f"Database connection failed: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

    def load_existing_data(self):
        """Load existing foreign key data to ensure referential integrity"""
        try:
            cursor = self.connection.cursor()
            
            # Load categories
            cursor.execute("SELECT category_id, category_name FROM Categories")
            self.existing_data['categories'] = cursor.fetchall()
            
            # Load customers
            cursor.execute("SELECT customer_id FROM Customers")
            self.existing_data['customers'] = [row[0] for row in cursor.fetchall()]
            
            # Load products
            cursor.execute("SELECT product_id FROM Products")
            self.existing_data['products'] = [row[0] for row in cursor.fetchall()]
            
            # Load orders
            cursor.execute("SELECT order_id FROM Orders")
            self.existing_data['orders'] = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            
            log_msg = f"Loaded existing data: {len(self.existing_data['categories'])} categories, " \
                     f"{len(self.existing_data['customers'])} customers, " \
                     f"{len(self.existing_data['products'])} products, " \
                     f"{len(self.existing_data['orders'])} orders"
            
            self.log_message(log_msg)
            print(f"📊 {log_msg}")
            
        except psycopg2.Error as e:
            error_msg = f"Error loading existing data: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            return False
        
        return True

    def reset_faker_unique(self):
        """Reset Faker's unique provider when it runs out of values"""
        try:
            fake.unique.clear()
            self.log_message("Reset Faker unique provider")
            print("🔄 Reset Faker unique provider")
        except Exception as e:
            warning_msg = f"Could not reset Faker unique provider: {e}"
            self.log_message(warning_msg, 'WARNING')
            print(f"⚠️  {warning_msg}")

    def generate_customers(self, count):
        """Generate customer data with guaranteed unique emails"""
        customers = []
        used_emails = set()
        
        # Get existing emails from database to avoid duplicates
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT email FROM customers")
            existing_emails = cursor.fetchall()
            used_emails.update([email[0] for email in existing_emails])
            cursor.close()
            if len(used_emails) > 0:
                self.log_message(f"Found {len(used_emails):,} existing emails in database")
                print(f"   Found {len(used_emails):,} existing emails in database")
        except Exception as e:
            warning_msg = f"Could not check existing emails: {e}"
            self.log_message(warning_msg, 'WARNING')
            print(f"   {warning_msg}")
        
        for i in range(count):
            # Generate unique email with fallback strategies
            attempts = 0
            email = None
            
            while attempts < 10:  # Prevent infinite loop
                try:
                    if attempts == 0:
                        # First attempt: use Faker's unique email
                        email = fake.unique.email()
                    elif attempts < 5:
                        # Fallback 1: Add random numbers to base email
                        base_email = fake.email()
                        username, domain = base_email.split('@')
                        email = f"{username}{random.randint(1000, 999999)}@{domain}"
                    else:
                        # Fallback 2: Create completely custom email
                        username = f"{fake.user_name()}{random.randint(10000, 999999)}"
                        domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'example.com'])
                        email = f"{username}@{domain}"
                    
                    # Check if email is unique
                    if email and email not in used_emails:
                        used_emails.add(email)
                        break
                    attempts += 1
                    
                except Exception:
                    # If fake.unique.email() fails due to exhaustion, use fallback
                    attempts = 5
                    continue
            
            if not email or email in used_emails:
                # If all attempts failed, create a guaranteed unique email
                email = f"user_{i}_{random.randint(100000, 999999)}@testdata.com"
                used_emails.add(email)
            
            customer = (
                fake.first_name(),
                fake.last_name(),
                email,
                fake.phone_number()[:20],  # Limit phone length
                fake.date_time_between(start_date='-2y', end_date='now'),
                fake.address().replace('\n', ', ')[:500]  # Limit address length
            )
            customers.append(customer)
            
            # Progress indicator for large datasets
            if count > 10000 and (i + 1) % 10000 == 0:
                progress_msg = f"Generated {i + 1:,} customers..."
                self.log_message(progress_msg)
                print(f"   {progress_msg}")
        
        return customers

    def generate_categories(self, count):
        """Generate category data with duplicate checking"""
        categories = []
        
        # Get existing category names to avoid duplicates
        existing_names = set()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT category_name FROM categories")
            existing_names = {row[0] for row in cursor.fetchall()}
            cursor.close()
            if existing_names:
                self.log_message(f"Found {len(existing_names)} existing categories")
                print(f"   Found {len(existing_names)} existing categories")
        except Exception as e:
            warning_msg = f"Could not check existing categories: {e}"
            self.log_message(warning_msg, 'WARNING')
            print(f"   {warning_msg}")
        
        base_categories = [
            ('Electronics', 'Electronic devices and accessories'),
            ('Clothing', 'Apparel and fashion items'),
            ('Books', 'Books and educational materials'),
            ('Home & Garden', 'Home improvement and garden supplies'),
            ('Sports', 'Sports and fitness equipment'),
            ('Beauty', 'Beauty and personal care products'),
            ('Toys', 'Toys and games'),
            ('Automotive', 'Car parts and accessories'),
            ('Health & Wellness', 'Health and wellness products'),
            ('Food & Beverages', 'Food and drink items'),
            ('Music & Movies', 'Entertainment media'),
            ('Office Supplies', 'Business and office equipment'),
            ('Pet Supplies', 'Pet care and accessories'),
            ('Travel & Luggage', 'Travel accessories and luggage'),
            ('Art & Crafts', 'Art supplies and craft materials')
        ]
        
        # Add base categories that don't already exist
        for name, desc in base_categories:
            if name not in existing_names and len(categories) < count:
                categories.append((name, desc))
                existing_names.add(name)  # Track what we're adding
        
        # Generate additional unique categories if needed
        remaining = count - len(categories)
        attempts = 0
        max_attempts = remaining * 10  # Prevent infinite loop
        
        while remaining > 0 and attempts < max_attempts:
            # Generate a unique category name
            category_name = f"{fake.word().title()} {fake.word().title()}"
            
            # Make it more unique if it already exists
            if category_name in existing_names:
                category_name = f"{fake.word().title()} {fake.word().title()} {fake.word().title()}"
            
            # If still not unique, add a number
            if category_name in existing_names:
                category_name = f"{fake.word().title()} {fake.word().title()} {random.randint(1, 9999)}"
            
            # Check if this name is unique
            if category_name not in existing_names:
                category = (
                    category_name,
                    fake.text(max_nb_chars=200)
                )
                categories.append(category)
                existing_names.add(category_name)
                remaining -= 1
            
            attempts += 1
        
        if remaining > 0:
            warning_msg = f"Could only generate {len(categories)} unique categories out of {count} requested"
            self.log_message(warning_msg, 'WARNING')
            print(f"⚠️  {warning_msg}")
        
        return categories

    def generate_products(self, count):
        """Generate product data with valid category references"""
        if not self.existing_data['categories']:
            raise ValueError("No categories found. Please create categories first.")
        
        products = []
        for _ in range(count):
            category_id, category_name = random.choice(self.existing_data['categories'])
            
            # Generate product name based on category
            if 'phone' in category_name.lower() or 'smartphone' in category_name.lower():
                product_name = f"{random.choice(self.brands)} {random.choice(self.product_templates['smartphones'])} {random.randint(10, 20)}"
            elif 'laptop' in category_name.lower() or 'computer' in category_name.lower():
                product_name = f"{random.choice(self.brands)} {random.choice(self.product_templates['laptops'])} {random.randint(13, 17)}\""
            elif 'clothing' in category_name.lower() or 'apparel' in category_name.lower():
                product_name = f"{random.choice(['Men\'s', 'Women\'s', 'Unisex'])} {random.choice(self.product_templates['clothing'])}"
            elif 'book' in category_name.lower():
                product_name = f"{random.choice(self.product_templates['books'])} {fake.word().title()}"
            else:
                product_name = f"{fake.word().title()} {fake.word().title()}"
            
            # Generate realistic price based on category
            if 'phone' in category_name.lower():
                price = round(random.uniform(200, 1500), 2)
            elif 'laptop' in category_name.lower():
                price = round(random.uniform(500, 3000), 2)
            elif 'book' in category_name.lower():
                price = round(random.uniform(10, 80), 2)
            else:
                price = round(random.uniform(5, 500), 2)
            
            product = (
                product_name,
                fake.text(max_nb_chars=300),
                price,
                category_id,
                random.choice(self.brands),
                random.randint(0, 1000),
                random.choice([True, True, True, False])  # 75% chance active
            )
            products.append(product)
        
        return products

    def generate_orders(self, count):
        """Generate order data with valid customer references"""
        if not self.existing_data['customers']:
            raise ValueError("No customers found. Please create customers first.")
        
        orders = []
        for _ in range(count):
            customer_id = random.choice(self.existing_data['customers'])
            order_date = fake.date_time_between(start_date='-1y', end_date='now')
            
            # Generate realistic total amount (will be recalculated from order items)
            total_amount = round(random.uniform(20, 2000), 2)
            
            order = (
                customer_id,
                order_date,
                total_amount,
                random.choice(self.order_statuses),
                random.choice(self.payment_methods)
            )
            orders.append(order)
        
        return orders

    def generate_order_items(self, count):
        """Generate order item data with valid order and product references"""
        if not self.existing_data['orders']:
            raise ValueError("No orders found. Please create orders first.")
        if not self.existing_data['products']:
            raise ValueError("No products found. Please create products first.")
        
        order_items = []
        for _ in range(count):
            order_id = random.choice(self.existing_data['orders'])
            product_id = random.choice(self.existing_data['products'])
            quantity = random.randint(1, 5)
            
            # Get realistic unit price (simulating product price lookup)
            unit_price = round(random.uniform(10, 500), 2)
            
            order_item = (
                order_id,
                product_id,
                quantity,
                unit_price
            )
            order_items.append(order_item)
        
        return order_items

    def generate_product_associations(self, count):
        """Generate product association data based on actual order patterns"""
        if len(self.existing_data['products']) < 2:
            raise ValueError("Need at least 2 products to create associations.")
        
        # First, let's calculate actual associations from existing order data
        actual_associations = self._calculate_actual_associations()
        
        # If we have actual associations, use them; otherwise generate realistic ones
        if actual_associations:
            self.log_message(f"Using {len(actual_associations)} actual product associations from order data")
            print(f"📊 Using {len(actual_associations)} actual product associations from order data")
            # Limit to requested count
            return list(actual_associations.items())[:count]
        else:
            self.log_message("No order data found, generating realistic product associations")
            print("📊 No order data found, generating realistic product associations")
            return self._generate_realistic_associations(count)
    
    def _calculate_actual_associations(self):
        """Calculate actual product associations from existing order data"""
        try:
            cursor = self.connection.cursor()
            
            # Query to find products bought together in the same order
            query = """
                SELECT 
                    oi1.product_id as product_a_id,
                    oi2.product_id as product_b_id,
                    COUNT(*) as frequency_count,
                    MAX(o.order_date) as last_calculated
                FROM order_items oi1
                JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                JOIN orders o ON oi1.order_id = o.order_id
                WHERE oi1.product_id < oi2.product_id  -- Avoid duplicates and self-references
                GROUP BY oi1.product_id, oi2.product_id
                HAVING COUNT(*) >= 2  -- Only include pairs bought together at least twice
                ORDER BY frequency_count DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            associations = {}
            for product_a, product_b, freq_count, last_calc in results:
                associations[(product_a, product_b, freq_count, last_calc)] = True
            
            return associations
            
        except psycopg2.Error as e:
            warning_msg = f"Could not calculate actual associations: {e}"
            self.log_message(warning_msg, 'WARNING')
            print(f"⚠️  {warning_msg}")
            return {}
    
    def update_product_associations_from_orders(self):
        """Update product associations based on actual order patterns"""
        try:
            cursor = self.connection.cursor()
            
            # First, clear existing associations that might be outdated
            self.log_message("Updating product associations based on actual order patterns")
            print("🔄 Updating product associations based on actual order patterns...")
            
            # Calculate current associations from order data
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
            self.connection.commit()
            cursor.close()
            
            success_msg = f"Updated {rows_affected} product associations based on order data"
            self.log_message(success_msg)
            print(f"✅ {success_msg}")
            return True
            
        except psycopg2.Error as e:
            error_msg = f"Error updating product associations: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            self.connection.rollback()
            return False
    
    def _generate_realistic_associations(self, count):
        """Generate realistic product associations with meaningful frequency counts"""
        associations = []
        used_pairs = set()
        
        # Get products grouped by category for more realistic associations
        category_products = self._group_products_by_category()
        
        for _ in range(count):
            attempts = 0
            while attempts < 100:  # Prevent infinite loop
                
                # 70% chance for products from same/related categories, 30% random
                if random.random() < 0.7 and category_products:
                    product_a, product_b = self._get_related_products(category_products)
                else:
                    product_a = random.choice(self.existing_data['products'])
                    product_b = random.choice(self.existing_data['products'])
                
                if product_a != product_b:
                    pair = tuple(sorted([product_a, product_b]))
                    if pair not in used_pairs:
                        used_pairs.add(pair)
                        
                        # Generate realistic frequency based on product relationship
                        frequency = self._calculate_realistic_frequency(product_a, product_b, category_products)
                        
                        association = (
                            product_a,
                            product_b,
                            frequency,
                            fake.date_time_between(start_date='-6m', end_date='now')
                        )
                        associations.append(association)
                        break
                        
                attempts += 1
        
        return associations
    
    def _group_products_by_category(self):
        """Group products by their categories for smarter associations"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT p.product_id, p.product_name, c.category_name, c.category_id
                FROM products p
                JOIN categories c ON p.category_id = c.category_id
                WHERE p.is_active = true
            """)
            results = cursor.fetchall()
            cursor.close()
            
            category_groups = {}
            for product_id, product_name, category_name, category_id in results:
                if category_name not in category_groups:
                    category_groups[category_name] = []
                category_groups[category_name].append({
                    'id': product_id, 
                    'name': product_name, 
                    'category_id': category_id
                })
            
            return category_groups
            
        except psycopg2.Error:
            return {}
    
    def _get_related_products(self, category_products):
        """Get two products that are likely to be bought together"""
        
        # Define category relationships (products often bought together)
        related_categories = {
            'smartphones': ['laptops', 'electronics'],
            'laptops': ['smartphones', 'electronics'],
            'electronics': ['smartphones', 'laptops'],
            'clothing': ['clothing'],  # Different clothing items
            'books': ['books'],
        }
        
        # Pick a random category
        category_name = random.choice(list(category_products.keys()))
        products_in_category = category_products[category_name]
        
        if len(products_in_category) >= 2:
            # Same category products
            product_a = random.choice(products_in_category)['id']
            product_b = random.choice(products_in_category)['id']
            return product_a, product_b
        else:
            # Find related category
            for cat_key, related_cats in related_categories.items():
                if cat_key.lower() in category_name.lower():
                    for related_cat in related_cats:
                        for cat_name, products in category_products.items():
                            if related_cat.lower() in cat_name.lower() and len(products) > 0:
                                product_a = random.choice(products_in_category)['id']
                                product_b = random.choice(products)['id']
                                return product_a, product_b
        
        # Fallback to random products
        all_products = []
        for products in category_products.values():
            all_products.extend([p['id'] for p in products])
        
        if len(all_products) >= 2:
            return random.sample(all_products, 2)
        else:
            return random.choice(self.existing_data['products']), random.choice(self.existing_data['products'])
    
    def _calculate_realistic_frequency(self, product_a, product_b, category_products):
        """Calculate realistic frequency count based on product types"""
        
        # Get category information for both products
        product_a_category = self._get_product_category(product_a, category_products)
        product_b_category = self._get_product_category(product_b, category_products)
        
        # Base frequency ranges by category combination
        if product_a_category == product_b_category:
            if 'electronics' in product_a_category.lower() or 'smartphone' in product_a_category.lower():
                return random.randint(15, 50)  # Electronics often bought together
            elif 'clothing' in product_a_category.lower():
                return random.randint(20, 60)  # Clothing items often bought together
            else:
                return random.randint(5, 25)   # Other same-category items
        else:
            # Cross-category associations (less frequent)
            if ('smartphone' in product_a_category.lower() and 'laptop' in product_b_category.lower()) or \
               ('laptop' in product_a_category.lower() and 'smartphone' in product_b_category.lower()):
                return random.randint(10, 30)  # Tech ecosystem purchases
            else:
                return random.randint(2, 15)   # Random cross-category
    
    def _get_product_category(self, product_id, category_products):
        """Get category name for a product"""
        for category_name, products in category_products.items():
            for product in products:
                if product['id'] == product_id:
                    return category_name
        return 'unknown'

    def insert_data(self, table_name, data, columns):
        """Insert data into specified table with better error handling for large datasets"""
        if not data:
            warning_msg = f"No data to insert for {table_name}"
            self.log_message(warning_msg, 'WARNING')
            print(f"⚠️  {warning_msg}")
            return True
        
        self.total_operations += 1
        
        try:
            cursor = self.connection.cursor()
            
            # Create parameterized query
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # For large datasets, use smaller batch sizes and show progress
            batch_size = 1000 if len(data) > 10000 else len(data)
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            inserted_count = 0
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                try:
                    # Use execute_batch for better performance
                    execute_batch(cursor, query, batch, page_size=batch_size)
                    self.connection.commit()
                    inserted_count += len(batch)
                    
                    # Show progress for large datasets
                    if total_batches > 1:
                        batch_num = (i // batch_size) + 1
                        progress_msg = f"Batch {batch_num}/{total_batches} completed ({inserted_count:,}/{len(data):,} rows)"
                        self.log_message(progress_msg)
                        print(f"   {progress_msg}")
                        
                except Exception as batch_error:
                    # Handle individual batch errors (like unique constraint violations)
                    warning_msg = f"Batch {i//batch_size + 1} failed, trying individual inserts..."
                    self.log_message(warning_msg, 'WARNING')
                    print(f"⚠️  {warning_msg}")
                    self.connection.rollback()
                    
                    # Try inserting rows individually to identify problematic rows
                    individual_inserted = 0
                    for row in batch:
                        try:
                            cursor.execute(query, row)
                            self.connection.commit()
                            individual_inserted += 1
                        except Exception as row_error:
                            self.connection.rollback()
                            if "unique constraint" in str(row_error).lower():
                                skip_msg = f"Skipped duplicate row: {row[1] if len(row) > 1 else row[0]}"
                                self.log_message(skip_msg, 'WARNING')
                                print(f"   {skip_msg}")
                            else:
                                error_msg = f"Error with row: {str(row_error)}"
                                self.log_message(error_msg, 'ERROR')
                                print(f"   {error_msg}")
                    
                    inserted_count += individual_inserted
                    if individual_inserted < len(batch):
                        batch_msg = f"Successfully inserted {individual_inserted}/{len(batch)} rows from failed batch"
                        self.log_message(batch_msg)
                        print(f"   {batch_msg}")
            
            cursor.close()
            
            # Track records created
            self.records_created[table_name] = inserted_count
            
            success_msg = f"Inserted {inserted_count:,} rows into {table_name}"
            self.log_message(success_msg)
            print(f"✅ {success_msg}")
            
            self.successful_operations += 1
            return True
            
        except psycopg2.Error as e:
            error_msg = f"Error inserting data into {table_name}: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            self.connection.rollback()
            self.failed_operations += 1
            return False

    def generate_table_data(self, table_name, count):
        """Generate data for a specific table"""
        # Reset Faker unique provider for large datasets
        if count > 100000:
            self.reset_faker_unique()
            msg = f"Generating {count:,} rows for {table_name} (this may take a while)..."
            self.log_message(msg)
            print(f"🔄 {msg}")
        
        table_generators = {
            'customers': {
                'generator': self.generate_customers,
                'columns': ['first_name', 'last_name', 'email', 'phone', 'registration_date', 'address']
            },
            'categories': {
                'generator': self.generate_categories,
                'columns': ['category_name', 'description']
            },
            'products': {
                'generator': self.generate_products,
                'columns': ['product_name', 'description', 'price', 'category_id', 'brand', 'stock_qty', 'is_active']
            },
            'orders': {
                'generator': self.generate_orders,
                'columns': ['customer_id', 'order_date', 'total_amount', 'status', 'payment_method']
            },
            'order_items': {
                'generator': self.generate_order_items,
                'columns': ['order_id', 'product_id', 'quantity', 'unit_price']
            },
            'product_associations': {
                'generator': self.generate_product_associations,
                'columns': ['product_a_id', 'product_b_id', 'frequency_count', 'last_calculated']
            }
        }
        
        table_name_lower = table_name.lower()
        if table_name_lower not in table_generators:
            error_msg = f"Unknown table: {table_name}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            return False
        
        gen_msg = f"Generating {count:,} rows for {table_name}"
        self.log_message(gen_msg)
        print(f"🔄 {gen_msg}...")
        
        generator_info = table_generators[table_name_lower]
        try:
            data = generator_info['generator'](count)
            return self.insert_data(table_name, data, generator_info['columns'])
        except Exception as e:
            error_msg = f"Error generating data for {table_name}: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"❌ {error_msg}")
            # Try resetting Faker and retry once for customer generation
            if table_name_lower == 'customers' and 'unique' in str(e).lower():
                retry_msg = "Retrying customer generation with reset Faker..."
                self.log_message(retry_msg)
                print(f"🔄 {retry_msg}")
                self.reset_faker_unique()
                try:
                    data = generator_info['generator'](count)
                    return self.insert_data(table_name, data, generator_info['columns'])
                except Exception as retry_e:
                    retry_error_msg = f"Retry failed: {retry_e}"
                    self.log_message(retry_error_msg, 'ERROR')
                    print(f"❌ {retry_error_msg}")
            return False

    def refresh_existing_data(self):
        """Refresh the existing data cache after insertions"""
        return self.load_existing_data()

    def get_final_statistics(self):
        """Get final statistics and return as dictionary"""
        try:
            cursor = self.connection.cursor()
            
            tables = ['customers', 'categories', 'products', 'orders', 'order_items', 'product_associations']
            statistics = {}
            total_records = 0
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    statistics[table] = count
                    total_records += count
                except Exception as e:
                    error_msg = f"Error getting count for {table}: {e}"
                    self.log_message(error_msg, 'ERROR')
                    statistics[table] = 0
            
            cursor.close()
            statistics['total_records'] = total_records
            
            return statistics
            
        except Exception as e:
            error_msg = f"Could not generate statistics: {e}"
            self.log_message(error_msg, 'ERROR')
            return {}

    def show_final_statistics(self):
        """Show final statistics after large data generation"""
        statistics = self.get_final_statistics()
        
        if statistics:
            print(f"\n📊 Final Database Statistics:")
            print("=" * 40)
            
            for table, count in statistics.items():
                if table != 'total_records':
                    print(f"{table.capitalize():20}: {count:,} rows")
            
            print("=" * 40)
            print(f"{'Total Records':20}: {statistics.get('total_records', 0):,} rows")

    def get_memory_usage(self):
        """Get current memory usage in MB"""
        try:
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            return round(memory_mb, 2)
        except:
            return 0.0

    def get_environment_info(self):
        """Get system environment information"""
        try:
            return {
                'python_version': sys.version,
                'platform': sys.platform,
                'cpu_count': os.cpu_count(),
                'memory_mb': self.get_memory_usage(),
                'working_directory': os.getcwd()
            }
        except:
            return {}
    
    def get_complete_database_state(self):
        """Get complete database state with row counts for all tables"""
        try:
            cursor = self.connection.cursor()
            
            # Define all tables we want to track
            tables = [
                'categories', 'customers', 'products', 'orders', 
                'order_items', 'product_associations', 'analytics_runs', 
                'analytics_query_results', 'test_data_execution_log'
            ]
            
            database_state = {}
            total_records = 0
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    database_state[table] = count
                    total_records += count
                except psycopg2.Error as e:
                    # If table doesn't exist or access error, set to 0
                    self.log_message(f"Could not get count for {table}: {e}", 'WARNING')
                    database_state[table] = 0
            
            database_state['total_records'] = total_records
            
            # Add timestamp for when this state was captured
            database_state['captured_at'] = datetime.now().isoformat()
            
            cursor.close()
            return database_state
            
        except psycopg2.Error as e:
            error_msg = f"Could not capture database state: {e}"
            self.log_message(error_msg, 'ERROR')
            # Return empty state with timestamp
            return {
                'captured_at': datetime.now().isoformat(),
                'error': str(e),
                'total_records': 0
            }

    def store_execution_log(self, args, statistics, execution_status):
        """Store execution results with complete database state for performance analysis"""
        try:
            # Check if connection is still valid
            if not self.connection or self.connection.closed:
                warning_msg = "Database connection is closed, cannot store execution log"
                self.log_message(warning_msg, 'WARNING')
                print(f"⚠️  {warning_msg}")
                return
            
            cursor = self.connection.cursor()
            
            # Calculate execution metrics
            execution_end_time = datetime.now()
            total_execution_time_ms = (execution_end_time - self.execution_start_time).total_seconds() * 1000
            average_operation_time_ms = total_execution_time_ms / max(self.total_operations, 1)
            
            # Calculate total records created in this execution
            total_records_created = sum(self.records_created.values())
            
            # Get COMPLETE database state for performance analysis
            complete_database_state = self.get_complete_database_state()
            
            # Calculate data volume estimate (rough estimate: 100 bytes per record)
            data_volume_mb = (total_records_created * 100) / (1024 * 1024)
            
            # Prepare enhanced configuration with complete state tracking
            configuration_used = {
                'execution_details': {
                    'rows_per_table_requested': getattr(args, 'rows', 0),
                    'tables_processed': getattr(args, 'tables', []) if hasattr(args, 'tables') else [],
                    'batch_size': getattr(args, 'batch_size', 1000),
                    'skip_duplicates': getattr(args, 'skip_duplicates', False),
                    'update_associations': getattr(args, 'update_associations', False),
                    'all_tables': getattr(args, 'all', False),
                    'operation_type': 'association_update' if getattr(args, 'update_associations', False) else 'data_generation'
                },
                'records_created_this_execution': dict(self.records_created),
                'total_records_created_this_execution': total_records_created,
                'database_state_after_execution': complete_database_state,
                'performance_context': {
                    'execution_duration_seconds': round(total_execution_time_ms / 1000, 2),
                    'records_per_second': round(total_records_created / max(total_execution_time_ms / 1000, 0.1), 2) if total_records_created > 0 else 0,
                    'operations_per_second': round(self.total_operations / max(total_execution_time_ms / 1000, 0.1), 2),
                    'database_size_at_completion': complete_database_state.get('total_records', 0)
                }
            }
            
            # Enhanced tables affected tracking
            if getattr(args, 'update_associations', False):
                tables_affected = ['product_associations']
            else:
                tables_affected = list(self.records_created.keys())
            
            # NEW: Store the complete database state in a separate field for easy querying
            database_state_json = json.dumps(complete_database_state)
            
            # Insert execution log with enhanced data
            insert_query = """
                INSERT INTO Test_Data_Execution_Log (
                    execution_timestamp, script_name, script_version, execution_type,
                    database_host, database_name, total_operations, successful_operations,
                    failed_operations, total_execution_time_ms, average_operation_time_ms,
                    memory_usage_mb, records_created, tables_affected, data_volume_mb,
                    execution_status, error_count, warning_count, execution_log,
                    error_details, configuration_used, environment_info
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            # Prepare error details
            error_details = None
            if self.error_count > 0:
                error_logs = [log for log in self.execution_log if 'ERROR:' in log]
                error_details = '\n'.join(error_logs[-10:])  # Last 10 errors
            
            values = (
                self.execution_start_time,
                'generate_testdata.py',
                '1.0',
                configuration_used['execution_details']['operation_type'],
                self.db_config['host'],
                self.db_config['database'],
                self.total_operations,
                self.successful_operations,
                self.failed_operations,
                round(total_execution_time_ms, 2),
                round(average_operation_time_ms, 2),
                self.get_memory_usage(),
                total_records_created,
                json.dumps(tables_affected),
                round(data_volume_mb, 2),
                execution_status,
                self.error_count,
                self.warning_count,
                '\n'.join(self.execution_log[-50:]),  # Last 50 log entries
                error_details,
                json.dumps(configuration_used),  # Complete configuration including database state
                json.dumps(self.get_environment_info())
            )
            
            cursor.execute(insert_query, values)
            execution_id = cursor.lastrowid if cursor.lastrowid else "unknown"
            self.connection.commit()
            cursor.close()
            
            # Enhanced success message with complete database state
            print(f"✅ Execution log stored in Test_Data_Execution_Log table (ID: {execution_id})")
            
            # Show what was created in this execution
            if total_records_created > 0:
                print(f"📊 Created in this execution: {total_records_created:,} records across {len(self.records_created)} tables:")
                for table, count in self.records_created.items():
                    print(f"   • {table}: {count:,} rows")
            
            # Show complete database state for performance context
            if complete_database_state and complete_database_state.get('total_records', 0) > 0:
                print(f"📈 Complete database state after execution:")
                core_tables = ['categories', 'customers', 'products', 'orders', 'order_items', 'product_associations']
                for table in core_tables:
                    if table in complete_database_state:
                        count = complete_database_state[table]
                        print(f"   • {table.capitalize():<20}: {count:,} rows")
                print(f"   • {'Total Records':<20}: {complete_database_state['total_records']:,} rows")
            
            # Show performance context
            perf_context = configuration_used['performance_context']
            if perf_context['records_per_second'] > 0:
                print(f"⚡ Performance: {perf_context['records_per_second']:,.0f} records/sec, {perf_context['execution_duration_seconds']} seconds")
            
            self.log_message(f"Execution log stored with complete database state (ID: {execution_id})")
            
        except psycopg2.OperationalError as e:
            if "connection" in str(e).lower():
                warning_msg = f"Connection error while storing execution log: {e}"
                self.log_message(warning_msg, 'WARNING')
                print(f"⚠️  {warning_msg}")
            else:
                error_msg = f"Database error while storing execution log: {e}"
                self.log_message(error_msg, 'ERROR')
                print(f"⚠️  {error_msg}")
        except Exception as e:
            error_msg = f"Failed to store execution log: {e}"
            self.log_message(error_msg, 'ERROR')
            print(f"⚠️  {error_msg}")
            # Don't fail the entire operation if logging fails
            try:
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
            except:
                pass

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
    parser = argparse.ArgumentParser(description='Generate test data for database tables')
    parser.add_argument('--rows', type=int, default=1000, help='Number of rows to generate (default: 1000)')
    parser.add_argument('--tables', nargs='+', default=['customers', 'products'], 
                       help='Tables to generate data for (default: customers products)')
    parser.add_argument('--update-associations', action='store_true', 
                       help='Update product associations based on actual order data')
    parser.add_argument('--all', action='store_true', help='Generate data for all tables')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Batch size for database inserts (default: 1000)')
    parser.add_argument('--skip-duplicates', action='store_true',
                       help='Skip duplicate entries instead of failing (useful for large datasets)')
    
    args = parser.parse_args()
    
    print(f"🚀 Starting test data generation...")
    print(f"📊 Rows per table: {args.rows:,}")
    
    if args.rows > 100000:
        print(f"⚠️  Large dataset detected ({args.rows:,} rows)")
        print("💡 Tips for large datasets:")
        print("   - This may take several minutes")
        print("   - Consider using --skip-duplicates for retries")
        print("   - Database will be processed in batches")
        
        response = input("Continue? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            print("Operation cancelled.")
            return
    
    # Load database configuration
    db_config = load_environment()
    
    # Initialize generator
    generator = TestDataGenerator(db_config)
    
    if not generator.connect():
        sys.exit(1)
    
    execution_status = 'success'
    statistics = {}
    
    try:
        # Load existing data for foreign key validation
        if not generator.load_existing_data():
            sys.exit(1)
        
        # Handle association updates if requested
        if args.update_associations:
            generator.log_message("Starting product associations update")
            print("🔄 Updating product associations from order data...")
            
            # Define start time for associations update
            associations_start_time = datetime.now()
            
            try:
                if generator.update_product_associations_from_orders():
                    print("🎉 Association update completed!")
                    execution_status = 'success'
                else:
                    execution_status = 'partial_success'
                
                # Get final statistics
                statistics = generator.get_final_statistics()
                
                # Calculate execution summary for associations update
                completion_msg = f"Product associations update completed with status: {execution_status}"
                generator.log_message(completion_msg)
                print(f"\n🎉 {completion_msg}!")
                
                total_duration = datetime.now() - associations_start_time
                print(f"⏱️  Total time: {total_duration.total_seconds():.1f}s")
                
                # Show association statistics
                if statistics.get('product_associations', 0) > 0:
                    print(f"📊 Product associations in database: {statistics['product_associations']:,}")
                
            except KeyboardInterrupt:
                interruption_msg = "Association update interrupted by user"
                generator.log_message(interruption_msg, 'ERROR')
                print(f"\n⏹️  {interruption_msg}")
                execution_status = 'failure'
                statistics = generator.get_final_statistics()
            except Exception as e:
                error_msg = f"Unexpected error during association update: {e}"
                generator.log_message(error_msg, 'ERROR')
                print(f"❌ {error_msg}")
                execution_status = 'failure'
                statistics = generator.get_final_statistics()
            
            # DON'T store execution log or disconnect here - let the main finally block handle it
            return  # Exit after handling associations
        
        # Determine which tables to process
        if args.all:
            tables = ['categories', 'customers', 'products', 'orders', 'order_items', 'product_associations']
        else:
            tables = [table.lower() for table in args.tables]
        
        generator.log_message(f"Processing tables: {', '.join(tables)}")
        print(f"📋 Processing tables: {', '.join(tables)}")
        
        # Process tables in dependency order
        table_order = ['categories', 'customers', 'products', 'orders', 'order_items', 'product_associations']
        ordered_tables = [table for table in table_order if table in tables]
        
        success_count = 0
        total_start_time = datetime.now()
        
        for table in ordered_tables:
            table_start_time = datetime.now()
            generator.log_message(f"Starting processing of {table}")
            print(f"\n📦 Processing {table}...")
            
            if generator.generate_table_data(table, args.rows):
                success_count += 1
                # Refresh existing data after each table for foreign key dependencies
                generator.refresh_existing_data()
                
                table_duration = datetime.now() - table_start_time
                duration_msg = f"{table} completed in {table_duration.total_seconds():.1f}s"
                generator.log_message(duration_msg)
                print(f"✅ {duration_msg}")
            else:
                warning_msg = f"Failed to generate data for {table}, continuing with next table..."
                generator.log_message(warning_msg, 'WARNING')
                print(f"⚠️  {warning_msg}")
        
        total_duration = datetime.now() - total_start_time
        
        # Determine final execution status
        if success_count == len(ordered_tables):
            execution_status = 'success'
        elif success_count > 0:
            execution_status = 'partial_success'
        else:
            execution_status = 'failure'
        
        completion_msg = f"Test data generation completed with status: {execution_status}"
        generator.log_message(completion_msg)
        print(f"\n🎉 {completion_msg}!")
        print(f"✅ Successfully processed {success_count}/{len(ordered_tables)} tables")
        print(f"⏱️  Total time: {total_duration.total_seconds():.1f}s")
        
        if args.rows > 10000:
            # Show final statistics for large datasets
            generator.show_final_statistics()
        
        # Get final statistics
        statistics = generator.get_final_statistics()
        
    except KeyboardInterrupt:
        interruption_msg = "Generation interrupted by user"
        generator.log_message(interruption_msg, 'ERROR')
        print(f"\n⏹️  {interruption_msg}")
        execution_status = 'failure'
        statistics = generator.get_final_statistics()
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        generator.log_message(error_msg, 'ERROR')
        print(f"❌ {error_msg}")
        execution_status = 'failure'
        statistics = generator.get_final_statistics()
    finally:
        # Always store execution log before disconnecting
        try:
            generator.store_execution_log(args, statistics, execution_status)
        except Exception as log_error:
            print(f"⚠️  Failed to store execution log: {log_error}")
        
        generator.disconnect()


if __name__ == "__main__":
    main()