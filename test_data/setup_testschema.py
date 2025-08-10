"""
Testdata for running example
"""

import sys
import psycopg2
from decouple import config

def load_environment():
    db_config = {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': config('DB_PORT', default=5433, cast=int),
        'connect_timeout': config('DB_CONNECT_TIMEOUT', default=10, cast=int)
    }
    
    return db_config

def get_schema_sql():
    return """
    -- Drop tables if they exist (in correct order to handle foreign keys)
    DROP TABLE IF EXISTS Product_Associations CASCADE;
    DROP TABLE IF EXISTS Order_Items CASCADE;
    DROP TABLE IF EXISTS Orders CASCADE;
    DROP TABLE IF EXISTS Products CASCADE;
    DROP TABLE IF EXISTS Categories CASCADE;
    DROP TABLE IF EXISTS Customers CASCADE;

    -- Categories table
    CREATE TABLE Categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(255) NOT NULL UNIQUE,
        description TEXT,
        parent_category_id INTEGER REFERENCES Categories(category_id) ON DELETE SET NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Products table (with proper foreign key to Categories)
    CREATE TABLE Products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        description TEXT,
        price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
        category_id INTEGER NOT NULL REFERENCES Categories(category_id) ON DELETE RESTRICT,
        brand VARCHAR(255),
        stock_qty INTEGER DEFAULT 0 CHECK (stock_qty >= 0),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Customers table
    CREATE TABLE Customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        phone VARCHAR(20),
        registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        address TEXT
    );

    -- Orders table
    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES Customers(customer_id) ON DELETE CASCADE,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
        status VARCHAR(50) DEFAULT 'pending',
        payment_method VARCHAR(50)
    );

    -- Order_Items table
    CREATE TABLE Order_Items (
        order_item_id SERIAL PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES Orders(order_id) ON DELETE CASCADE,
        product_id INTEGER NOT NULL REFERENCES Products(product_id) ON DELETE RESTRICT,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
        total_price DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
    );

    -- Product_Associations table
    CREATE TABLE Product_Associations (
        association_id SERIAL PRIMARY KEY,
        product_a_id INTEGER NOT NULL REFERENCES Products(product_id) ON DELETE CASCADE,
        product_b_id INTEGER NOT NULL REFERENCES Products(product_id) ON DELETE CASCADE,
        frequency_count INTEGER DEFAULT 1 CHECK (frequency_count > 0),
        last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CHECK (product_a_id != product_b_id),
        UNIQUE(product_a_id, product_b_id)
    );

    -- Create indexes for better performance
    CREATE INDEX idx_categories_parent ON Categories(parent_category_id);
    CREATE INDEX idx_categories_name ON Categories(category_name);
    CREATE INDEX idx_products_category ON Products(category_id);
    CREATE INDEX idx_products_brand ON Products(brand);
    CREATE INDEX idx_products_active ON Products(is_active);
    CREATE INDEX idx_customers_email ON Customers(email);
    CREATE INDEX idx_orders_customer ON Orders(customer_id);
    CREATE INDEX idx_orders_date ON Orders(order_date);
    CREATE INDEX idx_orders_status ON Orders(status);
    CREATE INDEX idx_order_items_order ON Order_Items(order_id);
    CREATE INDEX idx_order_items_product ON Order_Items(product_id);
    CREATE INDEX idx_associations_product_a ON Product_Associations(product_a_id);
    CREATE INDEX idx_associations_product_b ON Product_Associations(product_b_id);

    -- Create update timestamp trigger function
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    -- Add update triggers for tables with updated_at columns
    CREATE TRIGGER update_categories_updated_at BEFORE UPDATE ON Categories
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

    CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON Products
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """

def get_sample_data_sql():
    return """
    -- Insert sample categories
    INSERT INTO Categories (category_name, description) VALUES 
        ('Electronics', 'Electronic devices and accessories'),
        ('Clothing', 'Apparel and fashion items'),
        ('Books', 'Books and educational materials'),
        ('Home & Garden', 'Home improvement and garden supplies');

    -- Insert subcategories
    INSERT INTO Categories (category_name, description, parent_category_id) VALUES 
        ('Smartphones', 'Mobile phones and accessories', 1),
        ('Laptops', 'Portable computers', 1),
        ('Men''s Clothing', 'Clothing for men', 2),
        ('Women''s Clothing', 'Clothing for women', 2);

    -- Insert sample products
    INSERT INTO Products (product_name, description, price, category_id, brand, stock_qty) VALUES 
        ('iPhone 15 Pro', 'Latest Apple smartphone', 1199.99, 5, 'Apple', 50),
        ('Samsung Galaxy S24', 'Samsung flagship phone', 999.99, 5, 'Samsung', 30),
        ('MacBook Air M3', 'Apple laptop with M3 chip', 1299.99, 6, 'Apple', 25),
        ('Dell XPS 13', 'Premium ultrabook', 1099.99, 6, 'Dell', 20),
        ('Men''s T-Shirt', 'Cotton t-shirt', 29.99, 7, 'Generic', 100),
        ('Women''s Dress', 'Summer dress', 79.99, 8, 'Fashion Brand', 45);

    -- Insert sample customers
    INSERT INTO Customers (first_name, last_name, email, phone, address) VALUES 
        ('John', 'Doe', 'john.doe@email.com', '+1234567890', '123 Main St, City, State'),
        ('Jane', 'Smith', 'jane.smith@email.com', '+1234567891', '456 Oak Ave, City, State'),
        ('Bob', 'Johnson', 'bob.johnson@email.com', '+1234567892', '789 Pine Rd, City, State');

    -- Insert sample orders
    INSERT INTO Orders (customer_id, total_amount, status, payment_method) VALUES 
        (1, 1229.98, 'completed', 'credit_card'),
        (2, 79.99, 'pending', 'paypal'),
        (3, 2199.98, 'shipped', 'credit_card');

    -- Insert sample order items
    INSERT INTO Order_Items (order_id, product_id, quantity, unit_price) VALUES 
        (1, 1, 1, 1199.99),  -- iPhone 15 Pro
        (1, 5, 1, 29.99),    -- Men's T-Shirt
        (2, 6, 1, 79.99),    -- Women's Dress
        (3, 1, 1, 1199.99),  -- iPhone 15 Pro
        (3, 3, 1, 1299.99);  -- MacBook Air M3

    -- Insert sample product associations
    INSERT INTO Product_Associations (product_a_id, product_b_id, frequency_count) VALUES 
        (1, 3, 15),  -- iPhone often bought with MacBook
        (1, 5, 8),   -- iPhone often bought with T-Shirt
        (3, 1, 15);  -- MacBook often bought with iPhone
    """

def create_connection(db_config):
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_sql(connection, sql_script, description):
    try:
        cursor = connection.cursor()
        cursor.execute(sql_script)
        connection.commit()
        cursor.close()
        print(f"✅ {description} completed successfully")
        return True
    except psycopg2.Error as e:
        print(f"❌ Error during {description}: {e}")
        connection.rollback()
        return False

def verify_schema(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        cursor.close()
        
        expected_tables = {'categories', 'customers', 'orders', 'order_items', 'products', 'product_associations'}
        created_tables = {table[0] for table in tables}
        
        if expected_tables.issubset(created_tables):
            print(f"✅ All tables created successfully: {', '.join(sorted(created_tables))}")
            return True
        else:
            missing = expected_tables - created_tables
            print(f"❌ Missing tables: {', '.join(missing)}")
            return False
            
    except psycopg2.Error as e:
        print(f"❌ Error verifying schema: {e}")
        return False

def main():
    print("🚀 Starting database schema setup...")
    
    db_config = load_environment()
    print(f"📡 Connecting to database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    connection = create_connection(db_config)
    if not connection:
        sys.exit(1)
    
    try:
        if not execute_sql(connection, get_schema_sql(), "Schema creation"):
            sys.exit(1)

        if not verify_schema(connection):
            sys.exit(1)
            
        response = input("\n💡 Would you like to insert sample data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            if execute_sql(connection, get_sample_data_sql(), "Sample data insertion"):
                print("✅ Sample data inserted successfully")
            else:
                print("⚠️  Schema created but sample data insertion failed")
        
        print("\n🎉 Database setup completed successfully!")
        print(f"📊 You can now connect to your database at {db_config['host']}:{db_config['port']}")
        
    finally:
        connection.close()

if __name__ == "__main__":
    main()