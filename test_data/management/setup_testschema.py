"""
Testdata for running example with analytics tables support
"""

import sys
import argparse
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
    DROP TABLE IF EXISTS Analytics_Query_Results CASCADE;
    DROP TABLE IF EXISTS Analytics_Runs CASCADE;
    DROP TABLE IF EXISTS Test_Data_Execution_Log CASCADE;
    DROP TABLE IF EXISTS Product_Associations CASCADE;
    DROP TABLE IF EXISTS Order_Items CASCADE;
    DROP TABLE IF EXISTS Orders CASCADE;
    DROP TABLE IF EXISTS Products CASCADE;
    DROP TABLE IF EXISTS Categories CASCADE;
    DROP TABLE IF EXISTS Customers CASCADE;

     -- Categories table WITH HIERARCHY SUPPORT
    CREATE TABLE Categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(255) NOT NULL UNIQUE,
        description TEXT,
        parent_category_id INTEGER REFERENCES Categories(category_id) ON DELETE CASCADE,
        level_depth INTEGER DEFAULT 0 CHECK (level_depth >= 0),
        hierarchy_path TEXT, -- Stores path like "1/5/12" for fast queries
        is_leaf BOOLEAN DEFAULT TRUE, -- True if no children exist
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create hierarchical indexes for performance
    CREATE INDEX idx_categories_parent ON Categories(parent_category_id);
    CREATE INDEX idx_categories_level ON Categories(level_depth);
    CREATE INDEX idx_categories_hierarchy_path ON Categories USING GIN (string_to_array(hierarchy_path, '/'));
    CREATE INDEX idx_categories_leaf ON Categories(is_leaf) WHERE is_leaf = TRUE;

    -- Function to update hierarchy path and level
    CREATE OR REPLACE FUNCTION update_category_hierarchy()
    RETURNS TRIGGER AS $$
    BEGIN
        -- Calculate level depth
        IF NEW.parent_category_id IS NULL THEN
            NEW.level_depth := 0;
            NEW.hierarchy_path := NEW.category_id::TEXT;
        ELSE
            SELECT level_depth + 1, hierarchy_path || '/' || NEW.category_id::TEXT
            INTO NEW.level_depth, NEW.hierarchy_path
            FROM Categories 
            WHERE category_id = NEW.parent_category_id;
            
            -- Update parent to not be a leaf anymore
            UPDATE Categories 
            SET is_leaf = FALSE 
            WHERE category_id = NEW.parent_category_id AND is_leaf = TRUE;
        END IF;
        
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Trigger to maintain hierarchy
    CREATE TRIGGER category_hierarchy_trigger
        BEFORE INSERT OR UPDATE ON Categories
        FOR EACH ROW
        EXECUTE FUNCTION update_category_hierarchy();



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

     -- Analytics_Runs table for storing analytics execution metadata
    CREATE TABLE Analytics_Runs (
        run_id SERIAL PRIMARY KEY,
        export_timestamp TIMESTAMP NOT NULL,
        database_host VARCHAR(255),
        database_name VARCHAR(255),
        total_queries_executed INTEGER,
        successful_queries INTEGER,
        execution_order TEXT, -- JSON array of query names
        script_version VARCHAR(50),
        description TEXT,
        
        -- Configuration
        display_limit INTEGER,
        sample_data_limit INTEGER,
        export_filename_template VARCHAR(255),
        timestamp_format VARCHAR(50),
        
        -- Performance Summary
        total_execution_time_ms DECIMAL(10,2),
        total_rows_queried INTEGER,
        average_response_time_ms DECIMAL(10,2),
        success_rate_percent DECIMAL(5,2),
        
        -- Additional metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        analytics_json TEXT -- Store the complete JSON for backup/reference
    );

    -- Analytics_Query_Results table for individual query results
    CREATE TABLE Analytics_Query_Results (
        result_id SERIAL PRIMARY KEY,
        run_id INTEGER NOT NULL,
        
        -- Query Information
        query_name VARCHAR(255) NOT NULL,
        query_description TEXT,
        dataset_reference VARCHAR(100),
        query TEXT,
        affected_tables TEXT, -- JSON array of table names
        execution_timestamp TIMESTAMP,
        execution_order INTEGER,
        
        -- Performance Metrics
        response_time_ms DECIMAL(10,2),
        response_time_seconds DECIMAL(10,2),
        rows_returned INTEGER,
        columns_returned INTEGER,
        
        -- Data Structure
        column_names TEXT, -- JSON array of column names
        sample_data TEXT, -- JSON array of sample data rows
        data_types TEXT, -- JSON array of data types
        
        -- Results Summary
        has_data BOOLEAN,
        first_row TEXT, -- JSON array of first row data
        total_data_points INTEGER,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        system VARCHAR(255),
        
        FOREIGN KEY (run_id) REFERENCES Analytics_Runs(run_id) ON DELETE CASCADE
    );

    -- Test_Data_Execution_Log table for logging script executions
    CREATE TABLE Test_Data_Execution_Log (
        execution_id SERIAL PRIMARY KEY,
        
        -- Execution metadata
        execution_timestamp TIMESTAMP NOT NULL,
        script_name VARCHAR(255),
        script_version VARCHAR(50),
        execution_type VARCHAR(100), -- e.g., 'create_testdata', 'data_generation', 'schema_setup'
        
        -- Execution details
        database_host VARCHAR(255),
        database_name VARCHAR(255),
        total_operations INTEGER,
        successful_operations INTEGER,
        failed_operations INTEGER,
        
        -- Performance metrics
        total_execution_time_ms DECIMAL(10,2),
        average_operation_time_ms DECIMAL(10,2),
        memory_usage_mb DECIMAL(10,2),
        
        -- Data generation statistics (if applicable)
        records_created INTEGER,
        tables_affected TEXT, -- JSON array of affected table names
        data_volume_mb DECIMAL(10,2),
        
        -- Status and results
        execution_status VARCHAR(50), -- 'success', 'partial_success', 'failure'
        error_count INTEGER DEFAULT 0,
        warning_count INTEGER DEFAULT 0,
        
        -- Detailed logs
        execution_log TEXT, -- Detailed execution log/output
        error_details TEXT, -- Error messages and stack traces
        configuration_used TEXT, -- JSON of configuration parameters used
        
        -- Additional metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        environment_info TEXT -- JSON with system/environment details
    );

    -- Create indexes for better performance
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

    -- Analytics table indexes
    CREATE INDEX idx_analytics_runs_timestamp ON Analytics_Runs(export_timestamp);
    CREATE INDEX idx_analytics_runs_database ON Analytics_Runs(database_name);
    CREATE INDEX idx_analytics_runs_version ON Analytics_Runs(script_version);
    CREATE INDEX idx_query_results_run_id ON Analytics_Query_Results(run_id);
    CREATE INDEX idx_query_results_name ON Analytics_Query_Results(query_name);
    CREATE INDEX idx_query_results_timestamp ON Analytics_Query_Results(execution_timestamp);
    CREATE INDEX idx_query_results_performance ON Analytics_Query_Results(response_time_ms, rows_returned);
    CREATE INDEX idx_execution_log_timestamp ON Test_Data_Execution_Log(execution_timestamp);
    CREATE INDEX idx_execution_log_script ON Test_Data_Execution_Log(script_name);
    CREATE INDEX idx_execution_log_status ON Test_Data_Execution_Log(execution_status);
    CREATE INDEX idx_execution_log_type ON Test_Data_Execution_Log(execution_type);

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

    -- Insert sample analytics run
    INSERT INTO Analytics_Runs (
        export_timestamp, database_host, database_name, total_queries_executed,
        successful_queries, execution_order, script_version, description,
        display_limit, sample_data_limit, total_execution_time_ms,
        total_rows_queried, average_response_time_ms, success_rate_percent
    ) VALUES (
        CURRENT_TIMESTAMP, 'localhost', 'test_data', 4, 4,
        '["favorite_products", "favorite_categories", "customer_product_patterns", "product_associations"]',
        '2.0_flexible', 'Sample analytics run with test data',
        5, 3, 1000.0, 1000, 250.0, 100.0
    );

    -- Insert sample execution log
    INSERT INTO Test_Data_Execution_Log (
        execution_timestamp, script_name, script_version, execution_type,
        database_host, database_name, total_operations, successful_operations,
        failed_operations, execution_status, records_created
    ) VALUES (
        CURRENT_TIMESTAMP, 'setup_testschema.py', '1.0', 'schema_setup',
        'localhost', 'test_data', 1, 1, 0, 'success', 23
    );
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
        
        expected_tables = {
            'categories', 'customers', 'orders', 'order_items', 
            'products', 'product_associations', 'analytics_runs', 
            'analytics_query_results', 'test_data_execution_log'
        }
        created_tables = {table[0] for table in tables}
        
        if expected_tables.issubset(created_tables):
            print(f"✅ All tables created successfully:")
            print(f"   Core tables: categories, customers, orders, order_items, products, product_associations")
            print(f"   Analytics tables: analytics_runs, analytics_query_results, test_data_execution_log")
            return True
        else:
            missing = expected_tables - created_tables
            print(f"❌ Missing tables: {', '.join(missing)}")
            return False
            
    except psycopg2.Error as e:
        print(f"❌ Error verifying schema: {e}")
        return False

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Setup database schema with optional test data creation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
            Examples:
            python setup_testschema.py                    # Create schema only
            python setup_testschema.py --create-testdata  # Create schema with sample data
            python setup_testschema.py -h                 # Show this help
        '''
    )
    
    parser.add_argument(
        '--create-testdata', 
        action='store_true',
        help='Create sample test data after schema creation (default: False)'
    )
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    print("🚀 Starting database schema setup...")
    if args.create_testdata:
        print("📊 Test data creation: ENABLED")
    else:
        print("📊 Test data creation: DISABLED (use --create-testdata to enable)")
    
    db_config = load_environment()
    print(f"📡 Connecting to database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    connection = create_connection(db_config)
    if not connection:
        sys.exit(1)
    
    try:
        # Always create the schema
        if not execute_sql(connection, get_schema_sql(), "Schema creation"):
            sys.exit(1)

        if not verify_schema(connection):
            sys.exit(1)
        
        # Create test data if requested
        if args.create_testdata:
            print("\n📊 Creating sample test data...")
            if execute_sql(connection, get_sample_data_sql(), "Sample data insertion"):
                print("✅ Sample data inserted successfully")
            else:
                print("⚠️  Schema created but sample data insertion failed")
                sys.exit(1)
        else:
            print("\n💡 To create sample data later, run:")
            print("   python setup_testschema.py --create-testdata")
        
        print("\n🎉 Database setup completed successfully!")
        print(f"📊 You can now connect to your database at {db_config['host']}:{db_config['port']}")
        
        # Show what was created
        print("\n📋 Created tables:")
        print("   • Core e-commerce tables: Categories, Products, Customers, Orders, Order_Items, Product_Associations")
        print("   • Analytics tables: Analytics_Runs, Analytics_Query_Results")
        print("   • Logging table: Test_Data_Execution_Log")
        
    finally:
        connection.close()

if __name__ == "__main__":
    main()