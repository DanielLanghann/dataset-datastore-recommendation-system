#!/usr/bin/env python3
"""
Progressive database scaling and performance testing script with database result storage
"""
import os
import sys
import time
import json
import re
import subprocess
import psycopg2
from datetime import datetime
from decimal import Decimal
from decouple import config
from pathlib import Path

class ProgressivePerformanceTester:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.test_results = []
        self.current_iteration = 0
        self.start_time = datetime.now()
        self.test_session_id = self.start_time.strftime('%Y%m%d_%H%M%S')

        # Initial data amounts
        self.initial_data = {
            'categories': 2000,
            'customers': 1000,
            'products': 4000,
            'orders': 8000,
            'order_items': 16000
        }

        # Growth multipliers
        self.growth_multipliers = {
            'categories': 1.25,
            'customers': 2.0,
            'products': 2.0,
            'orders': 2.5,
            'order_items': 2.5
        }

        # Target limit (stop when orders reach this)
        self.max_orders = 1000000
        
        # Performance optimization settings
        self.use_optimized_associations = True  # Use direct DB updates for large datasets
        self.association_timeout_seconds = 3600  # 1 hour timeout for associations
        self.batch_size = 10000  # Batch size for data operations

        # Current data counts (start with initial)
        self.current_data = self.initial_data.copy()

        # Script paths
        self.script_dir = Path(__file__).parent
        self.generate_script = self.script_dir / "generate_testdata.py"
        self.queries_script = self.script_dir / "perform_queries.py"

        print(f"🚀 Progressive Performance Tester Initialized")
        print(f"📁 Script directory: {self.script_dir}")
        print(f"🎯 Target: Stop when orders reach {self.max_orders:,}")
        print(f"📊 Initial data: {self.initial_data}")
        print(f"📈 Growth multipliers: {self.growth_multipliers}")
        print(f"🆔 Test session ID: {self.test_session_id}")
        print(f"⚡ Optimizations enabled:")
        print(f"   • Intelligent associations algorithm: {self.use_optimized_associations}")
        print(f"   • Association timeout: {self.association_timeout_seconds}s")
        print(f"   • Batch size: {self.batch_size:,}")
        print(f"   • Associations are ALWAYS computed (never skipped)")

    def test_setup(self):
        """Test all components before running the full loop"""
        print(f"\n🔬 Testing setup before starting loop...")
        
        # Test 1: Database connection
        print(f"\n1️⃣ Testing database connection...")
        if not self.connect():
            print(f"❌ Database connection failed")
            return False
        print(f"✅ Database connection successful")
        
        # Test 2: Check required scripts exist
        print(f"\n2️⃣ Testing script availability...")
        if not self.generate_script.exists():
            print(f"❌ Generate script not found: {self.generate_script}")
            return False
        print(f"✅ Generate script found: {self.generate_script}")
        
        if not self.queries_script.exists():
            print(f"❌ Queries script not found: {self.queries_script}")
            return False
        print(f"✅ Queries script found: {self.queries_script}")
        
        # Test 3: Test small data generation
        print(f"\n3️⃣ Testing data generation...")
        test_cmd = [sys.executable, str(self.generate_script), '--table', 'categories', '--rows', '10']
        try:
            result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print(f"✅ Data generation test successful")
            else:
                print(f"❌ Data generation test failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"❌ Data generation test error: {e}")
            return False
            
        # Test 4: Test query script
        print(f"\n4️⃣ Testing query execution...")
        query_cmd = [sys.executable, str(self.queries_script)]
        try:
            result = subprocess.run(query_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"✅ Query execution test successful")
                print(f"📊 Query output preview:")
                lines = result.stdout.split('\n')[:10]
                for line in lines:
                    if line.strip():
                        print(f"    {line[:80]}...")
            else:
                print(f"❌ Query execution test failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"❌ Query execution test error: {e}")
            return False
            
        # Test 5: Test analytics storage
        print(f"\n5️⃣ Testing analytics storage...")
        test_analytics = {
            'total_queries': 2,
            'successful_queries': 2,
            'total_execution_time': 100.0,
            'queries': [
                {'name': 'test_query_1', 'execution_time_ms': 50.0, 'rows_returned': 10},
                {'name': 'test_query_2', 'execution_time_ms': 50.0, 'rows_returned': 5}
            ],
            'execution_order': ['test_query_1', 'test_query_2'],
            'average_response_time': 50.0
        }
        
        test_iteration = {
            'iteration': 0,
            'timestamp': datetime.now().isoformat(),
            'final_counts': {'test': 100}
        }
        
        analytics_id = self.create_analytics_run_entry(test_iteration, test_analytics)
        if analytics_id:
            print(f"✅ Analytics storage test successful (run_id: {analytics_id})")
        else:
            print(f"❌ Analytics storage test failed")
            return False
            
        # Test 6: Test execution log storage
        print(f"\n6️⃣ Testing execution log storage...")
        test_exec_data = {
            'iteration': 0,
            'timestamp': datetime.now().isoformat(),
            'targets': {'test': 100},
            'initial_counts': {'test': 90},
            'final_counts': {'test': 100},
            'rows_added': {'categories': 10},
            'data_generation_successful': True,
            'associations_successful': True,
            'queries_successful': True,
            'total_duration': 1.0,
            'query_results': test_analytics
        }
        
        exec_id = self.store_iteration_results(test_exec_data, analytics_id)
        if exec_id:
            print(f"✅ Execution log storage test successful (exec_id: {exec_id})")
        else:
            print(f"❌ Execution log storage test failed")
            return False
            
        print(f"\n🎉 All tests passed! Ready to start progressive testing.")
        return True

    def connect(self):
        """Connect to database with optimizations"""
        try:
            # Add connection optimizations for better performance
            optimized_config = self.db_config.copy()
            optimized_config.update({
                'connect_timeout': 30,
                'application_name': f'testdata_loop_{self.test_session_id}'
            })
            
            self.connection = psycopg2.connect(**optimized_config)
            self.connection.autocommit = False  # Use transactions for better control
            
            # Set session parameters for better performance (only those that can be changed)
            cursor = self.connection.cursor()
            try:
                cursor.execute("""
                    SET work_mem = '256MB';
                    SET maintenance_work_mem = '512MB';
                    SET random_page_cost = 1.1;
                """)
                self.connection.commit()
                print(f"✅ Applied performance optimizations")
            except psycopg2.Error as e:
                print(f"⚠️  Could not apply all optimizations: {e}")
                self.connection.rollback()
            finally:
                cursor.close()
            
            print(f"✅ Connected to PostgreSQL: {self.db_config['host']}:{self.db_config['port']}")
            print(f"   Database: {self.db_config['database']}")
            return True
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

    def get_current_row_counts(self):
        """Get current row counts from database"""
        try:
            cursor = self.connection.cursor()
            counts = {}
            tables = ['categories', 'customers', 'products', 'orders', 'order_items', 'product_associations']

            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]

            cursor.close()
            return counts
        except psycopg2.Error as e:
            print(f"❌ Error getting row counts: {e}")
            return {}

    def get_latest_analytics_run_id(self):
        """Get the ID of the most recent analytics run"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT run_id, export_timestamp, total_queries_executed, successful_queries
                FROM Analytics_Runs
                ORDER BY export_timestamp DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            cursor.close()

            if result:
                return {
                    'run_id': result[0],
                    'timestamp': result[1],
                    'total_queries': result[2],
                    'successful_queries': result[3]
                }
            return None
        except psycopg2.Error as e:
            print(f"❌ Error getting latest analytics run: {e}")
            return None

    def calculate_rows_to_add(self, current_counts):
        """Calculate how many rows to add for each table"""
        rows_to_add = {}

        for table in ['categories', 'customers', 'products', 'orders', 'order_items']:
            target_count = int(self.current_data[table])
            current_count = current_counts.get(table, 0)
            rows_needed = max(0, target_count - current_count)
            rows_to_add[table] = rows_needed
            print(f"  {table:15}: Current={current_count:,}, Target={target_count:,}, Add={rows_needed:,}")

        return rows_to_add

    def update_data_targets_for_next_iteration(self):
        """Update data targets for next iteration based on growth multipliers"""
        print(f"\n📈 Calculating targets for iteration {self.current_iteration + 1}:")

        for table, multiplier in self.growth_multipliers.items():
            old_target = self.current_data[table]
            new_target = int(old_target * multiplier)
            self.current_data[table] = new_target
            growth = new_target - old_target
            print(f"  {table:15}: {old_target:,} → {new_target:,} (+{growth:,})")

    def run_data_generation(self, rows_to_add):
        """Run the data generation script for specified tables and amounts"""
        print(f"\n🔄 Running data generation...")

        tables_to_process = [table for table, count in rows_to_add.items() if count > 0]

        if not tables_to_process:
            print("  ℹ️ No data generation needed")
            return True

        table_order = ['categories', 'customers', 'products', 'orders', 'order_items']
        ordered_tables = [table for table in table_order if table in tables_to_process]

        success_count = 0
        total_tables = len(ordered_tables)

        for table in ordered_tables:
            rows_needed = rows_to_add[table]
            if rows_needed <= 0:
                continue

            print(f"\n  📦 Generating {rows_needed:,} rows for {table}...")

            cmd = [
                sys.executable, str(self.generate_script),
                '--table', table,
                '--rows', str(rows_needed)
            ]

            try:
                start_time = time.time()
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                end_time = time.time()

                if result.returncode == 0:
                    duration = end_time - start_time
                    print(f"    ✅ {table} completed in {duration:.1f}s")
                    success_count += 1
                else:
                    print(f"    ❌ {table} failed:")
                    print(f"    Error: {result.stderr}")

            except subprocess.TimeoutExpired:
                print(f"    ⏰ {table} timed out after 1 hour")
            except Exception as e:
                print(f"    ❌ {table} error: {e}")

        print(f"\n  📊 Data generation summary: {success_count}/{total_tables} tables successful")
        return success_count > 0

    def update_product_associations(self):
        """Update product associations with highly optimized algorithm for large datasets"""
        print(f"\n🔗 Updating product associations...")
        
        # Get current order_items count to choose the best strategy
        current_counts = self.get_current_row_counts()
        order_items_count = current_counts.get('order_items', 0)
        
        print(f"  📊 Processing {order_items_count:,} order_items")
        
        if order_items_count < 50000:
            # Small dataset - use simple approach
            print(f"  � Using simple approach for small dataset")
            return self.update_associations_simple()
        elif order_items_count < 200000:
            # Medium dataset - use optimized single query
            print(f"  🚀 Using optimized single-query approach")
            return self.update_associations_optimized()
        else:
            # Large dataset - use incremental batch processing
            print(f"  � Using incremental batch processing for large dataset")
            return self.update_associations_incremental()
    
    def update_associations_simple(self):
        """Simple associations update for small datasets"""
        try:
            start_time = time.time()
            cursor = self.connection.cursor()
            
            # Clear existing associations
            cursor.execute("DELETE FROM product_associations")
            
            # Simple approach - works well for small datasets
            cursor.execute("""
                INSERT INTO product_associations (product_a_id, product_b_id, frequency_count, last_calculated)
                SELECT 
                    LEAST(oi1.product_id, oi2.product_id) as product_a_id,
                    GREATEST(oi1.product_id, oi2.product_id) as product_b_id,
                    COUNT(*) as frequency_count,
                    CURRENT_TIMESTAMP
                FROM order_items oi1
                JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                WHERE oi1.product_id != oi2.product_id
                GROUP BY 
                    LEAST(oi1.product_id, oi2.product_id),
                    GREATEST(oi1.product_id, oi2.product_id)
                HAVING COUNT(*) >= 2
            """)
            
            rows_created = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            duration = time.time() - start_time
            print(f"  ✅ Created {rows_created:,} associations in {duration:.1f}s")
            return True, duration
            
        except Exception as e:
            print(f"  ❌ Error in simple associations: {e}")
            if self.connection:
                self.connection.rollback()
            return False, 0
    
    def update_associations_optimized(self):
        """Optimized associations update using CTE for medium datasets"""
        try:
            start_time = time.time()
            cursor = self.connection.cursor()
            
            print(f"  🗑️  Clearing existing associations...")
            cursor.execute("DELETE FROM product_associations")
            
            print(f"  🔄 Computing associations with optimized query...")
            
            # Optimized query using CTE to avoid repeated work
            cursor.execute("""
                INSERT INTO product_associations (product_a_id, product_b_id, frequency_count, last_calculated)
                WITH distinct_pairs AS (
                    SELECT DISTINCT 
                        oi1.order_id,
                        LEAST(oi1.product_id, oi2.product_id) as product_a_id,
                        GREATEST(oi1.product_id, oi2.product_id) as product_b_id
                    FROM order_items oi1
                    JOIN order_items oi2 ON oi1.order_id = oi2.order_id 
                        AND oi1.product_id < oi2.product_id  -- Only one direction to avoid duplicates
                ),
                aggregated AS (
                    SELECT 
                        product_a_id,
                        product_b_id,
                        COUNT(*) as frequency_count
                    FROM distinct_pairs
                    GROUP BY product_a_id, product_b_id
                    HAVING COUNT(*) >= 2
                )
                SELECT 
                    product_a_id,
                    product_b_id,
                    frequency_count,
                    CURRENT_TIMESTAMP
                FROM aggregated
            """)
            
            rows_created = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            duration = time.time() - start_time
            print(f"  ✅ Created {rows_created:,} associations in {duration:.1f}s")
            return True, duration
            
        except Exception as e:
            print(f"  ❌ Error in optimized associations: {e}")
            if self.connection:
                self.connection.rollback()
            return False, 0
    
    def update_associations_incremental(self):
        """Incremental batch processing for very large datasets"""
        try:
            start_time = time.time()
            cursor = self.connection.cursor()
            
            print(f"  🗑️  Clearing existing associations...")
            cursor.execute("DELETE FROM product_associations")
            
            # Get the range of order IDs to process in batches
            cursor.execute("SELECT MIN(order_id), MAX(order_id), COUNT(*) FROM orders")
            min_order, max_order, total_orders = cursor.fetchone()
            
            if not min_order or not max_order:
                print(f"  ⚠️  No orders found")
                return True, 0
                
            print(f"  📊 Processing {total_orders:,} orders in batches...")
            print(f"  📋 Order ID range: {min_order} to {max_order}")
            
            # Calculate batch size based on total orders
            batch_size = max(1000, min(10000, total_orders // 20))  # 20 batches max
            total_associations = 0
            batch_count = 0
            
            # Process in batches
            for batch_start in range(min_order, max_order + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, max_order)
                batch_count += 1
                
                print(f"    🔄 Batch {batch_count}: Orders {batch_start}-{batch_end}")
                
                # Process this batch of orders
                cursor.execute("""
                    INSERT INTO product_associations (product_a_id, product_b_id, frequency_count, last_calculated)
                    SELECT 
                        product_a_id,
                        product_b_id,
                        SUM(frequency_count) as total_frequency,
                        CURRENT_TIMESTAMP
                    FROM (
                        SELECT 
                            LEAST(oi1.product_id, oi2.product_id) as product_a_id,
                            GREATEST(oi1.product_id, oi2.product_id) as product_b_id,
                            COUNT(*) as frequency_count
                        FROM order_items oi1
                        JOIN order_items oi2 ON oi1.order_id = oi2.order_id 
                            AND oi1.product_id < oi2.product_id
                        WHERE oi1.order_id BETWEEN %s AND %s
                        GROUP BY 
                            LEAST(oi1.product_id, oi2.product_id),
                            GREATEST(oi1.product_id, oi2.product_id)
                    ) batch_associations
                    GROUP BY product_a_id, product_b_id
                    HAVING SUM(frequency_count) >= 1
                    ON CONFLICT (product_a_id, product_b_id) 
                    DO UPDATE SET 
                        frequency_count = product_associations.frequency_count + EXCLUDED.frequency_count,
                        last_calculated = EXCLUDED.last_calculated
                """, (batch_start, batch_end))
                
                batch_associations = cursor.rowcount
                total_associations += batch_associations
                
                # Commit after each batch to avoid long transactions
                self.connection.commit()
                
                print(f"      ✅ +{batch_associations:,} associations")
            
            # Clean up associations with frequency < 2
            print(f"  🧹 Cleaning up low-frequency associations...")
            cursor.execute("DELETE FROM product_associations WHERE frequency_count < 2")
            removed_count = cursor.rowcount
            self.connection.commit()
            
            cursor.close()
            
            duration = time.time() - start_time
            final_count = total_associations - removed_count
            print(f"  ✅ Created {final_count:,} associations ({removed_count:,} low-frequency removed)")
            print(f"  ⏱️  Total time: {duration:.1f}s ({batch_count} batches)")
            
            return True, duration
            
        except Exception as e:
            print(f"  ❌ Error in incremental associations: {e}")
            if self.connection:
                self.connection.rollback()
            return False, 0

    def run_performance_queries(self):
        """Run the performance queries script and capture detailed metrics"""
        print(f"\n🔍 Running performance queries...")

        try:
            cmd = [sys.executable, str(self.queries_script)]
            start_time = time.time()

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            end_time = time.time()

            duration = end_time - start_time

            if result.returncode == 0:
                print(f"  ✅ Performance queries completed in {duration:.1f}s")
                
                # Parse the output to extract query performance data
                query_results = self.parse_query_output(result.stdout)
                query_results['total_script_duration'] = duration * 1000  # Convert to ms
                
                # Show summary
                if query_results['total_queries'] > 0:
                    print(f"    📊 Executed {query_results['total_queries']} queries")
                    print(f"    ✅ {query_results['successful_queries']} successful")
                    print(f"    ⏱️ Average response: {query_results.get('average_response_time', 0):.1f}ms")
                
                return True, duration, query_results
            else:
                print(f"  ❌ Performance queries failed:")
                print(f"  Error: {result.stderr}")
                return False, duration, None

        except subprocess.TimeoutExpired:
            print(f"  ⏰ Performance queries timed out after 30 minutes")
            return False, 0, None
        except Exception as e:
            print(f"  ❌ Performance queries error: {e}")
            return False, 0, None

    def parse_query_output(self, output):
        """Parse query output to extract performance metrics"""
        query_results = {
            'total_queries': 0,
            'successful_queries': 0,
            'total_execution_time': 0,
            'queries': [],
            'execution_order': []
        }
        
        # Split by lines and process
        lines = output.strip().split('\n')
        current_query = None
        
        for line in lines:
            line = line.strip()
            
            # Look for query start patterns - [X/Y] Processing: query_name
            if '] Processing:' in line and '[' in line:
                # Extract query name - pattern: [X/Y] Processing: query_name
                parts = line.split('] Processing: ')
                if len(parts) == 2:
                    query_name = parts[1].strip()
                    current_query = {
                        'name': query_name,
                        'success': False,
                        'execution_time_ms': 0,
                        'rows_returned': 0,
                        'columns_returned': 0
                    }
                    query_results['total_queries'] += 1
                    query_results['execution_order'].append(query_name)
            
            # Look for response time - ⏱️  Response time: X.XXms
            elif current_query and 'Response time:' in line:
                # Extract time in ms - pattern: Response time: X.XXms
                time_match = re.search(r'Response time:\s*(\d+\.?\d*)ms', line)
                if time_match:
                    current_query['execution_time_ms'] = float(time_match.group(1))
                    query_results['total_execution_time'] += current_query['execution_time_ms']
            
            # Look for rows returned - 📊 Rows returned: X,XXX
            elif current_query and 'Rows returned:' in line:
                # Extract row count - pattern: Rows returned: X,XXX
                rows_match = re.search(r'Rows returned:\s*([\d,]+)', line)
                if rows_match:
                    # Remove commas and convert to int
                    rows_str = rows_match.group(1).replace(',', '')
                    current_query['rows_returned'] = int(rows_str)
            
            # Look for query completion - ✅ Query completed successfully
            elif current_query and 'Query completed successfully' in line:
                current_query['success'] = True
                query_results['successful_queries'] += 1
                query_results['queries'].append(current_query)
                current_query = None
        
        # Calculate average response time
        if query_results['successful_queries'] > 0:
            query_results['average_response_time'] = query_results['total_execution_time'] / query_results['successful_queries']
        else:
            query_results['average_response_time'] = 0
        
        print(f"   📊 Parsed {query_results['total_queries']} queries from output:")
        for query in query_results['queries']:
            print(f"      • {query['name']}: {query['execution_time_ms']}ms, {query['rows_returned']:,} rows")
            
        return query_results

    def create_analytics_run_entry(self, iteration_data, query_results):
        """Create an entry in the Analytics_Runs table and individual query results"""
        if not query_results:
            return None
            
        try:
            cursor = self.connection.cursor()
            
            # Calculate metrics
            success_rate = (query_results['successful_queries'] / max(query_results['total_queries'], 1)) * 100
            avg_response_time = query_results['total_execution_time'] / max(query_results['successful_queries'], 1)
            
            # Prepare execution order
            execution_order = query_results.get('execution_order', [q['name'] for q in query_results['queries']])
            
            insert_query = """
                INSERT INTO Analytics_Runs (
                    export_timestamp, database_host, database_name, total_queries_executed,
                    successful_queries, execution_order, script_version, description,
                    total_execution_time_ms, average_response_time_ms, success_rate_percent,
                    analytics_json
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING run_id
            """
            
            description = f'Progressive performance test iteration {iteration_data["iteration"]} - Session {self.test_session_id}'
            analytics_json = json.dumps({
                'test_session_id': self.test_session_id,
                'iteration': iteration_data['iteration'],
                'database_state': iteration_data['final_counts'],
                'query_details': query_results['queries']
            })
            
            values = (
                datetime.fromisoformat(iteration_data['timestamp']),
                self.db_config['host'],
                self.db_config['database'],
                query_results['total_queries'],
                query_results['successful_queries'],
                json.dumps(execution_order),
                'progressive_test_v1.0',
                description,
                query_results['total_execution_time'],
                avg_response_time,
                success_rate,
                analytics_json
            )
            
            cursor.execute(insert_query, values)
            analytics_run_id = cursor.fetchone()[0]
            self.connection.commit()
            cursor.close()
            
            print(f"  💾 Analytics run stored (run_id: {analytics_run_id})")
            
            # Store individual query results
            self.store_query_results(analytics_run_id, iteration_data, query_results)
            
            return analytics_run_id
            
        except psycopg2.Error as e:
            print(f"  ⚠️ Could not store analytics run: {e}")
            if self.connection:
                self.connection.rollback()
            return None

    def store_query_results(self, analytics_run_id, iteration_data, query_results):
        """Store individual query results in Analytics_Query_Results table"""
        if not query_results.get('queries'):
            return
            
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
                INSERT INTO Analytics_Query_Results (
                    run_id, query_name, query_description, dataset_reference,
                    sql_query, affected_tables, execution_timestamp, execution_order,
                    response_time_ms, response_time_seconds, rows_returned, columns_returned,
                    column_names, sample_data, data_types, has_data, first_row, total_data_points
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            for i, query in enumerate(query_results['queries'], 1):
                # Extract query details with safe defaults
                query_name = query.get('name', f'unknown_query_{i}')
                execution_time_ms = query.get('execution_time_ms', 0)
                rows_returned = query.get('rows_returned', 0)
                columns_returned = query.get('columns_returned', 0)
                has_data = rows_returned > 0
                
                values = (
                    analytics_run_id,
                    query_name,
                    f"Query from progressive test iteration {iteration_data['iteration']}",
                    f"iteration_{iteration_data['iteration']}",
                    "-- SQL query stored separately --",  # We don't have the actual SQL here
                    json.dumps([]),  # affected_tables - would need to be extracted from perform_queries.py output
                    datetime.fromisoformat(iteration_data['timestamp']),
                    i,
                    execution_time_ms,
                    execution_time_ms / 1000.0,
                    rows_returned,
                    columns_returned,
                    json.dumps([]),  # column_names - would need from perform_queries.py
                    json.dumps([]),  # sample_data - would need from perform_queries.py  
                    json.dumps([]),  # data_types - would need from perform_queries.py
                    has_data,
                    json.dumps([]),  # first_row - would need from perform_queries.py
                    rows_returned
                )
                
                cursor.execute(insert_query, values)
            
            self.connection.commit()
            cursor.close()
            print(f"  💾 {len(query_results['queries'])} query results stored in Analytics_Query_Results")
            
        except psycopg2.Error as e:
            print(f"  ⚠️ Could not store query results: {e}")
            if self.connection:
                self.connection.rollback()

    def store_iteration_results(self, iteration_data, analytics_run_id=None):
        """Store iteration results in database"""
        try:
            cursor = self.connection.cursor()

            configuration_used = {
                'test_type': 'progressive_performance_test',
                'test_session_id': self.test_session_id,
                'iteration_number': iteration_data['iteration'],
                'data_targets': iteration_data['targets'],
                'rows_added_this_iteration': iteration_data['rows_added'],
                'growth_multipliers': self.growth_multipliers,
                'max_orders_limit': self.max_orders,
                'database_state_before': iteration_data['initial_counts'],
                'database_state_after': iteration_data['final_counts'],
                'analytics_run_id': analytics_run_id
            }

            insert_query = """
                INSERT INTO Test_Data_Execution_Log (
                    execution_timestamp, script_name, script_version, execution_type,
                    database_host, database_name, total_operations, successful_operations,
                    failed_operations, total_execution_time_ms, records_created,
                    tables_affected, execution_status, configuration_used
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING execution_id
            """

            total_records_added = sum(iteration_data['rows_added'].values())
            tables_affected = [table for table, count in iteration_data['rows_added'].items() if count > 0]
            
            # Count total operations: data generation + associations + queries
            total_operations = len(tables_affected) + 1 + (iteration_data.get('query_results', {}).get('total_queries', 0))
            
            # Count successful operations
            successful_ops = 0
            if iteration_data['data_generation_successful']:
                successful_ops += len(tables_affected)
            if iteration_data.get('associations_successful', False):
                successful_ops += 1
            if iteration_data['queries_successful']:
                successful_ops += iteration_data.get('query_results', {}).get('successful_queries', 0)
            
            failed_ops = total_operations - successful_ops

            values = (
                datetime.fromisoformat(iteration_data['timestamp']),
                'testdata_loop.py',
                '2.0',
                f'progressive_test_iteration_{iteration_data["iteration"]}',
                self.db_config['host'],
                self.db_config['database'],
                total_operations,
                successful_ops,
                failed_ops,
                iteration_data['total_duration'] * 1000,
                total_records_added,
                json.dumps(tables_affected),
                'success' if iteration_data['data_generation_successful'] and iteration_data['queries_successful'] else 'partial_success',
                json.dumps(configuration_used)
            )

            cursor.execute(insert_query, values)
            execution_id = cursor.fetchone()[0]
            self.connection.commit()
            cursor.close()

            print(f"  💾 Iteration data stored in database (execution_id: {execution_id})")
            return execution_id

        except psycopg2.Error as e:
            print(f"  ⚠️ Could not store iteration data in database: {e}")
            if self.connection:
                self.connection.rollback()
            return None

    def save_iteration_results(self, iteration_data):
        """Save results for this iteration"""
        self.test_results.append(iteration_data)

        results_file = self.script_dir / f"progressive_test_results_{self.test_session_id}.json"

        try:
            with open(results_file, 'w') as f:
                json.dump(self.test_results, f, indent=2, default=str)
            print(f"  💾 Results saved to: {results_file}")
        except Exception as e:
            print(f"  ⚠️ Could not save results: {e}")

    def print_iteration_summary(self, iteration_data):
        """Print summary for current iteration"""
        print(f"\n" + "="*80)
        print(f"📋 ITERATION {iteration_data['iteration']} SUMMARY")
        print(f"="*80)

        counts = iteration_data['final_counts']
        targets = iteration_data['targets']
        print(f"📊 Final database state vs targets:")
        for table, count in counts.items():
            target = targets.get(table, 0)
            percentage = (count / target * 100) if target > 0 else 0
            print(f"  {table:15}: {count:>8,} / {target:>8,} ({percentage:5.1f}%)")

        print(f"\n⏱️ Timings:")
        print(f"  Data generation: {iteration_data['data_generation_duration']:.1f}s")
        print(f"  Associations update: {iteration_data.get('associations_duration', 0):.1f}s")
        print(f"  Query execution: {iteration_data['query_duration']:.1f}s")
        print(f"  Total iteration: {iteration_data['total_duration']:.1f}s")

        # Show detailed query results if available
        if 'query_results' in iteration_data and iteration_data['query_results']:
            qr = iteration_data['query_results']
            print(f"\n📊 Query Performance:")
            print(f"  Total queries: {qr.get('total_queries', 0)}")
            print(f"  Successful: {qr.get('successful_queries', 0)}")
            print(f"  Success rate: {((qr.get('successful_queries', 0) / max(qr.get('total_queries', 1), 1)) * 100):.1f}%")
            print(f"  Average response: {qr.get('average_response_time', 0):.1f}ms")

        success_indicators = []
        if iteration_data['data_generation_successful']:
            success_indicators.append("data generation")
        if iteration_data.get('associations_successful', False):
            success_indicators.append("associations update")
        if iteration_data['queries_successful']:
            success_indicators.append("performance queries")

        if len(success_indicators) == 3:
            print(f"✅ Iteration completed successfully (all steps completed)")
        else:
            print(f"⚠️ Iteration completed with issues: {', '.join(success_indicators)} successful")
            
        # Performance info for next iteration
        next_order_items = int(targets.get('order_items', 0) * self.growth_multipliers.get('order_items', 1))
        if next_order_items > 200000:
            print(f"\n📈 Next iteration will have {next_order_items:,} order_items")
            print(f"   Will use incremental batch processing for optimal performance")

    def check_continue_condition(self, current_counts):
        """Check if we should continue with next iteration"""
        current_orders = current_counts.get('orders', 0)
        next_target_orders = int(self.current_data['orders'] * self.growth_multipliers['orders'])

        print(f"\n🎯 Continue condition check:")
        print(f"  Current orders: {current_orders:,}")
        print(f"  Next target orders: {next_target_orders:,}")
        print(f"  Limit: {self.max_orders:,}")

        should_continue = next_target_orders <= self.max_orders

        if should_continue:
            print(f"  ✅ Continue: Next target ({next_target_orders:,}) ≤ limit ({self.max_orders:,})")
        else:
            print(f"  🛑 Stop: Next target ({next_target_orders:,}) > limit ({self.max_orders:,})")

        return should_continue

    def print_final_summary(self):
        """Print final test summary"""
        end_time = datetime.now()
        total_duration = end_time - self.start_time

        print(f"\n" + "="*80)
        print(f"🎉 PROGRESSIVE PERFORMANCE TEST COMPLETE")
        print(f"="*80)
        print(f"📅 Start time: {self.start_time}")
        print(f"📅 End time: {end_time}")
        print(f"⏱️ Total duration: {total_duration}")
        print(f"📊 Iterations completed: {self.current_iteration}")
        print(f"🎯 Final orders count: {self.get_current_row_counts().get('orders', 0):,}")

    def run_test_loop(self, max_iterations=None):
        """Run the progressive performance test loop"""
        if not self.connect():
            return

        try:
            iteration_count = 0
            while True:
                self.current_iteration += 1
                iteration_count += 1
                iteration_start_time = datetime.now()

                print(f"\n🔄 Starting iteration {self.current_iteration}...")
                
                # Check max iterations limit
                if max_iterations and iteration_count > max_iterations:
                    print(f"\n🏁 Reached maximum iterations limit: {max_iterations}")
                    break

                # Get current state
                initial_counts = self.get_current_row_counts()
                print(f"📊 Current database state:")
                for table, count in initial_counts.items():
                    print(f"  {table:20}: {count:,} rows")

                # Step 1: Calculate and generate new data
                rows_to_add = self.calculate_rows_to_add(initial_counts)
                data_generation_success = self.run_data_generation(rows_to_add)
                
                # Step 2: Update product associations (critical for recommendation queries)
                associations_success = False
                associations_duration = 0
                if data_generation_success:
                    print(f"\n🔗 Updating product associations (ALWAYS REQUIRED for recommendations)...")
                    associations_success, associations_duration = self.update_product_associations()

                # Step 3: Run performance queries
                queries_success, query_duration, query_results = self.run_performance_queries()

                # Get final state
                final_counts = self.get_current_row_counts()

                # Store results with all timing information
                iteration_data = {
                    'iteration': self.current_iteration,
                    'timestamp': datetime.now().isoformat(),
                    'initial_counts': initial_counts,
                    'final_counts': final_counts,
                    'targets': self.current_data.copy(),
                    'rows_added': rows_to_add,
                    'data_generation_successful': data_generation_success,
                    'associations_successful': associations_success,
                    'queries_successful': queries_success,
                    'data_generation_duration': 0,  # This could be enhanced to capture actual timing
                    'associations_duration': associations_duration,
                    'query_duration': query_duration,
                    'total_duration': (datetime.now() - iteration_start_time).total_seconds(),
                    'query_results': query_results
                }

                # Create analytics run entry
                analytics_run_id = None
                if query_results:
                    analytics_run_id = self.create_analytics_run_entry(iteration_data, query_results)

                # Store iteration data in execution log
                self.store_iteration_results(iteration_data, analytics_run_id)

                # Save to file
                self.save_iteration_results(iteration_data)

                # Print summary
                self.print_iteration_summary(iteration_data)

                # Check if we should continue
                if not self.check_continue_condition(final_counts):
                    break

                # Update targets for next iteration
                self.update_data_targets_for_next_iteration()

        finally:
            self.disconnect()
            self.print_final_summary()

def load_environment():
    """Load database configuration from environment"""
    return {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': config('DB_PORT', default=5433, cast=int),
        'connect_timeout': config('DB_CONNECT_TIMEOUT', default=10, cast=int)
    }

def main():
    """Main execution function with configuration support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Progressive database performance testing')
    parser.add_argument('--config', choices=['small', 'medium', 'large', 'stress'], 
                       default='medium', help='Performance configuration preset')
    parser.add_argument('--skip-setup', action='store_true', 
                       help='Skip setup tests and start immediately')
    parser.add_argument('--max-iterations', type=int, default=None,
                       help='Maximum number of iterations to run')
    
    args = parser.parse_args()
    
    print(f"🚀 Progressive Database Performance Testing")
    print(f"=" * 60)
    
    db_config = load_environment()
    tester = ProgressivePerformanceTester(db_config)
    
    # Apply configuration
    try:
        from performance_config import apply_config
        if not apply_config(tester, args.config):
            print("❌ Failed to apply configuration")
            return
    except ImportError:
        print("⚠️  performance_config.py not found, using default settings")
        print(f"   Using '{args.config}' configuration (manual fallback)")
        
        # Manual fallback configurations
        config_map = {
            'small': {'max_orders': 100000},
            'medium': {'max_orders': 500000},
            'large': {'max_orders': 2000000},
            'stress': {'max_orders': 10000000}
        }
        
        if args.config in config_map:
            cfg = config_map[args.config]
            tester.max_orders = cfg['max_orders']
    
    print(f"🎛️  Configuration: {args.config}")
    print(f"📊 Initial data: {tester.initial_data}")
    print(f"🎯 Max orders target: {tester.max_orders:,}")
    print(f"🔗 Associations are ALWAYS computed using intelligent algorithms")
    
    if args.max_iterations:
        print(f"🔢 Max iterations: {args.max_iterations}")
    
    # Run setup tests unless skipped
    if not args.skip_setup:
        if not tester.test_setup():
            print("\n❌ Setup tests failed. Use --skip-setup to bypass.")
            return
    else:
        print("⏭️  Skipping setup tests")
    
    # Run the main test loop
    try:
        tester.run_test_loop(max_iterations=args.max_iterations)
        print("\n🎉 Performance testing completed!")
    except KeyboardInterrupt:
        print("\n⏹️  Testing interrupted by user")
    except Exception as e:
        print(f"\n❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
