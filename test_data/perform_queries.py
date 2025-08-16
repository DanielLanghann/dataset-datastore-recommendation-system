#!/usr/bin/env python3
"""
main_analytics.py
Flexible Product Analytics Query Performance Script
Imports queries from separate module and executes them in a configurable loop
Now with database storage capability
"""

import psycopg2
import time
import json
import re
from datetime import datetime, date
from decimal import Decimal
from decouple import config
from tabulate import tabulate

# Import our separate queries module
from queries import BUSINESS_QUERIES, QUERY_CONFIG, get_query_list, get_all_queries


class FlexibleProductAnalytics:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.results = {}
        self.execution_order = []
        self.analytics_run_id = None
        self.execution_start_time = datetime.now()
    
    def json_serializer(self, obj):
        """Custom JSON serializer for PostgreSQL data types"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):  # Handle other datetime-like objects
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")
    
    def safe_json_dumps(self, obj):
        """Safely serialize objects to JSON with custom serializer"""
        return json.dumps(obj, default=self.json_serializer)
    
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
    
    def extract_tables_from_query(self, query_sql):
        """Extract table names from SQL query using regex"""
        # Remove comments and normalize whitespace
        clean_query = re.sub(r'--.*?\n', '\n', query_sql)
        clean_query = re.sub(r'/\*.*?\*/', '', clean_query, flags=re.DOTALL)
        clean_query = re.sub(r'\s+', ' ', clean_query).upper()
        
        # Find table names after FROM and JOIN keywords
        table_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)'
        matches = re.findall(table_pattern, clean_query)
        
        # Remove duplicates and sort
        tables = sorted(list(set(matches)))
        return tables
    
    def create_analytics_run(self, query_names=None):
        """Create a new analytics run record and return the run_id"""
        try:
            cursor = self.connection.cursor()
            
            # Determine which queries will be executed
            if query_names is None:
                execution_order = list(get_all_queries().keys())
                total_queries = len(execution_order)
                description = f"Complete analytics run with all {total_queries} available queries"
            else:
                execution_order = query_names
                total_queries = len(query_names)
                description = f"Selective analytics run with {total_queries} specified queries: {', '.join(query_names)}"
            
            # Insert analytics run record
            insert_query = """
                INSERT INTO Analytics_Runs (
                    export_timestamp, database_host, database_name, 
                    total_queries_executed, successful_queries, execution_order,
                    script_version, description, display_limit, sample_data_limit,
                    export_filename_template, timestamp_format
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING run_id
            """
            
            values = (
                self.execution_start_time,
                self.db_config.get('host', 'unknown'),
                self.db_config.get('database', 'unknown'),
                total_queries,
                0,  # Will be updated later
                json.dumps(execution_order),
                "2.0_flexible_db",
                description,
                QUERY_CONFIG.get('display_limit', 5),
                QUERY_CONFIG.get('sample_data_limit', 3),
                QUERY_CONFIG.get('export_filename_template', 'analytics_results_{timestamp}.json'),
                QUERY_CONFIG.get('timestamp_format', '%Y%m%d_%H%M%S')
            )
            
            cursor.execute(insert_query, values)
            self.analytics_run_id = cursor.fetchone()[0]
            self.connection.commit()
            cursor.close()
            
            print(f"📋 Created analytics run #{self.analytics_run_id}: {description}")
            return self.analytics_run_id
            
        except psycopg2.Error as e:
            print(f"❌ Failed to create analytics run record: {e}")
            if self.connection:
                self.connection.rollback()
            return None
    
    def store_query_result(self, query_name, query_data, result_data):
        """Store individual query result in Analytics_Query_Results table"""
        if not self.analytics_run_id:
            print(f"⚠️  No analytics run ID available, skipping storage for {query_name}")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare data for insertion
            query_info = result_data['query_info']
            performance_metrics = result_data['performance_metrics']
            data_structure = result_data['data_structure']
            results_summary = result_data['results_summary']
            
            # Handle error cases
            error_occurred = 'error' in result_data and result_data['error']['occurred']
            
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
            
            values = (
                self.analytics_run_id,
                query_info['name'],
                query_info['description'],
                query_info['dataset_reference'],
                query_info['sql'],
                self.safe_json_dumps(query_info['affected_tables']),
                datetime.fromisoformat(query_info['execution_timestamp'].replace('Z', '+00:00')) if 'T' in query_info['execution_timestamp'] else datetime.now(),
                query_info['execution_order'],
                performance_metrics['response_time_ms'] if not error_occurred else 0,
                performance_metrics['response_time_seconds'] if not error_occurred else 0,
                performance_metrics['rows_returned'] if not error_occurred else 0,
                performance_metrics['columns_returned'] if not error_occurred else 0,
                self.safe_json_dumps(data_structure['column_names']) if not error_occurred else json.dumps([]),
                self.safe_json_dumps(data_structure['sample_data']) if not error_occurred else json.dumps([]),
                self.safe_json_dumps(data_structure['data_types']) if not error_occurred else json.dumps([]),
                results_summary['has_data'] if not error_occurred else False,
                self.safe_json_dumps(results_summary['first_row']) if not error_occurred and results_summary['first_row'] else None,
                results_summary['total_data_points'] if not error_occurred else 0
            )
            
            cursor.execute(insert_query, values)
            self.connection.commit()
            cursor.close()
            
            print(f"   💾 Stored query result for {query_name} in database")
            return True
            
        except psycopg2.Error as e:
            print(f"   ❌ Failed to store query result for {query_name}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        except Exception as e:
            print(f"   ❌ Unexpected error storing {query_name}: {e}")
            return False
    
    def update_analytics_run_summary(self):
        """Update the analytics run with final summary information"""
        if not self.analytics_run_id:
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Calculate summary statistics
            successful_queries = len([k for k, v in self.results.items() 
                                    if not ('error' in v and v['error']['occurred'])])
            
            total_execution_time_ms = sum([
                v['performance_metrics']['response_time_ms'] 
                for v in self.results.values() 
                if 'performance_metrics' in v and 'error' not in v
            ])
            
            total_rows_queried = sum([
                v['performance_metrics']['rows_returned'] 
                for v in self.results.values() 
                if 'performance_metrics' in v and 'error' not in v
            ])
            
            average_response_time_ms = total_execution_time_ms / max(successful_queries, 1)
            success_rate_percent = (successful_queries / len(self.results) * 100) if self.results else 0
            
            # Create complete analytics JSON for backup
            analytics_json = {
                'export_metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'database_host': self.db_config.get('host', 'unknown'),
                    'database_name': self.db_config.get('database', 'unknown'),
                    'total_queries_executed': len(self.results),
                    'successful_queries': successful_queries,
                    'execution_order': self.execution_order,
                    'script_version': "2.0_flexible_db",
                    'description': "Flexible business analytics queries with database storage"
                },
                'configuration': QUERY_CONFIG,
                'performance_summary': {
                    'total_execution_time_ms': total_execution_time_ms,
                    'total_rows_queried': total_rows_queried,
                    'average_response_time_ms': average_response_time_ms,
                    'success_rate_percent': success_rate_percent
                },
                'query_results': self.results
            }
            
            # Update analytics run record
            update_query = """
                UPDATE Analytics_Runs SET
                    successful_queries = %s,
                    total_execution_time_ms = %s,
                    total_rows_queried = %s,
                    average_response_time_ms = %s,
                    success_rate_percent = %s,
                    analytics_json = %s
                WHERE run_id = %s
            """
            
            values = (
                successful_queries,
                total_execution_time_ms,
                total_rows_queried,
                average_response_time_ms,
                success_rate_percent,
                self.safe_json_dumps(analytics_json),
                self.analytics_run_id
            )
            
            cursor.execute(update_query, values)
            self.connection.commit()
            cursor.close()
            
            print(f"📊 Updated analytics run #{self.analytics_run_id} with final summary")
            print(f"   ✅ Successful queries: {successful_queries}/{len(self.results)}")
            print(f"   ⏱️  Total execution time: {total_execution_time_ms:.2f}ms")
            print(f"   📈 Success rate: {success_rate_percent:.1f}%")
            
            return True
            
        except psycopg2.Error as e:
            print(f"❌ Failed to update analytics run summary: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def execute_query(self, query_name, query_data):
        """Execute a single query and measure detailed performance"""
        print(f"\n🔍 Executing: {query_name}")
        print(f"   Description: {query_data['description']}")
        print(f"   Dataset Reference: {query_data['dataset_reference']}")
        
        try:
            cursor = self.connection.cursor()
            
            # Extract tables from query
            affected_tables = self.extract_tables_from_query(query_data['sql'])
            
            # Measure execution time in milliseconds
            start_time = time.time()
            cursor.execute(query_data['sql'])
            results = cursor.fetchall()
            end_time = time.time()
            
            execution_time_ms = (end_time - start_time) * 1000
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            
            # Store comprehensive results
            result_data = {
                'query_info': {
                    'name': query_name,
                    'description': query_data['description'],
                    'dataset_reference': query_data['dataset_reference'],
                    'sql': query_data['sql'].strip(),
                    'affected_tables': affected_tables,
                    'execution_timestamp': datetime.now().isoformat(),
                    'execution_order': len(self.execution_order) + 1
                },
                'performance_metrics': {
                    'response_time_ms': round(execution_time_ms, 2),
                    'response_time_seconds': round(execution_time_ms / 1000, 4),
                    'rows_returned': len(results),
                    'columns_returned': len(column_names)
                },
                'data_structure': {
                    'column_names': column_names,
                    'sample_data': results[:QUERY_CONFIG['sample_data_limit']] if results else [],
                    'data_types': [str(type(col).__name__) if results and col is not None else 'NoneType' 
                                 for col in (results[0] if results else [])]
                },
                'results_summary': {
                    'has_data': len(results) > 0,
                    'first_row': list(results[0]) if results else None,
                    'total_data_points': len(results) * len(column_names) if results else 0
                }
            }
            
            # Convert results to JSON-serializable format immediately
            if results:
                # Convert each row to handle Decimal and datetime objects
                serializable_results = []
                for row in results[:QUERY_CONFIG['sample_data_limit']]:
                    serializable_row = []
                    for item in row:
                        if isinstance(item, Decimal):
                            serializable_row.append(float(item))
                        elif isinstance(item, (datetime, date)):
                            serializable_row.append(item.isoformat())
                        else:
                            serializable_row.append(item)
                    serializable_results.append(tuple(serializable_row))
                
                result_data['data_structure']['sample_data'] = serializable_results
                
                # Convert first row as well
                if result_data['results_summary']['first_row']:
                    first_row_serializable = []
                    for item in result_data['results_summary']['first_row']:
                        if isinstance(item, Decimal):
                            first_row_serializable.append(float(item))
                        elif isinstance(item, (datetime, date)):
                            first_row_serializable.append(item.isoformat())
                        else:
                            first_row_serializable.append(item)
                    result_data['results_summary']['first_row'] = first_row_serializable
            
            self.results[query_name] = result_data
            
            # Track execution order
            self.execution_order.append(query_name)
            
            # Store in database
            self.store_query_result(query_name, query_data, result_data)
            
            print(f"   ⏱️  Response time: {execution_time_ms:.2f}ms")
            print(f"   📊 Rows returned: {len(results):,}")
            print(f"   🗂️  Tables queried: {', '.join(affected_tables)}")
            
            return True
            
        except psycopg2.Error as e:
            print(f"   ❌ Query failed: {e}")
            
            # Store error information
            error_data = {
                'query_info': {
                    'name': query_name,
                    'description': query_data['description'],
                    'dataset_reference': query_data['dataset_reference'],
                    'sql': query_data['sql'].strip(),
                    'affected_tables': self.extract_tables_from_query(query_data['sql']),
                    'execution_timestamp': datetime.now().isoformat(),
                    'execution_order': len(self.execution_order) + 1
                },
                'performance_metrics': {
                    'response_time_ms': 0,
                    'response_time_seconds': 0,
                    'rows_returned': 0,
                    'columns_returned': 0
                },
                'data_structure': {
                    'column_names': [],
                    'sample_data': [],
                    'data_types': []
                },
                'results_summary': {
                    'has_data': False,
                    'first_row': None,
                    'total_data_points': 0
                },
                'error': {
                    'occurred': True,
                    'message': str(e),
                    'error_type': type(e).__name__
                }
            }
            
            self.results[query_name] = error_data
            self.execution_order.append(query_name)
            
            # Still try to store error information
            self.store_query_result(query_name, query_data, error_data)
            
            return False
    
    def execute_queries_in_loop(self, query_names=None, skip_on_error=False):
        """
        Execute queries in a flexible loop with database storage
        
        Args:
            query_names (list): Specific queries to run. If None, runs all queries
            skip_on_error (bool): Whether to continue if a query fails
        """
        print("🚀 Starting Flexible Query Execution Loop with Database Storage\n")
        print("=" * 70)
        
        # Create analytics run record
        if not self.create_analytics_run(query_names):
            print("❌ Failed to create analytics run record")
            return False
        
        # Determine which queries to run
        if query_names is None:
            queries_to_run = get_all_queries()
            print(f"📋 Executing ALL {len(queries_to_run)} available queries")
        else:
            # Validate query names
            available_queries = get_query_list()
            invalid_queries = [q for q in query_names if q not in available_queries]
            
            if invalid_queries:
                print(f"❌ Invalid query names: {invalid_queries}")
                print(f"✅ Available queries: {available_queries}")
                return False
            
            queries_to_run = {name: BUSINESS_QUERIES[name] for name in query_names}
            print(f"📋 Executing {len(queries_to_run)} selected queries: {list(queries_to_run.keys())}")
        
        print(f"⚙️  Skip on error: {'Yes' if skip_on_error else 'No'}")
        print(f"💾 Database storage: Analytics Run #{self.analytics_run_id}")
        print("=" * 70)
        
        # Execute queries in loop
        successful_queries = 0
        failed_queries = 0
        
        for i, (query_name, query_data) in enumerate(queries_to_run.items(), 1):
            print(f"\n[{i}/{len(queries_to_run)}] Processing: {query_name}")
            
            success = self.execute_query(query_name, query_data)
            
            if success:
                successful_queries += 1
                print(f"   ✅ Query completed successfully")
            else:
                failed_queries += 1
                print(f"   ❌ Query failed")
                
                if not skip_on_error:
                    print(f"   🛑 Stopping execution due to error (skip_on_error=False)")
                    break
                else:
                    print(f"   ⏭️  Continuing to next query (skip_on_error=True)")
        
        # Update analytics run with final summary
        self.update_analytics_run_summary()
        
        # Execution summary
        print("\n" + "=" * 70)
        print("📊 EXECUTION LOOP SUMMARY")
        print("=" * 70)
        print(f"✅ Successful queries: {successful_queries}")
        print(f"❌ Failed queries: {failed_queries}")
        print(f"📋 Total attempted: {len(self.execution_order)}")
        print(f"🔄 Execution order: {' → '.join(self.execution_order)}")
        print(f"💾 Results stored in Analytics Run #{self.analytics_run_id}")
        
        return successful_queries > 0
    
    def display_results(self, query_name=None, limit=None):
        """Display query results in a formatted table"""
        if limit is None:
            limit = QUERY_CONFIG['display_limit']
        
        # Display specific query or all queries
        queries_to_display = [query_name] if query_name else list(self.results.keys())
        
        for qname in queries_to_display:
            if qname not in self.results:
                print(f"❌ No results found for query: {qname}")
                continue
            
            result_data = self.results[qname]
            
            # Check for errors
            if 'error' in result_data and result_data['error']['occurred']:
                print(f"\n❌ Query {qname} failed: {result_data['error']['message']}")
                continue
            
            # Get sample data
            sample_data = result_data['data_structure']['sample_data']
            columns = result_data['data_structure']['column_names']
            total_rows = result_data['performance_metrics']['rows_returned']
            
            print(f"\n📊 Results for {qname}:")
            print(f"   Execution order: #{result_data['query_info']['execution_order']}")
            print(f"   Total rows: {total_rows:,}")
            
            if not sample_data:
                print("   No data found")
                continue
            
            # Show sample results
            display_limit = min(limit, len(sample_data))
            print(f"   Showing first {display_limit} rows:")
            
            print(tabulate(sample_data[:display_limit], headers=columns, tablefmt="grid", floatfmt=".2f"))
            
            if total_rows > display_limit:
                print(f"   ... and {total_rows - display_limit} more rows")
    
    def display_performance_summary(self):
        """Display comprehensive performance summary"""
        print("\n" + "=" * 70)
        print("⚡ COMPREHENSIVE PERFORMANCE SUMMARY")
        print("=" * 70)
        
        performance_data = []
        total_time_ms = 0
        total_rows = 0
        
        # Sort by execution order
        sorted_results = sorted(
            self.results.items(),
            key=lambda x: x[1]['query_info']['execution_order']
        )
        
        for query_name, data in sorted_results:
            execution_order = data['query_info']['execution_order']
            
            if 'error' in data and data['error']['occurred']:
                performance_data.append([
                    f"#{execution_order}",
                    query_name.replace('_', ' ').title(),
                    "ERROR",
                    "0",
                    ', '.join(data['query_info']['affected_tables']),
                    data['error']['message'][:30] + "..."
                ])
                continue
            
            metrics = data['performance_metrics']
            response_time_ms = metrics['response_time_ms']
            rows = metrics['rows_returned']
            tables = ', '.join(data['query_info']['affected_tables'])
            
            total_time_ms += response_time_ms
            total_rows += rows
            
            performance_data.append([
                f"#{execution_order}",
                query_name.replace('_', ' ').title(),
                f"{response_time_ms:.2f}ms",
                f"{rows:,}",
                tables,
                "SUCCESS"
            ])
        
        headers = ["Order", "Query Name", "Response Time", "Rows", "Tables", "Status"]
        print(tabulate(performance_data, headers=headers, tablefmt="grid"))
        
        successful_queries = [k for k, v in self.results.items() 
                            if not ('error' in v and v['error']['occurred'])]
        
        if successful_queries:
            print(f"\n📈 Total execution time: {total_time_ms:.2f}ms")
            print(f"📊 Total rows queried: {total_rows:,}")
            print(f"⏱️  Average query time: {total_time_ms/len(successful_queries):.2f}ms")
            print(f"✅ Success rate: {len(successful_queries)}/{len(self.results)} ({len(successful_queries)/len(self.results)*100:.1f}%)")
            
            # Find slowest and fastest queries
            execution_times = [(k, v['performance_metrics']['response_time_ms']) 
                             for k, v in self.results.items() 
                             if k in successful_queries]
            
            if execution_times:
                slowest = max(execution_times, key=lambda x: x[1])
                fastest = min(execution_times, key=lambda x: x[1])
                
                print(f"🐌 Slowest query: {slowest[0]} ({slowest[1]:.2f}ms)")
                print(f"🚀 Fastest query: {fastest[0]} ({fastest[1]:.2f}ms)")
    
    def export_results_to_json(self, filename=None):
        """Export comprehensive results to JSON file with timestamp (backup option)"""
        if filename is None:
            # Generate timestamped filename
            timestamp = datetime.now().strftime(QUERY_CONFIG['timestamp_format'])
            filename = QUERY_CONFIG['export_filename_template'].format(timestamp=timestamp)
        
        # Create comprehensive export structure
        export_data = {
            'export_metadata': {
                'timestamp': datetime.now().isoformat(),
                'database_host': self.db_config.get('host', 'unknown'),
                'database_name': self.db_config.get('database', 'unknown'),
                'total_queries_executed': len(self.results),
                'successful_queries': len([k for k, v in self.results.items() 
                                         if not ('error' in v and v['error']['occurred'])]),
                'execution_order': self.execution_order,
                'script_version': "2.0_flexible_db",
                'description': "Flexible business analytics queries with database storage",
                'analytics_run_id': self.analytics_run_id
            },
            'configuration': QUERY_CONFIG,
            'performance_summary': {
                'total_execution_time_ms': sum([
                    v['performance_metrics']['response_time_ms'] 
                    for v in self.results.values() 
                    if 'performance_metrics' in v and 'error' not in v
                ]),
                'total_rows_queried': sum([
                    v['performance_metrics']['rows_returned'] 
                    for v in self.results.values() 
                    if 'performance_metrics' in v and 'error' not in v
                ]),
                'average_response_time_ms': None,
                'success_rate_percent': None
            },
            'query_results': self.results
        }
        
        # Calculate averages
        successful_count = len([v for v in self.results.values() if 'error' not in v])
        if successful_count > 0:
            export_data['performance_summary']['average_response_time_ms'] = (
                export_data['performance_summary']['total_execution_time_ms'] / successful_count
            )
            export_data['performance_summary']['success_rate_percent'] = (
                successful_count / len(self.results) * 100
            )
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, default=self.json_serializer, ensure_ascii=False)
            print(f"\n📄 Backup results exported to {filename}")
            print(f"   📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   🔗 Analytics Run ID: #{self.analytics_run_id}")
            return filename
        except Exception as e:
            print(f"❌ Failed to export backup results: {e}")
            return None
    
    def get_stored_results_summary(self):
        """Retrieve summary of stored results from database"""
        if not self.analytics_run_id:
            return None
        
        try:
            cursor = self.connection.cursor()
            
            # Get analytics run summary
            cursor.execute("""
                SELECT run_id, export_timestamp, total_queries_executed, 
                       successful_queries, total_execution_time_ms, 
                       total_rows_queried, success_rate_percent
                FROM Analytics_Runs 
                WHERE run_id = %s
            """, (self.analytics_run_id,))
            
            run_summary = cursor.fetchone()
            
            if run_summary:
                print(f"\n💾 DATABASE STORAGE SUMMARY")
                print("=" * 40)
                print(f"Analytics Run ID: #{run_summary[0]}")
                print(f"Execution Time: {run_summary[1]}")
                print(f"Total Queries: {run_summary[2]}")
                print(f"Successful: {run_summary[3]}")
                print(f"Execution Time: {run_summary[4]:.2f}ms")
                print(f"Rows Queried: {run_summary[5]:,}")
                print(f"Success Rate: {run_summary[6]:.1f}%")
                
                # Get query results count
                cursor.execute("""
                    SELECT COUNT(*) FROM Analytics_Query_Results 
                    WHERE run_id = %s
                """, (self.analytics_run_id,))
                
                query_count = cursor.fetchone()[0]
                print(f"Query Records Stored: {query_count}")
                print("=" * 40)
            
            cursor.close()
            return run_summary
            
        except psycopg2.Error as e:
            print(f"❌ Failed to retrieve stored results summary: {e}")
            return None


def load_environment():
    """Load environment variables with fallback defaults"""
    return {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': int(config('DB_PORT', 5433)),
        'connect_timeout': int(config('DB_CONNECT_TIMEOUT', 10))
    }


def main():
    """Main execution function with flexible options and database storage"""
    print("📊 Flexible Product Analytics & Performance Testing with Database Storage")
    print("=" * 80)
    print("🔄 Loop-based execution with separated query definitions")
    print("💾 Results stored in Analytics_Runs and Analytics_Query_Results tables")
    
    # Load database configuration
    db_config = load_environment()
    
    # Initialize analytics
    analytics = FlexibleProductAnalytics(db_config)
    
    if not analytics.connect():
        print("❌ Failed to connect to database. Please check your configuration.")
        return
    
    try:
        # Example execution options:
        
        # Option 1: Execute ALL queries
        success = analytics.execute_queries_in_loop()
        
        # Option 2: Execute specific queries
        # success = analytics.execute_queries_in_loop(
        #     query_names=["favorite_products", "favorite_categories"],
        #     skip_on_error=True
        # )
        
        # Option 3: Execute queries with error handling
        # success = analytics.execute_queries_in_loop(skip_on_error=True)
        
        if success:
            # Display results
            print("\n" + "=" * 70)
            print("📊 QUERY RESULTS PREVIEW")
            print("=" * 70)
            analytics.display_results()
            
            # Performance summary
            analytics.display_performance_summary()
            
            # Show database storage summary
            analytics.get_stored_results_summary()
            
            # Optional: Export backup file
            print(f"\n💾 Results have been stored in database (Analytics Run #{analytics.analytics_run_id})")
            backup_response = input("Create backup JSON file? (y/N): ").strip().lower()
            if backup_response in ['y', 'yes']:
                exported_file = analytics.export_results_to_json()
                if exported_file:
                    print(f"📄 Backup file created: {exported_file}")
            
            print("\n🎉 Flexible analytics completed successfully!")
            print("📋 Features used:")
            print("   • Separated query definitions in queries.py")
            print("   • Loop-based execution with error handling")
            print("   • Database storage in Analytics_Runs and Analytics_Query_Results tables")
            print("   • Configurable execution options")
            print(f"   • Results stored as Analytics Run #{analytics.analytics_run_id}")
            print("   • Optional JSON backup export")
            
            print(f"\n🔍 To view stored results later, query:")
            print(f"   SELECT * FROM Analytics_Runs WHERE run_id = {analytics.analytics_run_id};")
            print(f"   SELECT * FROM Analytics_Query_Results WHERE run_id = {analytics.analytics_run_id};")
            
        else:
            print("\n❌ Analytics execution failed or was incomplete")
            if analytics.analytics_run_id:
                print(f"⚠️  Partial results may be stored in Analytics Run #{analytics.analytics_run_id}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Analytics interrupted by user")
        if analytics.analytics_run_id:
            print(f"⚠️  Partial results stored in Analytics Run #{analytics.analytics_run_id}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        if analytics.analytics_run_id:
            print(f"⚠️  Partial results may be stored in Analytics Run #{analytics.analytics_run_id}")
    finally:
        analytics.disconnect()
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    main()