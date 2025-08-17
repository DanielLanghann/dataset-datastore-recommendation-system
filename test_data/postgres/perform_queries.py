#!/usr/bin/env python3
"""
enhanced_perform_queries.py
Enhanced query performance script with database storage
All results are stored in Analytics_Query_Results table
"""

import psycopg2
import time
import json
import re
from datetime import datetime, date
from decimal import Decimal
from decouple import config
from tabulate import tabulate

# Import our enhanced queries module
from test_data.postgres.queries import (
    get_all_queries, get_query_list, get_query, 
    QUERY_CONFIG
)


class EnhancedAnalytics:
    def __init__(self, postgres_config):
        self.postgres_config = postgres_config
        self.pg_connection = None
        self.results = {}
        self.execution_order = []
        self.analytics_run_id = None
        self.execution_start_time = datetime.now()
        self.total_queries_executed = 0
        self.successful_queries = 0
        self.failed_queries = 0
    
    def json_serializer(self, obj):
        """Custom JSON serializer for PostgreSQL and Neo4j data types"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")
    
    def safe_json_dumps(self, obj):
        """Safely serialize objects to JSON with custom serializer"""
        return json.dumps(obj, default=self.json_serializer)
    
    def connect_databases(self):
        """Connect to PostgreSQL"""
        success = True
        
        # Connect to PostgreSQL
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            success = False
        
        return success
    
    def disconnect_databases(self):
        """Close database connections"""
        if self.pg_connection:
            self.pg_connection.close()
    
    def create_analytics_run(self):
        """Create a new analytics run record and return its ID"""
        try:
            cursor = self.pg_connection.cursor()
            
            cursor.execute("""
                INSERT INTO Analytics_Runs (
                    export_timestamp, database_host, database_name, 
                    script_version, description, display_limit, sample_data_limit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
            """, (
                self.execution_start_time,
                self.postgres_config['host'],
                self.postgres_config['database'],
                '3.0_database_storage',
                'Enhanced analytics with database storage',
                QUERY_CONFIG.get('display_limit', 5),
                QUERY_CONFIG.get('sample_data_limit', 3)
            ))
            
            self.analytics_run_id = cursor.fetchone()[0]
            self.pg_connection.commit()
            cursor.close()
            
            print(f"📊 Created analytics run with ID: {self.analytics_run_id}")
            return True
            
        except psycopg2.Error as e:
            print(f"❌ Failed to create analytics run: {e}")
            self.pg_connection.rollback()
            return False
    
    def update_analytics_run(self):
        """Update the analytics run with final statistics"""
        if not self.analytics_run_id:
            return False
        
        try:
            cursor = self.pg_connection.cursor()
            
            execution_end_time = datetime.now()
            total_execution_time_ms = (execution_end_time - self.execution_start_time).total_seconds() * 1000
            
            # Calculate total rows queried
            total_rows_queried = sum(
                result['performance_metrics']['rows_returned'] 
                for result in self.results.values() 
                if 'error' not in result
            )
            
            # Calculate average response time
            successful_response_times = [
                result['performance_metrics']['response_time_ms']
                for result in self.results.values()
                if 'error' not in result and result['performance_metrics']['response_time_ms'] > 0
            ]
            
            avg_response_time = (
                sum(successful_response_times) / len(successful_response_times)
                if successful_response_times else 0
            )
            
            success_rate = (
                (self.successful_queries / self.total_queries_executed * 100)
                if self.total_queries_executed > 0 else 0
            )
            
            cursor.execute("""
                UPDATE Analytics_Runs SET
                    total_queries_executed = %s,
                    successful_queries = %s,
                    execution_order = %s,
                    total_execution_time_ms = %s,
                    total_rows_queried = %s,
                    average_response_time_ms = %s,
                    success_rate_percent = %s
                WHERE run_id = %s
            """, (
                self.total_queries_executed,
                self.successful_queries,
                self.safe_json_dumps(self.execution_order),
                round(total_execution_time_ms, 2),
                total_rows_queried,
                round(avg_response_time, 2),
                round(success_rate, 2),
                self.analytics_run_id
            ))
            
            self.pg_connection.commit()
            cursor.close()
            
            print(f"✅ Updated analytics run {self.analytics_run_id} with final statistics")
            return True
            
        except psycopg2.Error as e:
            print(f"❌ Failed to update analytics run: {e}")
            self.pg_connection.rollback()
            return False
    
    def store_query_result(self, query_name, result_data):
        """Store individual query result in Analytics_Query_Results table"""
        if not self.analytics_run_id:
            print("⚠️  No analytics run ID available, cannot store query result")
            return False
        
        try:
            cursor = self.pg_connection.cursor()
            
            query_info = result_data['query_info']
            performance_metrics = result_data['performance_metrics']
            data_structure = result_data['data_structure']
            results_summary = result_data['results_summary']
            
            # Handle error cases
            error_occurred = 'error' in result_data
            error_message = result_data.get('error', {}).get('message', None) if error_occurred else None
            
            cursor.execute("""
                INSERT INTO Analytics_Query_Results (
                    run_id, query_name, query_description, dataset_reference,
                    sql_query, affected_tables, execution_timestamp, execution_order,
                    response_time_ms, response_time_seconds, rows_returned, columns_returned,
                    column_names, sample_data, data_types,
                    has_data, first_row, total_data_points
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                self.analytics_run_id,
                query_name,
                query_info['description'],
                query_info['dataset_reference'],
                query_info['sql'],
                self.safe_json_dumps(query_info['affected_tables']),
                datetime.fromisoformat(query_info['execution_timestamp'].replace('Z', '+00:00')),
                query_info['execution_order'],
                performance_metrics['response_time_ms'],
                performance_metrics['response_time_seconds'],
                performance_metrics['rows_returned'],
                performance_metrics['columns_returned'],
                self.safe_json_dumps(data_structure['column_names']),
                self.safe_json_dumps(data_structure['sample_data']),
                self.safe_json_dumps(data_structure['data_types']),
                results_summary['has_data'],
                self.safe_json_dumps(results_summary['first_row']),
                results_summary['total_data_points']
            ))
            
            self.pg_connection.commit()
            cursor.close()
            
            print(f"   💾 Stored query result in database")
            return True
            
        except psycopg2.Error as e:
            print(f"   ❌ Failed to store query result: {e}")
            self.pg_connection.rollback()
            return False
    
    def extract_tables_from_query(self, query_sql):
        """Extract table names from SQL query using regex"""
        clean_query = re.sub(r'--.*?\n', '\n', query_sql)
        clean_query = re.sub(r'/\*.*?\*/', '', clean_query, flags=re.DOTALL)
        clean_query = re.sub(r'\s+', ' ', clean_query).upper()
        
        table_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)'
        matches = re.findall(table_pattern, clean_query)
        
        return sorted(list(set(matches)))
    
    def execute_postgresql_query(self, query_name, query_data):
        """Execute a PostgreSQL query"""
        print(f"🔍 PostgreSQL: {query_name}")
        
        try:
            cursor = self.pg_connection.cursor()
            
            affected_tables = self.extract_tables_from_query(query_data['sql'])
            
            start_time = time.time()
            cursor.execute(query_data['sql'])
            results = cursor.fetchall()
            end_time = time.time()
            
            execution_time_ms = (end_time - start_time) * 1000
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            
            return self._format_query_result(
                query_name, query_data, results, column_names, 
                execution_time_ms, affected_tables, 'postgresql'
            )
            
        except psycopg2.Error as e:
            print(f"   ❌ PostgreSQL query failed: {e}")
            return self._format_error_result(query_name, query_data, str(e), 'postgresql')
    
    def execute_neo4j_query(self, query_name, query_data):
        """Neo4j queries not supported - return error"""
        print(f"⚠️  Neo4j: {query_name} - Not supported in this version")
        return self._format_error_result(query_name, query_data, "Neo4j not supported", 'neo4j')
    
    def _format_query_result(self, query_name, query_data, results, column_names, execution_time_ms, affected_tables, database_type):
        """Format query result in standard format"""
        result_data = {
            'query_info': {
                'name': query_name,
                'description': query_data['description'],
                'dataset_reference': query_data['dataset_reference'],
                'database': database_type,
                'sql': query_data.get('sql', query_data.get('cypher', '')),
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
                'sample_data': results[:QUERY_CONFIG.get('sample_data_limit', 3)] if results else [],
                'data_types': [str(type(col).__name__) if results and col is not None else 'NoneType' 
                             for col in (results[0] if results else [])]
            },
            'results_summary': {
                'has_data': len(results) > 0,
                'first_row': list(results[0]) if results else None,
                'total_data_points': len(results) * len(column_names) if results else 0
            }
        }
        
        # Convert results to JSON-serializable format
        if results:
            serializable_results = []
            for row in results[:QUERY_CONFIG.get('sample_data_limit', 3)]:
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
        
        print(f"   ⏱️  Response time: {execution_time_ms:.2f}ms")
        print(f"   📊 Rows returned: {len(results):,}")
        print(f"   🗂️  Tables/Collections: {', '.join(affected_tables)}")
        
        return result_data
    
    def _format_error_result(self, query_name, query_data, error_message, database_type):
        """Format error result in standard format"""
        return {
            'query_info': {
                'name': query_name,
                'description': query_data['description'],
                'dataset_reference': query_data['dataset_reference'],
                'database': database_type,
                'sql': query_data.get('sql', query_data.get('cypher', '')),
                'affected_tables': [],
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
                'message': error_message,
                'error_type': 'DatabaseError'
            }
        }
    
    def execute_query(self, query_name, query_data):
        """Execute a query based on its database type and store result"""
        database_type = query_data.get('database', 'postgresql')
        
        self.total_queries_executed += 1
        
        if database_type == 'postgresql':
            result = self.execute_postgresql_query(query_name, query_data)
        elif database_type == 'neo4j':
            result = self.execute_neo4j_query(query_name, query_data)
        else:
            print(f"❌ Unknown database type: {database_type}")
            return False
        
        if result:
            # Store result in memory for comparisons
            self.results[query_name] = result
            self.execution_order.append(query_name)
            
            # Store result in database
            self.store_query_result(query_name, result)
            
            # Check if query was successful
            if 'error' not in result:
                self.successful_queries += 1
                print(f"   ✅ Query completed successfully")
                return True
            else:
                self.failed_queries += 1
                print(f"   ❌ Query failed")
                return False
        
        self.failed_queries += 1
        return False
    
    def execute_queries_in_loop(self, query_names=None, database_filter=None, skip_on_error=False):
        """Execute queries with enhanced database support and storage"""
        print("🚀 Enhanced Query Execution with Database Storage\n")
        print("=" * 80)
        
        # Create analytics run
        if not self.create_analytics_run():
            print("❌ Failed to create analytics run, stopping execution")
            return False
        
        # Determine which queries to run
        if query_names is None:
            queries_to_run = get_all_queries()
        else:
            available_queries = get_query_list()
            invalid_queries = [q for q in query_names if q not in available_queries]
            
            if invalid_queries:
                print(f"❌ Invalid query names: {invalid_queries}")
                print(f"✅ Available queries: {available_queries}")
                return False
            
            queries_to_run = {name: get_query(name) for name in query_names}
        
        # Filter by database type if specified
        if database_filter:
            queries_to_run = {name: query for name, query in queries_to_run.items() 
                            if query.get('database') == database_filter}
            print(f"🔍 Filtered to {database_filter} queries only")
        
        print(f"📋 Executing {len(queries_to_run)} queries")
        print(f"📊 Analytics Run ID: {self.analytics_run_id}")
        print(f"⚙️  Skip on error: {'Yes' if skip_on_error else 'No'}")
        print(f"💾 Storage: Database only (no file export)")
        print("=" * 80)
        
        # Execute individual queries
        for i, (query_name, query_data) in enumerate(queries_to_run.items(), 1):
            print(f"\n[{i}/{len(queries_to_run)}] Processing: {query_name}")
            print(f"   Database: {query_data.get('database', 'postgresql').upper()}")
            
            success = self.execute_query(query_name, query_data)
            
            if not success and not skip_on_error:
                print(f"   🛑 Stopping execution due to error")
                break
            elif not success:
                print(f"   ⏭️  Continuing to next query")
        
        # Update analytics run with final statistics
        self.update_analytics_run()
        
        # Execution summary
        print("\n" + "=" * 80)
        print("📊 ENHANCED EXECUTION SUMMARY")
        print("=" * 80)
        print(f"📊 Analytics Run ID: {self.analytics_run_id}")
        print(f"✅ Successful queries: {self.successful_queries}")
        print(f"❌ Failed queries: {self.failed_queries}")
        print(f"📋 Total attempted: {self.total_queries_executed}")
        
        # Database breakdown
        pg_queries = len([q for q in self.results.values() if q['query_info']['database'] == 'postgresql'])
        
        print(f"🗄️  PostgreSQL queries: {pg_queries}")
        
        print(f"\n💾 All results stored in Analytics_Query_Results table")
        print(f"💡 Use view_analytics_results.py to view stored results")
        
        return self.successful_queries > 0
    
    def display_performance_summary(self):
        """Display performance summary for executed queries"""
        if not self.results:
            print("No query results available for performance summary")
            return
        
        print("\n" + "=" * 80)
        print("⚡ QUERY PERFORMANCE SUMMARY")
        print("=" * 80)
        
        performance_data = []
        total_time = 0
        total_rows = 0
        
        for query_name, result in self.results.items():
            if 'error' not in result:
                response_time = result['performance_metrics']['response_time_ms']
                rows_returned = result['performance_metrics']['rows_returned']
                
                total_time += response_time
                total_rows += rows_returned
                
                performance_data.append([
                    query_name,
                    f"{response_time:.2f}ms",
                    f"{rows_returned:,}",
                    result['query_info']['database'].upper()
                ])
        
        headers = ["Query Name", "Response Time", "Rows", "Database"]
        print(tabulate(performance_data, headers=headers, tablefmt="grid"))
        
        # Overall summary
        if performance_data:
            avg_time = total_time / len(performance_data)
            print(f"\n🎯 Overall Performance:")
            print(f"   Total execution time: {total_time:.2f}ms")
            print(f"   Average query time: {avg_time:.2f}ms")
            print(f"   Total rows returned: {total_rows:,}")
            print(f"   Queries executed: {len(performance_data)}")


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
    
    return postgres_config


def main():
    """Main execution function with PostgreSQL database support"""
    print("📊 Enhanced PostgreSQL Analytics with Database Storage")
    print("=" * 80)
    print("🔄 PostgreSQL query execution with database storage")
    print("💾 All results stored in Analytics_Query_Results table")
    
    # Load database configurations
    postgres_config = load_environment()
    
    # Initialize enhanced analytics
    analytics = EnhancedAnalytics(postgres_config)
    
    if not analytics.connect_databases():
        print("❌ Failed to connect to required databases")
        return
    
    try:
        # Execute all PostgreSQL queries with database storage
        success = analytics.execute_queries_in_loop()
        
        # Alternative execution options:
        # success = analytics.execute_queries_in_loop(database_filter='postgresql')
        # success = analytics.execute_queries_in_loop(
        #     query_names=["favorite_products", "favorite_categories"]
        # )
        
        if success:
            # Display performance summary
            analytics.display_performance_summary()
            
            print("\n🎉 Enhanced analytics completed successfully!")
            print("📋 Features used:")
            print("   • PostgreSQL query execution")
            print("   • Complete database storage in Analytics_Query_Results")
            print("   • Performance analysis and tracking")
            print("   • No file exports - all data in database")
            print(f"\n💾 Analytics Run ID: {analytics.analytics_run_id}")
            print("💡 Query results to view stored data:")
            print("   • SELECT * FROM Analytics_Runs ORDER BY export_timestamp DESC;")
            print(f"   • SELECT * FROM Analytics_Query_Results WHERE run_id = {analytics.analytics_run_id};")
            
        else:
            print("\n❌ Analytics execution failed or was incomplete")
        
    except KeyboardInterrupt:
        print("\n⏹️  Analytics interrupted by user")
        if analytics.analytics_run_id:
            print(f"💾 Partial results saved in Analytics Run ID: {analytics.analytics_run_id}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analytics.disconnect_databases()
        print("\n🔌 Database connections closed")


if __name__ == "__main__":
    main()