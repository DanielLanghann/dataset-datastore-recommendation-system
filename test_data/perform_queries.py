#!/usr/bin/env python3
"""
enhanced_perform_queries.py
Enhanced query performance script with Neo4j support and database comparison
"""

import psycopg2
import time
import json
import re
from datetime import datetime, date
from decimal import Decimal
from decouple import config
from tabulate import tabulate
from neo4j import GraphDatabase

# Import our enhanced queries module
from queries import (
    get_all_queries, get_query_list, get_query, 
    get_comparison_query_pairs,
    QUERY_CONFIG
)


class EnhancedAnalytics:
    def __init__(self, postgres_config, neo4j_config=None):
        self.postgres_config = postgres_config
        self.neo4j_config = neo4j_config
        self.pg_connection = None
        self.neo4j_driver = None
        self.results = {}
        self.execution_order = []
        self.analytics_run_id = None
        self.execution_start_time = datetime.now()
        self.comparison_results = {}
    
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
        """Connect to both PostgreSQL and Neo4j"""
        success = True
        
        # Connect to PostgreSQL
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            success = False
        
        # Connect to Neo4j (optional)
        if self.neo4j_config:
            try:
                self.neo4j_driver = GraphDatabase.driver(
                    self.neo4j_config['uri'],
                    auth=(self.neo4j_config['user'], self.neo4j_config['password'])
                )
                
                # Test Neo4j connection
                with self.neo4j_driver.session() as session:
                    result = session.run("RETURN 'Connection successful' as message")
                    print(f"✅ Connected to Neo4j: {self.neo4j_config['uri']}")
                    
            except Exception as e:
                print(f"⚠️  Neo4j connection failed: {e}")
                print("   Neo4j queries will be skipped")
                self.neo4j_driver = None
        
        return success
    
    def disconnect_databases(self):
        """Close database connections"""
        if self.pg_connection:
            self.pg_connection.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
    
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
        """Execute a Neo4j query"""
        if not self.neo4j_driver:
            print(f"⚠️  Neo4j: {query_name} - Driver not available")
            return self._format_error_result(query_name, query_data, "Neo4j driver not available", 'neo4j')
        
        print(f"🔍 Neo4j: {query_name}")
        
        try:
            with self.neo4j_driver.session() as session:
                start_time = time.time()
                
                # Handle parameterized queries
                parameters = query_data.get('parameters', {})
                if 'product_name' in query_data.get('cypher', '') and not parameters:
                    # Default parameters for recommendation queries
                    parameters = {'product_name': 'iPhone 15 Pro', 'limit': 10}
                
                result = session.run(query_data['cypher'], parameters)
                records = list(result)
                end_time = time.time()
                
                execution_time_ms = (end_time - start_time) * 1000
                
                # Convert Neo4j records to list format
                if records:
                    column_names = list(records[0].keys())
                    results = [list(record.values()) for record in records]
                else:
                    column_names = []
                    results = []
                
                return self._format_query_result(
                    query_name, query_data, results, column_names,
                    execution_time_ms, ['Neo4j Graph'], 'neo4j'
                )
                
        except Exception as e:
            print(f"   ❌ Neo4j query failed: {e}")
            return self._format_error_result(query_name, query_data, str(e), 'neo4j')
    
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
        
        # Convert results to JSON-serializable format
        if results:
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
        """Execute a query based on its database type"""
        database_type = query_data.get('database', 'postgresql')
        
        if database_type == 'postgresql':
            result = self.execute_postgresql_query(query_name, query_data)
        elif database_type == 'neo4j':
            result = self.execute_neo4j_query(query_name, query_data)
        else:
            print(f"❌ Unknown database type: {database_type}")
            return None
        
        self.results[query_name] = result
        self.execution_order.append(query_name)
        
        return result is not None and 'error' not in result
    
    def run_performance_comparison(self):
        """Run performance comparison between PostgreSQL and Neo4j"""
        print("\n" + "=" * 80)
        print("⚡ DATABASE PERFORMANCE COMPARISON")
        print("=" * 80)
        
        comparison_pairs = get_comparison_query_pairs()
        
        for comparison in comparison_pairs:
            print(f"\n🔄 {comparison['comparison_name']}:")
            print(f"   Description: {comparison['description']}")
            
            pg_query_name = comparison['postgresql_query']
            neo4j_query_name = comparison['neo4j_query']
            
            # Execute PostgreSQL query
            pg_query = get_query(pg_query_name)
            neo4j_query = get_query(neo4j_query_name)
            
            if not pg_query or not neo4j_query:
                print(f"   ⚠️  Queries not found: {pg_query_name}, {neo4j_query_name}")
                continue
            
            pg_success = self.execute_query(pg_query_name, pg_query)
            neo4j_success = self.execute_query(neo4j_query_name, neo4j_query)
            
            # Compare results
            if pg_success and neo4j_success:
                pg_result = self.results[pg_query_name]
                neo4j_result = self.results[neo4j_query_name]
                
                pg_time = pg_result['performance_metrics']['response_time_ms']
                neo4j_time = neo4j_result['performance_metrics']['response_time_ms']
                pg_rows = pg_result['performance_metrics']['rows_returned']
                neo4j_rows = neo4j_result['performance_metrics']['rows_returned']
                
                # Calculate performance difference
                if pg_time > 0 and neo4j_time > 0:
                    if pg_time > neo4j_time:
                        speedup = pg_time / neo4j_time
                        faster_db = "Neo4j"
                    else:
                        speedup = neo4j_time / pg_time
                        faster_db = "PostgreSQL"
                    
                    self.comparison_results[comparison['comparison_name']] = {
                        'postgresql_time_ms': pg_time,
                        'neo4j_time_ms': neo4j_time,
                        'postgresql_rows': pg_rows,
                        'neo4j_rows': neo4j_rows,
                        'faster_database': faster_db,
                        'speedup_factor': round(speedup, 2),
                        'performance_difference_percent': round(((max(pg_time, neo4j_time) - min(pg_time, neo4j_time)) / max(pg_time, neo4j_time)) * 100, 1)
                    }
                    
                    print(f"   📊 PostgreSQL: {pg_time:.2f}ms ({pg_rows:,} rows)")
                    print(f"   📊 Neo4j: {neo4j_time:.2f}ms ({neo4j_rows:,} rows)")
                    print(f"   🏆 Faster: {faster_db} ({speedup:.2f}x speedup)")
    
    def execute_queries_in_loop(self, query_names=None, database_filter=None, skip_on_error=False, run_comparisons=True):
        """Execute queries with enhanced database support"""
        print("🚀 Enhanced Query Execution with Multi-Database Support\n")
        print("=" * 80)
        
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
        print(f"⚙️  Skip on error: {'Yes' if skip_on_error else 'No'}")
        print(f"🔄 Performance comparisons: {'Yes' if run_comparisons else 'No'}")
        print("=" * 80)
        
        # Execute individual queries
        successful_queries = 0
        failed_queries = 0
        
        for i, (query_name, query_data) in enumerate(queries_to_run.items(), 1):
            print(f"\n[{i}/{len(queries_to_run)}] Processing: {query_name}")
            print(f"   Database: {query_data.get('database', 'postgresql').upper()}")
            
            success = self.execute_query(query_name, query_data)
            
            if success:
                successful_queries += 1
                print(f"   ✅ Query completed successfully")
            else:
                failed_queries += 1
                print(f"   ❌ Query failed")
                
                if not skip_on_error:
                    print(f"   🛑 Stopping execution due to error")
                    break
                else:
                    print(f"   ⏭️  Continuing to next query")
        
        # Run performance comparisons if enabled
        if run_comparisons and self.neo4j_driver:
            self.run_performance_comparison()
        
        # Execution summary
        print("\n" + "=" * 80)
        print("📊 ENHANCED EXECUTION SUMMARY")
        print("=" * 80)
        print(f"✅ Successful queries: {successful_queries}")
        print(f"❌ Failed queries: {failed_queries}")
        print(f"📋 Total attempted: {len(self.execution_order)}")
        
        # Database breakdown
        pg_queries = len([q for q in self.results.values() if q['query_info']['database'] == 'postgresql'])
        neo4j_queries = len([q for q in self.results.values() if q['query_info']['database'] == 'neo4j'])
        
        print(f"🗄️  PostgreSQL queries: {pg_queries}")
        print(f"🌐 Neo4j queries: {neo4j_queries}")
        
        # Performance comparison summary
        if self.comparison_results:
            print(f"\n🏆 Performance Comparison Results:")
            for comparison_name, results in self.comparison_results.items():
                print(f"   {comparison_name}: {results['faster_database']} wins by {results['speedup_factor']}x")
        
        return successful_queries > 0
    
    def display_comparison_summary(self):
        """Display detailed performance comparison summary"""
        if not self.comparison_results:
            print("No performance comparisons available")
            return
        
        print("\n" + "=" * 80)
        print("🏆 DETAILED PERFORMANCE COMPARISON SUMMARY")
        print("=" * 80)
        
        comparison_data = []
        total_pg_time = 0
        total_neo4j_time = 0
        
        for comparison_name, results in self.comparison_results.items():
            pg_time = results['postgresql_time_ms']
            neo4j_time = results['neo4j_time_ms']
            faster_db = results['faster_database']
            speedup = results['speedup_factor']
            
            total_pg_time += pg_time
            total_neo4j_time += neo4j_time
            
            comparison_data.append([
                comparison_name,
                f"{pg_time:.2f}ms",
                f"{neo4j_time:.2f}ms",
                f"{faster_db}",
                f"{speedup:.2f}x"
            ])
        
        headers = ["Comparison", "PostgreSQL", "Neo4j", "Winner", "Speedup"]
        print(tabulate(comparison_data, headers=headers, tablefmt="grid"))
        
        # Overall summary
        if total_pg_time > 0 and total_neo4j_time > 0:
            overall_winner = "Neo4j" if total_neo4j_time < total_pg_time else "PostgreSQL"
            overall_speedup = max(total_pg_time, total_neo4j_time) / min(total_pg_time, total_neo4j_time)
            
            print(f"\n🎯 Overall Performance:")
            print(f"   Total PostgreSQL time: {total_pg_time:.2f}ms")
            print(f"   Total Neo4j time: {total_neo4j_time:.2f}ms")
            print(f"   Overall winner: {overall_winner} ({overall_speedup:.2f}x faster)")


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
    """Main execution function with enhanced database support"""
    print("📊 Enhanced Multi-Database Analytics & Performance Testing")
    print("=" * 80)
    print("🔄 PostgreSQL + Neo4j support with performance comparisons")
    
    # Load database configurations
    postgres_config, neo4j_config = load_environment()
    
    # Initialize enhanced analytics
    analytics = EnhancedAnalytics(postgres_config, neo4j_config)
    
    if not analytics.connect_databases():
        print("❌ Failed to connect to required databases")
        return
    
    try:
        # Example execution options:
        
        # Option 1: Execute ALL queries with comparisons
        success = analytics.execute_queries_in_loop(run_comparisons=True)
        
        # Option 2: Execute only PostgreSQL queries
        # success = analytics.execute_queries_in_loop(database_filter='postgresql')
        
        # Option 3: Execute only Neo4j queries
        # success = analytics.execute_queries_in_loop(database_filter='neo4j')
        
        # Option 4: Execute specific queries
        # success = analytics.execute_queries_in_loop(
        #     query_names=["product_associations_postgresql", "product_associations_neo4j"],
        #     run_comparisons=True
        # )
        
        if success:
            # Display detailed comparison summary
            analytics.display_comparison_summary()
            
            print("\n🎉 Enhanced analytics completed successfully!")
            print("📋 Features used:")
            print("   • Multi-database support (PostgreSQL + Neo4j)")
            print("   • Performance comparisons between databases")
            print("   • Graph-based queries for product associations")
            print("   • Detailed execution metrics and analysis")
            
        else:
            print("\n❌ Analytics execution failed or was incomplete")
        
    except KeyboardInterrupt:
        print("\n⏹️  Analytics interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analytics.disconnect_databases()
        print("\n🔌 Database connections closed")


if __name__ == "__main__":
    main()