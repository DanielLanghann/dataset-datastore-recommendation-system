#!/usr/bin/env python3
"""
perform_cypher_queries.py
Neo4j Cypher query performance script with PostgreSQL storage
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
from neo4j import GraphDatabase, basic_auth

# Import our Cypher queries module
from cypher_queries import (
    get_all_queries, get_query_list, get_query, 
    QUERY_CONFIG
)


class CypherAnalytics:
    def __init__(self, postgres_config, neo4j_config):
        self.postgres_config = postgres_config
        self.neo4j_config = neo4j_config
        self.pg_connection = None
        self.neo4j_driver = None
        self.results = {}
        self.execution_order = []
        self.analytics_run_id = None
        self.execution_start_time = datetime.now()
        self.total_queries_executed = 0
        self.successful_queries = 0
        self.failed_queries = 0
    
    def json_serializer(self, obj):
        """Custom JSON serializer for various data types"""
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
        # Connect to PostgreSQL
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            return False
        
        # Connect to Neo4j
        try:
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=basic_auth(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            # Test the connection
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
            print(f"✅ Connected to Neo4j: {self.neo4j_config['uri']}")
            return True
        except Exception as e:
            print(f"❌ Neo4j connection failed: {e}")
            return False
    
    def connect_databases(self):
        """Connect to both PostgreSQL and Neo4j"""
        # Connect to PostgreSQL
        try:
            self.pg_connection = psycopg2.connect(**self.postgres_config)
            print(f"✅ Connected to PostgreSQL: {self.postgres_config['host']}:{self.postgres_config['port']}")
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            return False
        
        # Connect to Neo4j
        try:
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=basic_auth(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            # Test the connection
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
            print(f"✅ Connected to Neo4j: {self.neo4j_config['uri']}")
            return True
        except Exception as e:
            print(f"❌ Neo4j connection failed: {e}")
            return False
    
    def disconnect_databases(self):
        """Close database connections"""
        if self.pg_connection:
            self.pg_connection.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
    
    def create_analytics_run(self):
        """Create a new analytics run record in PostgreSQL and return its ID"""
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
                self.neo4j_config['uri'],
                'neo4j_graph_db',
                '1.0_cypher_analytics',
                'Neo4j Cypher analytics with PostgreSQL storage',
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
            
            cursor.execute("""
                INSERT INTO Analytics_Query_Results (
                    run_id, query_name, query_description, dataset_reference,
                    query, affected_tables, execution_timestamp, execution_order,
                    response_time_ms, response_time_seconds, rows_returned, columns_returned,
                    column_names, sample_data, data_types,
                    has_data, first_row, total_data_points, system
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                self.analytics_run_id,
                query_name,
                query_info['description'],
                query_info['dataset_reference'],
                query_info['cypher'],  # Store Cypher query in query field
                self.safe_json_dumps(query_info['affected_nodes']),  # Store node types instead of tables
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
                results_summary['total_data_points'],
                'neo4j'  # Add system identifier
            ))
            
            self.pg_connection.commit()
            cursor.close()
            
            print(f"   💾 Stored query result in PostgreSQL database")
            return True
            
        except psycopg2.Error as e:
            print(f"   ❌ Failed to store query result: {e}")
            self.pg_connection.rollback()
            return False
    
    def extract_nodes_from_cypher(self, cypher_query):
        """Extract node types from Cypher query using regex"""
        # Clean the query
        clean_query = re.sub(r'//.*?\n', '\n', cypher_query)
        clean_query = re.sub(r'/\*.*?\*/', '', clean_query, flags=re.DOTALL)
        clean_query = re.sub(r'\s+', ' ', clean_query)
        
        # Extract node patterns like (p:Product), (c:Category)
        node_pattern = r'\([a-zA-Z0-9_]*:([A-Z][a-zA-Z0-9_]*)\)'
        matches = re.findall(node_pattern, clean_query)
        
        return sorted(list(set(matches)))
    
    def execute_cypher_query(self, query_name, query_data):
        """Execute a Cypher query against Neo4j"""
        print(f"🔍 Neo4j Cypher: {query_name}")
        print(f"   📝 Query: {query_data['cypher']}")
        
        try:
            affected_nodes = self.extract_nodes_from_cypher(query_data['cypher'])
            
            # First, let's check what data exists in Neo4j
            with self.neo4j_driver.session() as session:
                # Check if we have any Product nodes
                product_count = session.run("MATCH (p:Product) RETURN count(p) as count").single()["count"]
                print(f"   🔍 Debug: Found {product_count} Product nodes in Neo4j")
                
                # Check if we have any Category nodes
                category_count = session.run("MATCH (c:Category) RETURN count(c) as count").single()["count"]
                print(f"   🔍 Debug: Found {category_count} Category nodes in Neo4j")
                
                # Check if we have any BOUGHT_TOGETHER relationships
                bought_together_count = session.run("MATCH ()-[r:BOUGHT_TOGETHER]->() RETURN count(r) as count").single()["count"]
                print(f"   🔍 Debug: Found {bought_together_count} BOUGHT_TOGETHER relationships in Neo4j")
                
                # Check if we have any BELONGS_TO relationships
                belongs_count = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count").single()["count"]
                print(f"   🔍 Debug: Found {belongs_count} BELONGS_TO relationships in Neo4j")
                
                # Show sample Product nodes with their properties
                sample_products = session.run("MATCH (p:Product) RETURN p LIMIT 3").data()
                print(f"   🔍 Debug: Sample Product nodes: {sample_products}")
                
            start_time = time.time()
            
            with self.neo4j_driver.session() as session:
                result = session.run(query_data['cypher'])
                records = list(result)
            
            end_time = time.time()
            
            execution_time_ms = (end_time - start_time) * 1000
            
            print(f"   🔍 Debug: Raw Neo4j result contains {len(records)} records")
            
            # Convert Neo4j records to regular Python data
            results = []
            column_names = []
            
            if records:
                # Get column names from first record
                column_names = list(records[0].keys())
                print(f"   🔍 Debug: Column names from Neo4j: {column_names}")
                
                # Show first record in detail
                if records:
                    print(f"   🔍 Debug: First raw record: {dict(records[0])}")
                
                # Convert records to list of tuples
                for i, record in enumerate(records):
                    row = []
                    for key in column_names:
                        value = record[key]
                        # Convert Neo4j types to Python types
                        if value is None:
                            row.append(None)
                        elif hasattr(value, 'value'):  # Neo4j Integer, Float, etc.
                            row.append(value.value)
                        elif isinstance(value, (int, float, bool, str)):
                            row.append(value)
                        else:
                            # Convert other types to string
                            row.append(str(value))
                    results.append(tuple(row))
                    
                    # Show first few converted rows for debugging
                    if i < 3:
                        print(f"   🔍 Debug: Converted row {i}: {row}")
            else:
                print("   ⚠️  No records returned from Neo4j query")
                
                # Let's try a simpler query to see if any data exists
                with self.neo4j_driver.session() as session:
                    simple_result = session.run("MATCH (p:Product) RETURN p.product_name LIMIT 5").data()
                    print(f"   🔍 Debug: Simple Product query result: {simple_result}")
            
            print(f"   🔍 Debug: Final results list has {len(results)} items")
            print(f"   🔍 Debug: Column names: {column_names}")
            if results:
                print(f"   🔍 Debug: First converted row: {results[0]}")
            
            return self._format_query_result(
                query_name, query_data, results, column_names, 
                execution_time_ms, affected_nodes
            )
            
        except Exception as e:
            print(f"   ❌ Neo4j Cypher query failed: {e}")
            import traceback
            traceback.print_exc()
            return self._format_error_result(query_name, query_data, str(e))
    
    def _format_query_result(self, query_name, query_data, results, column_names, execution_time_ms, affected_nodes):
        """Format query result in standard format"""
        print(f"   🔍 Debug: _format_query_result called with:")
        print(f"        - results length: {len(results)}")
        print(f"        - column_names: {column_names}")
        print(f"        - execution_time_ms: {execution_time_ms}")
        print(f"        - affected_nodes: {affected_nodes}")
        
        # Determine data types from the first row if available
        data_types = []
        if results and len(results) > 0:
            first_row = results[0]
            data_types = [type(col).__name__ if col is not None else 'NoneType' for col in first_row]
            print(f"   🔍 Debug: Data types from first row: {data_types}")
        
        result_data = {
            'query_info': {
                'name': query_name,
                'description': query_data['description'],
                'dataset_reference': query_data['dataset_reference'],
                'database': 'neo4j',
                'cypher': query_data['cypher'],
                'affected_nodes': affected_nodes,  # Node types instead of tables
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
                'data_types': data_types
            },
            'results_summary': {
                'has_data': len(results) > 0,
                'first_row': list(results[0]) if results else None,
                'total_data_points': len(results) * len(column_names) if results else 0
            }
        }
        
        print(f"   🔍 Debug: Formatted result_data:")
        print(f"        - performance_metrics: {result_data['performance_metrics']}")
        print(f"        - results_summary: {result_data['results_summary']}")
        print(f"        - data_structure keys: {list(result_data['data_structure'].keys())}")
        print(f"        - sample_data length: {len(result_data['data_structure']['sample_data'])}")
        
        print(f"   ⏱️  Response time: {execution_time_ms:.2f}ms")
        print(f"   📊 Rows returned: {len(results):,}")
        print(f"   🗂️  Node types: {', '.join(affected_nodes)}")
        
        return result_data
    
    def _format_error_result(self, query_name, query_data, error_message):
        """Format error result in standard format"""
        return {
            'query_info': {
                'name': query_name,
                'description': query_data['description'],
                'dataset_reference': query_data['dataset_reference'],
                'database': 'neo4j',
                'cypher': query_data['cypher'],
                'affected_nodes': [],
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
                'error_type': 'CypherError'
            }
        }
    
    def execute_query(self, query_name, query_data):
        """Execute a Cypher query and store result"""
        self.total_queries_executed += 1
        
        result = self.execute_cypher_query(query_name, query_data)
        
        if result:
            # Store result in memory for comparisons
            self.results[query_name] = result
            self.execution_order.append(query_name)
            
            # Store result in PostgreSQL database
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
    
    def execute_queries_in_loop(self, query_names=None, skip_on_error=False):
        """Execute Cypher queries with PostgreSQL storage"""
        print("🚀 Neo4j Cypher Query Execution with PostgreSQL Storage\n")
        print("=" * 80)
        
        # Create analytics run in PostgreSQL
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
        
        print(f"📋 Executing {len(queries_to_run)} Cypher queries")
        print(f"📊 Analytics Run ID: {self.analytics_run_id}")
        print(f"⚙️  Skip on error: {'Yes' if skip_on_error else 'No'}")
        print(f"💾 Storage: PostgreSQL Analytics_Query_Results table")
        print("=" * 80)
        
        # Execute individual queries
        for i, (query_name, query_data) in enumerate(queries_to_run.items(), 1):
            print(f"\n[{i}/{len(queries_to_run)}] Processing: {query_name}")
            
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
        print("📊 CYPHER EXECUTION SUMMARY")
        print("=" * 80)
        print(f"📊 Analytics Run ID: {self.analytics_run_id}")
        print(f"✅ Successful queries: {self.successful_queries}")
        print(f"❌ Failed queries: {self.failed_queries}")
        print(f"📋 Total attempted: {self.total_queries_executed}")
        print(f"🗄️  Neo4j Cypher queries: {len(self.results)}")
        
        print(f"\n💾 All results stored in PostgreSQL Analytics_Query_Results table")
        print(f"💡 Results stored with database='neo4j' for identification")
        
        return self.successful_queries > 0
    
    def display_performance_summary(self):
        """Display performance summary for executed queries"""
        if not self.results:
            print("No query results available for performance summary")
            return
        
        print("\n" + "=" * 80)
        print("⚡ CYPHER QUERY PERFORMANCE SUMMARY")
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
                    "Neo4j"
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
    
    neo4j_config = {
        'uri': config('NEO4J_URI', 'bolt://localhost:7687'),
        'user': config('NEO4J_USER', 'neo4j'),
        'password': config('NEO4J_PASSWORD', 'password')
    }
    
    return postgres_config, neo4j_config


def main():
    """Main execution function with Neo4j and PostgreSQL support"""
    print("📊 Neo4j Cypher Analytics with PostgreSQL Storage")
    print("=" * 80)
    print("🔄 Neo4j Cypher query execution with PostgreSQL result storage")
    print("💾 All results stored in PostgreSQL Analytics_Query_Results table")
    
    # Load database configurations
    postgres_config, neo4j_config = load_environment()
    
    # Initialize Cypher analytics
    analytics = CypherAnalytics(postgres_config, neo4j_config)
    
    if not analytics.connect_databases():
        print("❌ Failed to connect to required databases")
        return
    
    try:
        # Execute all Cypher queries with PostgreSQL storage
        success = analytics.execute_queries_in_loop()
        
        # Alternative execution options:
        # success = analytics.execute_queries_in_loop(
        #     query_names=["product_associations"]
        # )
        
        if success:
            # Display performance summary
            analytics.display_performance_summary()
            
            print("\n🎉 Cypher analytics completed successfully!")
            print("📋 Features used:")
            print("   • Neo4j Cypher query execution")
            print("   • Complete PostgreSQL storage in Analytics_Query_Results")
            print("   • Performance analysis and tracking")
            print("   • Graph database analytics with relational storage")
            print(f"\n💾 Analytics Run ID: {analytics.analytics_run_id}")
            print("💡 Query results to view stored data:")
            print("   • SELECT * FROM Analytics_Runs WHERE database_name = 'neo4j_graph_db' ORDER BY export_timestamp DESC;")
            print(f"   • SELECT * FROM Analytics_Query_Results WHERE run_id = {analytics.analytics_run_id};")
            print("   • SELECT * FROM Analytics_Query_Results WHERE query LIKE '%MATCH%' ORDER BY execution_timestamp DESC;")
            
        else:
            print("\n❌ Cypher analytics execution failed or was incomplete")
        
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