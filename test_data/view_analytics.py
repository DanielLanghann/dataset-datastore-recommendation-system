#!/usr/bin/env python3
"""
view_analytics_results.py
View analytics results stored in the database
"""

import json
import psycopg2
from decouple import config
from datetime import datetime
import argparse
from tabulate import tabulate

def load_environment():
    """Load database configuration from environment"""
    return {
        'host': config('DB_HOST', 'localhost'),
        'database': config('DB_NAME', 'test_data'),
        'user': config('DB_USER', 'test'),
        'password': config('DB_PASSWORD', 'test'),
        'port': int(config('DB_PORT', 5433)),
        'connect_timeout': int(config('DB_CONNECT_TIMEOUT', 10))
    }

def connect_database():
    """Connect to database"""
    try:
        db_config = load_environment()
        connection = psycopg2.connect(**db_config)
        return connection
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def show_analytics_runs(connection, limit=10):
    """Show recent analytics runs"""
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT 
                run_id,
                export_timestamp,
                total_queries_executed,
                successful_queries,
                total_execution_time_ms,
                average_response_time_ms,
                success_rate_percent,
                script_version,
                description
            FROM Analytics_Runs 
            ORDER BY export_timestamp DESC 
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        cursor.close()
        
        if not results:
            print("📊 No analytics runs found")
            return
        
        print(f"📊 Recent {len(results)} Analytics Runs:")
        print("=" * 120)
        
        headers = ["Run ID", "Timestamp", "Queries", "Success", "Time(s)", "Avg(ms)", "Success%", "Version"]
        table_data = []
        
        for row in results:
            run_id, timestamp, total_q, success_q, total_time, avg_time, success_rate, version, desc = row
            
            table_data.append([
                run_id,
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                f"{success_q}/{total_q}" if total_q else "0/0",
                success_q or 0,
                f"{(total_time / 1000):.1f}" if total_time else "0.0",
                f"{avg_time:.1f}" if avg_time else "0.0",
                f"{success_rate:.1f}%" if success_rate else "0.0%",
                version or "Unknown"
            ])
        
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching analytics runs: {e}")

def show_run_details(connection, run_id):
    """Show detailed information for a specific analytics run"""
    try:
        cursor = connection.cursor()
        
        # Get run details
        run_query = """
            SELECT 
                run_id, export_timestamp, database_host, database_name,
                total_queries_executed, successful_queries, execution_order,
                script_version, description, total_execution_time_ms,
                total_rows_queried, average_response_time_ms, success_rate_percent
            FROM Analytics_Runs 
            WHERE run_id = %s
        """
        
        cursor.execute(run_query, (run_id,))
        run_result = cursor.fetchone()
        
        if not run_result:
            print(f"❌ No analytics run found with ID {run_id}")
            cursor.close()
            return
        
        (run_id, timestamp, db_host, db_name, total_queries, successful_queries,
         execution_order, script_version, description, total_time,
         total_rows, avg_time, success_rate) = run_result
        
        print(f"📊 Analytics Run Details (ID: {run_id})")
        print("=" * 80)
        print(f"Timestamp:          {timestamp}")
        print(f"Database:           {db_host}/{db_name}")
        print(f"Script Version:     {script_version}")
        print(f"Description:        {description}")
        print(f"Total Queries:      {total_queries}")
        print(f"Successful:         {successful_queries}")
        print(f"Success Rate:       {success_rate:.1f}%" if success_rate else "N/A")
        print(f"Total Time:         {(total_time / 1000):.2f}s" if total_time else "N/A")
        print(f"Average Time:       {avg_time:.2f}ms" if avg_time else "N/A")
        print(f"Total Rows:         {total_rows:,}" if total_rows else "N/A")
        
        # Show execution order
        try:
            if execution_order:
                order_list = json.loads(execution_order)
                print(f"Execution Order:    {', '.join(order_list)}")
        except:
            print(f"Execution Order:    {execution_order}")
        
        # Get query results for this run
        results_query = """
            SELECT 
                query_name, response_time_ms, rows_returned, 
                columns_returned, has_data, execution_timestamp
            FROM Analytics_Query_Results 
            WHERE run_id = %s
            ORDER BY execution_order
        """
        
        cursor.execute(results_query, (run_id,))
        query_results = cursor.fetchall()
        
        if query_results:
            print(f"\n📋 Query Results ({len(query_results)} queries):")
            print("-" * 80)
            
            headers = ["Query Name", "Time(ms)", "Rows", "Cols", "Has Data"]
            table_data = []
            
            for query_result in query_results:
                query_name, response_time, rows, cols, has_data, exec_time = query_result
                table_data.append([
                    query_name,
                    f"{response_time:.2f}" if response_time else "0.00",
                    f"{rows:,}" if rows else "0",
                    cols if cols else "0",
                    "✅" if has_data else "❌"
                ])
            
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching run details: {e}")

def show_query_details(connection, run_id, query_name):
    """Show detailed information for a specific query result"""
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT 
                query_name, query_description, dataset_reference, sql_query,
                affected_tables, execution_timestamp, response_time_ms,
                rows_returned, columns_returned, column_names, sample_data,
                has_data, first_row, total_data_points
            FROM Analytics_Query_Results 
            WHERE run_id = %s AND query_name = %s
        """
        
        cursor.execute(query, (run_id, query_name))
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"❌ No query result found for run {run_id}, query '{query_name}'")
            return
        
        (query_name, description, dataset_ref, sql_query, affected_tables,
         exec_timestamp, response_time, rows_returned, cols_returned,
         column_names, sample_data, has_data, first_row, total_data_points) = result
        
        print(f"🔍 Query Result Details")
        print("=" * 80)
        print(f"Run ID:             {run_id}")
        print(f"Query Name:         {query_name}")
        print(f"Description:        {description}")
        print(f"Dataset Reference:  {dataset_ref}")
        print(f"Execution Time:     {exec_timestamp}")
        print(f"Response Time:      {response_time:.2f}ms" if response_time else "N/A")
        print(f"Rows Returned:      {rows_returned:,}" if rows_returned else "0")
        print(f"Columns:            {cols_returned}" if cols_returned else "0")
        print(f"Has Data:           {'Yes' if has_data else 'No'}")
        print(f"Total Data Points:  {total_data_points:,}" if total_data_points else "0")
        
        # Show affected tables
        try:
            if affected_tables:
                tables = json.loads(affected_tables)
                print(f"Affected Tables:    {', '.join(tables)}")
        except:
            print(f"Affected Tables:    {affected_tables}")
        
        # Show SQL query (truncated)
        if sql_query:
            print(f"\nSQL Query:")
            print("-" * 40)
            if len(sql_query) > 500:
                print(sql_query[:500] + "...")
            else:
                print(sql_query)
        
        # Show column names
        try:
            if column_names:
                cols = json.loads(column_names)
                print(f"\nColumn Names: {', '.join(cols)}")
        except:
            print(f"\nColumn Names: {column_names}")
        
        # Show sample data
        try:
            if sample_data and has_data:
                sample = json.loads(sample_data)
                if sample:
                    print(f"\nSample Data:")
                    print("-" * 40)
                    
                    # Try to parse column names for table headers
                    try:
                        cols = json.loads(column_names) if column_names else []
                    except:
                        cols = []
                    
                    if cols and len(sample) > 0:
                        # Limit sample data display
                        display_sample = sample[:3]  # Show max 3 rows
                        print(tabulate(display_sample, headers=cols, tablefmt="grid"))
                        if len(sample) > 3:
                            print(f"... and {len(sample) - 3} more rows")
                    else:
                        for i, row in enumerate(sample[:3]):
                            print(f"Row {i+1}: {row}")
        except Exception as e:
            print(f"\nSample Data: Error parsing - {e}")
        
        # Show first row
        try:
            if first_row and has_data:
                first = json.loads(first_row)
                print(f"\nFirst Row: {first}")
        except:
            print(f"\nFirst Row: {first_row}")
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching query details: {e}")

def show_performance_summary(connection, run_id=None):
    """Show performance summary across runs or for specific run"""
    try:
        cursor = connection.cursor()
        
        if run_id:
            # Performance for specific run
            query = """
                SELECT 
                    query_name, response_time_ms, rows_returned
                FROM Analytics_Query_Results 
                WHERE run_id = %s
                ORDER BY response_time_ms DESC
            """
            cursor.execute(query, (run_id,))
            title = f"Performance Summary for Run {run_id}"
        else:
            # Performance across all recent runs
            query = """
                SELECT 
                    query_name, 
                    AVG(response_time_ms) as avg_time,
                    AVG(rows_returned) as avg_rows
                FROM Analytics_Query_Results 
                WHERE response_time_ms > 0
                GROUP BY query_name
                ORDER BY avg_time DESC
            """
            cursor.execute(query)
            title = "Performance Summary (All Runs)"
        
        results = cursor.fetchall()
        cursor.close()
        
        if not results:
            print("📊 No performance data found")
            return
        
        print(f"⚡ {title}")
        print("=" * 60)
        
        if run_id:
            headers = ["Query Name", "Time(ms)", "Rows"]
            table_data = [[name, f"{time_ms:.2f}", f"{rows:,}"] 
                         for name, time_ms, rows in results]
        else:
            headers = ["Query Name", "Avg Time(ms)", "Avg Rows"]
            table_data = [[name, f"{avg_time:.2f}", f"{avg_rows:.0f}"] 
                         for name, avg_time, avg_rows in results]
        
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching performance summary: {e}")

def list_queries_in_run(connection, run_id):
    """List all queries in a specific run"""
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT 
                query_name, has_data, rows_returned, response_time_ms
            FROM Analytics_Query_Results 
            WHERE run_id = %s
            ORDER BY execution_order
        """
        
        cursor.execute(query, (run_id,))
        results = cursor.fetchall()
        cursor.close()
        
        if not results:
            print(f"❌ No queries found for run {run_id}")
            return []
        
        print(f"📋 Queries in Run {run_id}:")
        for i, (query_name, has_data, rows, time_ms) in enumerate(results, 1):
            status = "✅" if has_data else "❌"
            print(f"{i:2d}. {query_name} {status} ({rows:,} rows, {time_ms:.2f}ms)")
        
        return [result[0] for result in results]
        
    except psycopg2.Error as e:
        print(f"❌ Error listing queries: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description='View analytics results stored in database')
    parser.add_argument('--run-id', type=int, metavar='ID', 
                       help='Show details for specific analytics run')
    parser.add_argument('--query', type=str, metavar='NAME',
                       help='Show details for specific query (requires --run-id)')
    parser.add_argument('--performance', action='store_true',
                       help='Show performance summary')
    parser.add_argument('--list-queries', action='store_true',
                       help='List queries in specific run (requires --run-id)')
    parser.add_argument('--limit', type=int, default=10,
                       help='Number of recent runs to show (default: 10)')
    
    args = parser.parse_args()
    
    print("📊 Analytics Results Viewer")
    print("=" * 50)
    
    connection = connect_database()
    if not connection:
        return
    
    try:
        if args.query and args.run_id:
            # Show specific query details
            show_query_details(connection, args.run_id, args.query)
        elif args.run_id and args.list_queries:
            # List queries in run
            queries = list_queries_in_run(connection, args.run_id)
            if queries:
                print(f"\n💡 Use --run-id {args.run_id} --query <name> to see details")
        elif args.run_id:
            # Show run details
            show_run_details(connection, args.run_id)
        elif args.performance:
            # Show performance summary
            show_performance_summary(connection)
        else:
            # Show recent runs
            show_analytics_runs(connection, args.limit)
            print(f"\n💡 Use --run-id <ID> to see details for a specific run")
            print(f"💡 Use --performance to see performance summary")
            
    finally:
        connection.close()

if __name__ == "__main__":
    main()