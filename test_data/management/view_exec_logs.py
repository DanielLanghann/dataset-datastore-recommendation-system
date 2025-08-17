#!/usr/bin/env python3
"""
View execution logs and table statistics from Test_Data_Execution_Log
"""

import json
import psycopg2
from decouple import config
from datetime import datetime
import argparse

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

def show_recent_executions(connection, limit=10):
    """Show recent test data executions"""
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT 
                execution_id,
                execution_timestamp,
                execution_type,
                execution_status,
                records_created,
                total_execution_time_ms,
                tables_affected,
                configuration_used
            FROM Test_Data_Execution_Log 
            ORDER BY execution_timestamp DESC 
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        cursor.close()
        
        if not results:
            print("📊 No execution logs found")
            return
        
        print(f"📊 Recent {len(results)} Test Data Executions:")
        print("=" * 100)
        print(f"{'ID':<4} {'Timestamp':<20} {'Type':<18} {'Status':<15} {'Records':<10} {'Time(s)':<8} {'Tables'}")
        print("-" * 100)
        
        for row in results:
            exec_id, timestamp, exec_type, status, records, time_ms, tables_json, config_json = row
            
            # Parse tables
            try:
                tables = json.loads(tables_json) if tables_json else []
                tables_str = ", ".join(tables)
            except:
                tables_str = str(tables_json)[:30] + "..." if tables_json else "N/A"
            
            # Format time
            time_s = round(time_ms / 1000, 1) if time_ms else 0
            
            # Truncate long table lists
            if len(tables_str) > 40:
                tables_str = tables_str[:37] + "..."
            
            print(f"{exec_id:<4} {timestamp.strftime('%Y-%m-%d %H:%M:%S'):<20} {exec_type:<18} {status:<15} {records or 0:<10} {time_s:<8} {tables_str}")
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching execution logs: {e}")
        

def show_execution_details(connection, execution_id):
    """Show detailed information for a specific execution"""
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT 
                execution_id, execution_timestamp, script_name, script_version,
                execution_type, execution_status, total_operations, successful_operations,
                failed_operations, total_execution_time_ms, records_created,
                tables_affected, configuration_used, error_count, warning_count
            FROM Test_Data_Execution_Log 
            WHERE execution_id = %s
        """
        
        cursor.execute(query, (execution_id,))
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"❌ No execution found with ID {execution_id}")
            return
        
        (exec_id, timestamp, script_name, script_version, exec_type, status,
         total_ops, success_ops, failed_ops, time_ms, records, tables_json,
         config_json, errors, warnings) = result
        
        print(f"📊 Execution Details (ID: {exec_id})")
        print("=" * 60)
        print(f"Timestamp:        {timestamp}")
        print(f"Script:           {script_name} v{script_version}")
        print(f"Type:             {exec_type}")
        print(f"Status:           {status}")
        print(f"Duration:         {round(time_ms / 1000, 2) if time_ms else 0} seconds")
        print(f"Records Created:  {records or 0:,}")
        print(f"Operations:       {success_ops}/{total_ops} successful")
        if errors > 0:
            print(f"Errors:           {errors}")
        if warnings > 0:
            print(f"Warnings:         {warnings}")
        
        # Show tables affected
        try:
            tables = json.loads(tables_json) if tables_json else []
            if tables:
                print(f"Tables Affected:  {', '.join(tables)}")
        except:
            print(f"Tables Affected:  {tables_json}")
        
        # Show detailed configuration
        try:
            config = json.loads(config_json) if config_json else {}
            if config:
                print("\n📋 Configuration:")
                if 'records_created_per_table' in config:
                    print("Records per table:")
                    for table, count in config['records_created_per_table'].items():
                        print(f"  • {table}: {count:,} rows")
                
                if 'rows_per_table' in config and config['rows_per_table'] > 0:
                    print(f"Requested rows per table: {config['rows_per_table']:,}")
                
                if 'operation_type' in config:
                    print(f"Operation type: {config['operation_type']}")
        except:
            print(f"Configuration: {config_json}")
            
    except psycopg2.Error as e:
        print(f"❌ Error fetching execution details: {e}")

def show_current_table_counts(connection):
    """Show current row counts for all tables"""
    try:
        cursor = connection.cursor()
        
        tables = ['categories', 'customers', 'products', 'orders', 'order_items', 'product_associations']
        print("📊 Current Table Statistics:")
        print("=" * 40)
        
        total_rows = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table.capitalize():<20}: {count:,} rows")
                total_rows += count
            except psycopg2.Error as e:
                print(f"{table.capitalize():<20}: Error - {e}")
        
        print("=" * 40)
        print(f"{'Total Records':<20}: {total_rows:,} rows")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"❌ Error fetching table counts: {e}")

def main():
    parser = argparse.ArgumentParser(description='View test data execution logs and statistics')
    parser.add_argument('--details', type=int, metavar='ID', 
                       help='Show detailed information for specific execution ID')
    parser.add_argument('--limit', type=int, default=10, 
                       help='Number of recent executions to show (default: 10)')
    parser.add_argument('--tables-only', action='store_true',
                       help='Show only current table statistics')
    
    args = parser.parse_args()
    
    print("🚀 Test Data Execution Log Viewer")
    print("=" * 50)
    
    connection = connect_database()
    if not connection:
        return
    
    try:
        if args.tables_only:
            show_current_table_counts(connection)
        elif args.details:
            show_execution_details(connection, args.details)
        else:
            show_recent_executions(connection, args.limit)
            print(f"\n💡 Use --details <ID> to see detailed information for a specific execution")
            print(f"💡 Use --tables-only to see current table row counts")
            
        # Always show current table counts at the end (unless showing details)
        if not args.details:
            print()
            show_current_table_counts(connection)
            
    finally:
        connection.close()

if __name__ == "__main__":
    main()