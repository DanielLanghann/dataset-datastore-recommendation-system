def debug_analytics_tables(self):
    """Debug method to check analytics tables and recent inserts"""
    try:
        cursor = self.pg_connection.cursor()
        
        # Check if analytics tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('analytics_runs', 'analytics_query_results')
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print(f"📋 Analytics tables found: {[t[0] for t in tables]}")
        
        # Check analytics runs
        cursor.execute("SELECT COUNT(*) FROM Analytics_Runs")
        runs_count = cursor.fetchone()[0]
        print(f"📊 Analytics runs in database: {runs_count}")
        
        if self.analytics_run_id:
            cursor.execute("SELECT COUNT(*) FROM Analytics_Query_Results WHERE run_id = %s", 
                         (self.analytics_run_id,))
            results_count = cursor.fetchone()[0]
            print(f"📈 Query results for run {self.analytics_run_id}: {results_count}")
        
        # Check recent analytics runs
        cursor.execute("""
            SELECT run_id, export_timestamp, total_queries_executed, successful_queries 
            FROM Analytics_Runs 
            ORDER BY export_timestamp DESC 
            LIMIT 3
        """)
        recent_runs = cursor.fetchall()
        print(f"🕒 Recent analytics runs:")
        for run in recent_runs:
            print(f"   Run {run[0]}: {run[1]} - {run[3]}/{run[2]} successful")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"❌ Debug query failed: {e}")

def test_simple_insert(self):
    """Test a simple insert to Analytics_Query_Results"""
    if not self.analytics_run_id:
        print("⚠️  No analytics run ID for testing")
        return False
    
    try:
        cursor = self.pg_connection.cursor()
        
        # Test insert with minimal data
        cursor.execute("""
            INSERT INTO Analytics_Query_Results (
                run_id, query_name, query_description, 
                response_time_ms, rows_returned, columns_returned,
                has_data, system
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING result_id
        """, (
            self.analytics_run_id,
            'test_query',
            'Test query for debugging',
            100.0,
            5,
            3,
            True,
            'postgres'
        ))
        
        result_id = cursor.fetchone()[0]
        self.pg_connection.commit()
        cursor.close()
        
        print(f"✅ Test insert successful, result_id: {result_id}")
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Test insert failed: {e}")
        self.pg_connection.rollback()
        return False