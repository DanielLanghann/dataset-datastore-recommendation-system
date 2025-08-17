#!/usr/bin/env python3
"""
Test script for the optimized testdata_loop with smaller targets for testing
"""
import sys
from testdata_loop import ProgressivePerformanceTester, load_environment

def main():
    """Test the optimized loop with smaller data sizes"""
    
    print("🧪 Testing Optimized Progressive Performance Loop")
    print("=" * 60)
    
    # Load database configuration
    db_config = load_environment()
    
    # Create tester with modified smaller targets for testing
    tester = ProgressivePerformanceTester(db_config)
    
    # Override settings for testing
    tester.initial_data = {
        'categories': 100,
        'customers': 200,
        'products': 500,
        'orders': 1000,
        'order_items': 2000
    }
    
    tester.growth_multipliers = {
        'categories': 1.5,
        'customers': 2.0,
        'products': 2.0,
        'orders': 3.0,
        'order_items': 3.0
    }
    
    tester.max_orders = 50000  # Much smaller for testing
    tester.skip_associations_threshold = 10000  # Lower threshold for testing
    tester.current_data = tester.initial_data.copy()
    
    print(f"📊 Test configuration:")
    print(f"   Initial data: {tester.initial_data}")
    print(f"   Growth multipliers: {tester.growth_multipliers}")
    print(f"   Max orders: {tester.max_orders:,}")
    print(f"   Skip associations threshold: {tester.skip_associations_threshold:,}")
    
    # Test setup first
    if not tester.test_setup():
        print("\n❌ Setup tests failed. Exiting.")
        return False
    
    # Run a few iterations
    print(f"\n🚀 Starting test iterations...")
    success = False
    try:
        # Connect to database
        if not tester.connect():
            print("❌ Failed to connect to database")
            return False
            
        # Run just 2-3 iterations for testing
        for i in range(3):
            print(f"\n{'='*60}")
            print(f"🔄 TEST ITERATION {i+1}")
            print(f"{'='*60}")
            
            # Get current state
            initial_counts = tester.get_current_row_counts()
            print(f"📊 Current state: {initial_counts}")
            
            # Calculate targets
            rows_to_add = tester.calculate_rows_to_add(initial_counts)
            print(f"📈 Rows to add: {rows_to_add}")
            
            # Test data generation
            print(f"\n1️⃣ Testing data generation...")
            data_success = tester.run_data_generation(rows_to_add)
            print(f"   Result: {'✅' if data_success else '❌'}")
            
            # Test associations update
            print(f"\n2️⃣ Testing associations update...")
            assoc_success, assoc_duration = tester.update_product_associations()
            print(f"   Result: {'✅' if assoc_success else '❌'} ({assoc_duration:.1f}s)")
            
            # Test query execution
            print(f"\n3️⃣ Testing query execution...")
            query_success, query_duration, query_results = tester.run_performance_queries()
            print(f"   Result: {'✅' if query_success else '❌'} ({query_duration:.1f}s)")
            
            # Get final state
            final_counts = tester.get_current_row_counts()
            print(f"📊 Final state: {final_counts}")
            
            # Check if we should continue
            if not tester.check_continue_condition(final_counts):
                print(f"\n🛑 Stopping test - reached limits")
                break
                
            # Update targets for next iteration
            tester.update_data_targets_for_next_iteration()
            
        success = True
        print(f"\n🎉 Test completed successfully!")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if tester.connection:
            tester.disconnect()
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
