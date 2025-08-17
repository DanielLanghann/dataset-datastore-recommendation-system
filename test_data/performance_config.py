#!/usr/bin/env python3
"""
Configuration script for testdata_loop.py performance settings
Allows tuning of thresholds and optimizations without editing the main script
"""

def get_performance_config():
    """Get performance configuration based on expected data sizes"""
    
    configs = {
        'small': {
            'description': 'Small datasets - good for testing',
            'initial_data': {
                'categories': 500,
                'customers': 1000,
                'products': 2000,
                'orders': 5000,
                'order_items': 10000
            },
            'max_orders': 100000,
            'use_optimized_associations': True,
            'association_timeout_seconds': 600  # 10 minutes
        },
        
        'medium': {
            'description': 'Medium datasets - production-like',
            'initial_data': {
                'categories': 2000,
                'customers': 1000,
                'products': 4000,
                'orders': 8000,
                'order_items': 16000
            },
            'max_orders': 500000,
            'use_optimized_associations': True,
            'association_timeout_seconds': 1800  # 30 minutes
        },
        
        'large': {
            'description': 'Large datasets - stress testing',
            'initial_data': {
                'categories': 5000,
                'customers': 10000,
                'products': 20000,
                'orders': 50000,
                'order_items': 100000
            },
            'max_orders': 2000000,
            'use_optimized_associations': True,
            'association_timeout_seconds': 3600  # 1 hour
        },
        
        'stress': {
            'description': 'Stress test - very large datasets',
            'initial_data': {
                'categories': 10000,
                'customers': 50000,
                'products': 100000,
                'orders': 200000,
                'order_items': 500000
            },
            'max_orders': 10000000,
            'use_optimized_associations': True,
            'association_timeout_seconds': 7200  # 2 hours
        }
    }
    
    return configs

def apply_config(tester, config_name='medium'):
    """Apply a configuration to a ProgressivePerformanceTester instance"""
    configs = get_performance_config()
    
    if config_name not in configs:
        print(f"❌ Unknown configuration: {config_name}")
        print(f"Available configurations: {list(configs.keys())}")
        return False
    
    config = configs[config_name]
    
    print(f"🎛️  Applying '{config_name}' configuration:")
    print(f"   {config['description']}")
    
    # Apply configuration
    tester.initial_data = config['initial_data']
    tester.current_data = config['initial_data'].copy()
    tester.max_orders = config['max_orders']
    tester.use_optimized_associations = config['use_optimized_associations']
    tester.association_timeout_seconds = config['association_timeout_seconds']
    
    print(f"   📊 Initial data: {config['initial_data']}")
    print(f"   🎯 Max orders: {config['max_orders']:,}")
    print(f"   ⚡ Use optimized associations: {config['use_optimized_associations']}")
    print(f"   ⏰ Association timeout: {config['association_timeout_seconds']}s")
    print(f"   🔗 Product associations are ALWAYS computed")
    
    return True

def get_config_recommendations():
    """Get recommendations for configuration based on system resources"""
    import psutil
    
    # Get system information
    cpu_count = psutil.cpu_count()
    memory_gb = round(psutil.virtual_memory().total / (1024**3))
    
    print(f"🖥️  System Information:")
    print(f"   CPU cores: {cpu_count}")
    print(f"   Total memory: {memory_gb} GB")
    
    # Provide recommendations
    if memory_gb >= 16 and cpu_count >= 8:
        recommended = 'large'
        print(f"💡 Recommended configuration: '{recommended}' (High-performance system)")
    elif memory_gb >= 8 and cpu_count >= 4:
        recommended = 'medium'
        print(f"💡 Recommended configuration: '{recommended}' (Standard system)")
    else:
        recommended = 'small'
        print(f"💡 Recommended configuration: '{recommended}' (Limited resources)")
    
    return recommended

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Configure testdata_loop performance settings')
    parser.add_argument('--show-configs', action='store_true', help='Show available configurations')
    parser.add_argument('--recommend', action='store_true', help='Get configuration recommendation')
    
    args = parser.parse_args()
    
    if args.show_configs:
        configs = get_performance_config()
        print("📋 Available Configurations:")
        print("=" * 50)
        for name, config in configs.items():
            print(f"\n🎛️  {name.upper()}:")
            print(f"   {config['description']}")
            print(f"   Max orders: {config['max_orders']:,}")
            print(f"   Optimized associations: {config['use_optimized_associations']}")
            print(f"   Timeout: {config['association_timeout_seconds']}s")
    
    if args.recommend:
        get_config_recommendations()
    
    if not args.show_configs and not args.recommend:
        parser.print_help()
