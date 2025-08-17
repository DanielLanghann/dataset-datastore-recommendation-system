"""
PostgreSQL Query Set for Database Analytics
Pure PostgreSQL implementation with database storage focus
"""

# Configuration for query execution
QUERY_CONFIG = {
    'display_limit': 5,
    'sample_data_limit': 3,
    'export_filename_template': 'analytics_results_{timestamp}',
    'timestamp_format': '%Y%m%d_%H%M%S'
}

# PostgreSQL Queries Only
ALL_QUERIES = {
    "product_associations": {
        "description": "Products frequently bought together - association analysis",
        "dataset_reference": "dataset 13",
        "database": "postgresql",
        "sql": """
        SELECT
            pa1.product_name as product_a,
            pa1.brand as brand_a,
            cat1.category_name as category_a,
            pa2.product_name as product_b,
            pa2.brand as brand_b,
            cat2.category_name as category_b,
            pa.frequency_count,
            pa.last_calculated
        FROM Product_Associations pa
        JOIN Products pa1 ON pa.product_a_id = pa1.product_id
        JOIN Products pa2 ON pa.product_b_id = pa2.product_id
        JOIN Categories cat1 ON pa1.category_id = cat1.category_id
        JOIN Categories cat2 ON pa2.category_id = cat2.category_id
        ORDER BY pa.frequency_count DESC;
        """
    },
    
}

def get_all_queries():
    """Return all available queries"""
    return ALL_QUERIES

def get_postgresql_queries():
    """Return all PostgreSQL queries"""
    return ALL_QUERIES

def get_query_list():
    """Return list of all query names"""
    return list(ALL_QUERIES.keys())

def get_query(query_name):
    """Get a specific query by name"""
    return ALL_QUERIES.get(query_name)

def get_queries_by_database(database_type):
    """Get queries filtered by database type"""
    if database_type == 'postgresql':
        return ALL_QUERIES
    return {}

def get_query_info(query_name):
    """Get query information without the SQL"""
    query = get_query(query_name)
    if query:
        return {
            'name': query_name,
            'description': query['description'],
            'dataset_reference': query['dataset_reference'],
            'database': query.get('database', 'postgresql')
        }
    return None

def validate_queries():
    """Validate that all queries have required fields"""
    required_fields = ['description', 'dataset_reference', 'database', 'sql']
    
    issues = []
    
    for name, query in ALL_QUERIES.items():
        missing = [field for field in required_fields if field not in query]
        
        if missing:
            issues.append(f"Query '{name}': Missing fields {missing}")
    
    return issues

def get_dataset_coverage():
    """Show which datasets are covered by queries"""
    datasets = {}
    for name, query in ALL_QUERIES.items():
        dataset = query.get('dataset_reference', 'unknown')
        if dataset not in datasets:
            datasets[dataset] = []
        datasets[dataset].append(name)
    
    return datasets

# Testing and validation
if __name__ == "__main__":
    print("🧪 Testing PostgreSQL Queries Module")
    print("=" * 50)
    
    # Validate all queries
    issues = validate_queries()
    if issues:
        print("❌ Query validation issues found:")
        for issue in issues:
            print(f"   • {issue}")
    else:
        print("✅ All queries validated successfully")
    
    # Show summary
    print(f"\n📋 Available PostgreSQL Queries ({len(ALL_QUERIES)}):")
    for query_name, query in ALL_QUERIES.items():
        print(f"   • {query_name}: {query['description']}")
    
    # Show dataset coverage
    print(f"\n📊 Dataset Coverage:")
    coverage = get_dataset_coverage()
    for dataset, queries in coverage.items():
        print(f"   {dataset}: {len(queries)} queries")
    
    print(f"\n📈 Total Statistics:")
    print(f"   Total Queries: {len(ALL_QUERIES)}")
    print(f"   Datasets: {len(coverage)}")
    
    print("\n💡 Module ready for use with perform_queries.py")