"""
Neo4j Cypher Query Set for Graph Database Analytics
Pure Cypher implementation for product association analysis
"""

# Configuration for query execution
QUERY_CONFIG = {
    'display_limit': 5,
    'sample_data_limit': 3,
    'export_filename_template': 'cypher_analytics_results_{timestamp}',
    'timestamp_format': '%Y%m%d_%H%M%S'
}

# Neo4j Cypher Queries
ALL_QUERIES = {
    "product_associations": {
        "description": "Products frequently bought together - graph-based association analysis using Neo4j",
        "dataset_reference": "dataset 13",
        "database": "neo4j",
        "cypher": """
        MATCH (p1:Product)-[r:BOUGHT_TOGETHER]->(p2:Product)
        OPTIONAL MATCH (p1)-[:BELONGS_TO]->(c1:Category)
        OPTIONAL MATCH (p2)-[:BELONGS_TO]->(c2:Category)
        RETURN p1.product_name as product_a,
               p1.brand as brand_a,
               c1.category_name as category_a,
               p2.product_name as product_b,
               p2.brand as brand_b,
               c2.category_name as category_b,
               r.frequency_count as frequency_count
        ORDER BY r.frequency_count DESC;
        """
    }
}

def get_all_queries():
    """Return all available Cypher queries"""
    return ALL_QUERIES

def get_neo4j_queries():
    """Return all Neo4j Cypher queries"""
    return ALL_QUERIES

def get_query_list():
    """Return list of all query names"""
    return list(ALL_QUERIES.keys())

def get_query(query_name):
    """Get a specific query by name"""
    return ALL_QUERIES.get(query_name)

def get_queries_by_database(database_type):
    """Get queries filtered by database type"""
    if database_type == 'neo4j':
        return ALL_QUERIES
    return {}

def get_query_info(query_name):
    """Get query information without the Cypher code"""
    query = get_query(query_name)
    if query:
        return {
            'name': query_name,
            'description': query['description'],
            'dataset_reference': query['dataset_reference'],
            'database': query.get('database', 'neo4j')
        }
    return None

def validate_queries():
    """Validate that all queries have required fields"""
    required_fields = ['description', 'dataset_reference', 'database', 'cypher']
    
    issues = []
    
    for name, query in ALL_QUERIES.items():
        missing = [field for field in required_fields if field not in query]
        
        if missing:
            issues.append(f"Query '{name}': Missing fields {missing}")
            
        # Validate database field
        if query.get('database') != 'neo4j':
            issues.append(f"Query '{name}': Database should be 'neo4j', got '{query.get('database')}'")
    
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

def get_cypher_patterns():
    """Analyze Cypher patterns used in queries"""
    patterns = {
        'node_patterns': set(),
        'relationship_patterns': set(),
        'return_patterns': set()
    }
    
    for name, query in ALL_QUERIES.items():
        cypher = query.get('cypher', '')
        
        # Simple pattern detection (could be enhanced with proper parsing)
        if '(p:Product)' in cypher:
            patterns['node_patterns'].add('Product')
        if '(c:Category)' in cypher:
            patterns['node_patterns'].add('Category')
        if 'BOUGHT_TOGETHER' in cypher:
            patterns['relationship_patterns'].add('BOUGHT_TOGETHER')
        if 'BELONGS_TO' in cypher:
            patterns['relationship_patterns'].add('BELONGS_TO')
    
    return patterns

# Testing and validation
if __name__ == "__main__":
    print("🧪 Testing Neo4j Cypher Queries Module")
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
    print(f"\n📋 Available Neo4j Cypher Queries ({len(ALL_QUERIES)}):")
    for query_name, query in ALL_QUERIES.items():
        print(f"   • {query_name}: {query['description']}")
    
    # Show dataset coverage
    print(f"\n📊 Dataset Coverage:")
    coverage = get_dataset_coverage()
    for dataset, queries in coverage.items():
        print(f"   {dataset}: {len(queries)} queries")
    
    # Show Cypher patterns
    print(f"\n🔍 Cypher Patterns Analysis:")
    patterns = get_cypher_patterns()
    print(f"   Node Types: {', '.join(patterns['node_patterns'])}")
    print(f"   Relationships: {', '.join(patterns['relationship_patterns'])}")
    
    print(f"\n📈 Total Statistics:")
    print(f"   Total Queries: {len(ALL_QUERIES)}")
    print(f"   Datasets: {len(coverage)}")
    print(f"   Node Types: {len(patterns['node_patterns'])}")
    print(f"   Relationship Types: {len(patterns['relationship_patterns'])}")
    
    print("\n💡 Module ready for use with perform_cypher_queries.py")