# test_ollama_integration.py
# Run this script from Django shell: python manage.py shell < test_ollama_integration.py

import os
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ddrs_api.settings')
django.setup()

from matching_engine.services.core_matching_service import CoreMatchingService
from dataset_api.models import DatasetBaseModel
from datastore_api.models import Datastore
from matching_engine.models import MatchingRequests, MatchingResponse

def create_sample_data():
    """Create sample datasets and datastores for testing"""
    
    print("Creating sample datastores...")
    
    # Create sample datastores
    postgres_store, created = Datastore.objects.get_or_create(
        name="Main PostgreSQL",
        defaults={
            'type': 'sql',
            'system': 'postgres',
            'description': 'Main PostgreSQL database for transactional workloads',
            'is_active': True,
            'max_connections': 100,
            'avg_response_time_ms': 50.0,
            'storage_capacity_gb': 1000.0
        }
    )
    if created:
        print(f"✓ Created datastore: {postgres_store.name}")
    
    mongodb_store, created = Datastore.objects.get_or_create(
        name="MongoDB Cluster",
        defaults={
            'type': 'document',
            'system': 'mongodb',
            'description': 'MongoDB cluster for semi-structured data',
            'is_active': True,
            'max_connections': 200,
            'avg_response_time_ms': 75.0,
            'storage_capacity_gb': 500.0
        }
    )
    if created:
        print(f"✓ Created datastore: {mongodb_store.name}")
    
    redis_store, created = Datastore.objects.get_or_create(
        name="Redis Cache",
        defaults={
            'type': 'keyvalue',
            'system': 'redis',
            'description': 'Redis for caching and session storage',
            'is_active': True,
            'max_connections': 1000,
            'avg_response_time_ms': 5.0,
            'storage_capacity_gb': 50.0
        }
    )
    if created:
        print(f"✓ Created datastore: {redis_store.name}")
    
    print("Creating sample datasets...")
    
    # Create sample datasets
    user_data, created = DatasetBaseModel.objects.get_or_create(
        name="User Analytics Data",
        defaults={
            'short_description': 'User behavior and analytics data from web application',
            'current_datastore': postgres_store,
            'data_structure': 'structured',
            'growth_rate': 'high',
            'access_patterns': 'read_heavy',
            'query_complexity': 'medium',
            'properties': ['user_id', 'session_id', 'page_views', 'timestamp', 'device_type'],
            'estimated_size_gb': 150.0,
            'avg_query_time_ms': 200.0,
            'queries_per_day': 5000.0
        }
    )
    if created:
        print(f"✓ Created dataset: {user_data.name}")
    
    product_catalog, created = DatasetBaseModel.objects.get_or_create(
        name="Product Catalog",
        defaults={
            'short_description': 'E-commerce product catalog with descriptions and metadata',
            'current_datastore': mongodb_store,
            'data_structure': 'semi_structured',
            'growth_rate': 'medium',
            'access_patterns': 'read_write_heavy',
            'query_complexity': 'low',
            'properties': ['product_id', 'name', 'description', 'categories', 'attributes'],
            'estimated_size_gb': 25.0,
            'avg_query_time_ms': 100.0,
            'queries_per_day': 2000.0
        }
    )
    if created:
        print(f"✓ Created dataset: {product_catalog.name}")
    
    session_data, created = DatasetBaseModel.objects.get_or_create(
        name="Session Cache",
        defaults={
            'short_description': 'User session data for fast access',
            'current_datastore': redis_store,
            'data_structure': 'structured',
            'growth_rate': 'high',
            'access_patterns': 'read_write_heavy',
            'query_complexity': 'low',
            'properties': ['session_id', 'user_id', 'data', 'expires_at'],
            'estimated_size_gb': 5.0,
            'avg_query_time_ms': 10.0,
            'queries_per_day': 10000.0
        }
    )
    if created:
        print(f"✓ Created dataset: {session_data.name}")

def test_ollama_integration():
    """Test the Ollama integration service"""
    
    print("=== Testing Ollama Integration Service ===")
    
    # Initialize service
    service = CoreMatchingService()
    
    # Check if we have data to work with
    datasets = DatasetBaseModel.objects.all()
    datastores = Datastore.objects.filter(is_active=True)
    
    print(f"Available datasets: {datasets.count()}")
    print(f"Available datastores: {datastores.count()}")
    
    if datasets.count() == 0 or datastores.count() == 0:
        print("Creating sample data...")
        create_sample_data()
        
        # Refresh counts
        datasets = DatasetBaseModel.objects.all()
        datastores = Datastore.objects.filter(is_active=True)
        print(f"After creation - Datasets: {datasets.count()}, Datastores: {datastores.count()}")
    
    if datasets.count() == 0 or datastores.count() == 0:
        print("❌ Still no datasets or datastores available. Cannot continue test.")
        return
    
    # Test 1: Check Ollama connectivity
    print("\n1. Testing Ollama connectivity...")
    from matching_engine.services.ollama_model_validator_service import OllamaModelValidator
    
    health = OllamaModelValidator.health_check()
    print(f"Ollama status: {health['status']}")
    
    if health['status'] != 'healthy':
        print("❌ Ollama is not available. Make sure Ollama is running.")
        return
    
    models = OllamaModelValidator.get_available_models()
    print(f"Available models: {models}")
    
    if not models:
        print("❌ No models available in Ollama. Please pull some models first.")
        print("Example: ollama pull qwen2.5:8b")
        return
    
    # Test 2: Create and process a simple request
    print("\n2. Testing create_and_process method...")
    
    try:
        # Prepare sample data
        sample_datasets = []
        for dataset in datasets[:4]:  # Use first 2 datasets
            sample_datasets.append({
                'id': dataset.id,
                'name': dataset.name,
                'data_structure': dataset.data_structure,
                'growth_rate': dataset.growth_rate,
                'access_patterns': dataset.access_patterns,
                'estimated_size_gb': dataset.estimated_size_gb or 0
            })
        
        sample_datastores = []
        for datastore in datastores[:4]:  # Use first 2 datastores
            sample_datastores.append({
                'id': datastore.id,
                'name': datastore.name,
                'type': datastore.type,
                'system': datastore.system,
                'storage_capacity_gb': datastore.storage_capacity_gb or 0
            })
        
        print(f"Processing {len(sample_datasets)} datasets with {len(sample_datastores)} datastores...")
        
        # Use the first available model
        model_to_use = models[1]
        print(f"Using model: {model_to_use}")
        
        # Create and process request
        request_obj, response_obj = service.create_request_and_process(
            datasets_data=sample_datasets,
            datastores_data=sample_datastores,
            model=model_to_use,
            description="Test integration with Ollama"
        )
        
        print(f"✓ Created request ID: {request_obj.id}")
        print(f"✓ Created response ID: {response_obj.id}")
        print(f"✓ Model used: {response_obj.model}")
        print(f"✓ Datasets in response: {response_obj.get_datasets_count()}")
        print(f"✓ Datastores in response: {response_obj.get_datastores_count()}")
        
        # Show the results
        print("\n--- Response Results ---")
        if 'recommendations' in response_obj.result:
            for i, rec in enumerate(response_obj.result['recommendations'], 1):
                print(f"{i}. Dataset {rec.get('dataset_id')} -> Datastore {rec.get('datastore_id')}")
                print(f"   Confidence: {rec.get('confidence', 'N/A')}")
                print(f"   Reason: {rec.get('reason', 'N/A')[:100]}...")
                print()
        else:
            print("Raw response:")
            print(response_obj.result)
        
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 3: Process an existing request
    print("\n3. Testing process_matching_request method...")
    
    try:
        # Create a simple request first
        simple_request = MatchingRequests.objects.create(
            related_datasets={'datasets': sample_datasets},
            related_datastores={'datastores': sample_datastores},
            system_prompt="You are a database expert. Recommend the best datastore for each dataset.",
            prompt="Please recommend optimal datastores based on the data provided.",
            requested_model=model_to_use,
            description="Simple test request"
        )
        
        print(f"Created test request ID: {simple_request.id}")
        
        # Process it
        response_obj = service.processing_matching_request(simple_request.id)
        
        print(f"✓ Processed request and created response ID: {response_obj.id}")
        print(f"✓ Response contains {response_obj.get_datasets_count()} dataset recommendations")
        
    except Exception as e:
        print(f"❌ Error processing existing request: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n=== Test Complete ===")
    print("✓ Ollama integration is working correctly!")
    print(f"✓ Total requests in system: {MatchingRequests.objects.count()}")
    print(f"✓ Total responses in system: {MatchingResponse.objects.count()}")

if __name__ == "__main__":
    test_ollama_integration()