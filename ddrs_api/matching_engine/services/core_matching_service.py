import json
import logging
import requests
from decouple import config
from typing import Dict, List, Tuple
from django.conf import settings
from django.db import transaction
from dataclasses import dataclass

from ..models import MatchingRequests, MatchingResponse
from dataset_api.models import DatasetBaseModel
from datastore_api.models import Datastore

logger = logging.getLogger(__name__)

@dataclass
class MatchingResult:
    dataset_id: int
    datastore_id: int
    reason: str
    confidence: float

class CoreMatchingService:
    """
    Enhanced service that includes dataset queries and relationships in AI prompts
    """

    def __init__(self):
        self.ollama_url = getattr(settings, 'OLLAMA_API_URL', 'http://localhost:11434')
        self.default_timeout = config("OLLAMA_REQUEST_TIMEOUT", default=300, cast=int)

    def processing_matching_request(
            self, 
            request_id: int,
            dataset_ids: List[int] = None,
            datastore_ids: List[int] = None,
    ) -> MatchingResponse:
        """
        Main method to process a matching request through Ollama
        Args:
            request_id: ID of the MatchingRequest
            dataset_ids: Optional list of specific dataset IDs to process
            datastore_ids: Optional list of specific datastore IDs to consider
            
        Returns:
            MatchingResponse: The created response object
        """
        try:
            matching_request = MatchingRequests.objects.get(id=request_id)

            # gather comprehensive dataset and datastore information
            datasets_data = self._gather_datasets_data_with_relationships(
                matching_request.related_datasets, 
                dataset_ids
            )

            datastores_data = self._gather_datastore_data(
                matching_request.related_datastores, 
                datastore_ids
            )

            # build the enhanced prompt with queries and dependencies
            full_prompt = self._build_prompt(
                datasets_data,
                datastores_data,
                matching_request.system_prompt,
                matching_request.prompt
            )

            # Call Ollama
            ollama_response = self._call_ollama(
                full_prompt,
                matching_request.requested_model
            )

            # parse response
            parsed_results = self._parse_ollama_response(ollama_response)

            # create and save
            with transaction.atomic():
                response = MatchingResponse.objects.create(
                    request_id=matching_request,
                    result=parsed_results,
                    model=matching_request.requested_model,
                    description=f"AI matching response for request {request_id}"
                )
            
            logger.info(f"Successfully processed matching request {request_id}")
            return response
        
        except Exception as e:
            logger.error(f"Error processing matching request {request_id}: {str(e)}")
            raise

    def _gather_datasets_data_with_relationships(self, related_datasets: Dict, dataset_ids: List[int] = None) -> List[Dict]:
        """Gather comprehensive dataset information including queries and relationships"""
        datasets_data = []

        # extract dataset IDs from the request
        if isinstance(related_datasets, list):
            request_dataset_ids = [d.get('id') for d in related_datasets if d.get('id')]
        else:
            request_dataset_ids = [d.get('id') for d in related_datasets.get('datasets', []) if d.get('id')]

        # filter if specific dataset_ids provided
        if dataset_ids:
            request_dataset_ids = [id for id in request_dataset_ids if id in dataset_ids]

        # fetch detailed information from database with all related data
        datasets_queryset = DatasetBaseModel.objects.prefetch_related(
            'queries', 
            'relationships_from', 
            'relationships_to',
            'relationships_from__to_dataset',
            'relationships_to__from_dataset'
        ).filter(id__in=request_dataset_ids)

        for dataset in datasets_queryset:
            try:
                # gather query information
                queries_info = []
                for query in dataset.queries.all():
                    query_info = {
                        'id': query.id,
                        'name': query.name,
                        'query_type': query.query_type,
                        'frequency': query.frequency,
                        'avg_execution_time_ms': query.avg_execution_time_ms,
                        'description': query.description,
                        'query_text': query.query_text[:500] if query.query_text else None  # Truncate long queries
                    }
                    queries_info.append(query_info)

                # gather relationship information (outgoing)
                outgoing_relationships = []
                for rel in dataset.relationships_from.filter(is_active=True):
                    rel_info = {
                        'type': rel.relationship_type,
                        'strength': rel.strength,
                        'target_dataset_id': rel.to_dataset.id if rel.to_dataset else None,
                        'target_dataset_name': rel.to_dataset.name if rel.to_dataset else None,
                        'description': rel.description,
                        'direction': 'outgoing'
                    }
                    outgoing_relationships.append(rel_info)

                # gather relationship information (incoming)
                incoming_relationships = []
                for rel in dataset.relationships_to.filter(is_active=True):
                    rel_info = {
                        'type': rel.relationship_type,
                        'strength': rel.strength,
                        'source_dataset_id': rel.from_dataset.id if rel.from_dataset else None,
                        'source_dataset_name': rel.from_dataset.name if rel.from_dataset else None,
                        'description': rel.description,
                        'direction': 'incoming'
                    }
                    incoming_relationships.append(rel_info)

                # combine all dataset information
                dataset_info = {
                    'id': dataset.id,
                    'name': dataset.name,
                    'description': dataset.short_description,
                    'data_structure': dataset.data_structure,
                    'growth_rate': dataset.growth_rate,
                    'access_patterns': dataset.access_patterns,
                    'query_complexity': dataset.query_complexity,
                    'estimated_size_gb': dataset.estimated_size_gb,
                    'avg_query_time_ms': dataset.avg_query_time_ms,
                    'queries_per_day': dataset.queries_per_day,
                    'properties': dataset.properties,
                    'sample_data': dataset.sample_data,
                    'current_datastore': dataset.current_datastore.id if dataset.current_datastore else None,
                    'current_datastore_name': dataset.current_datastore.name if dataset.current_datastore else None,
                    
                    # include queries and relationships
                    'queries': queries_info,
                    'outgoing_relationships': outgoing_relationships,
                    'incoming_relationships': incoming_relationships,
                    
                    # Summary-stats for LLM
                    'query_count': len(queries_info),
                    'relationship_count': len(outgoing_relationships) + len(incoming_relationships),
                    'has_foreign_keys': any(rel['type'] == 'foreign_key' for rel in outgoing_relationships + incoming_relationships),
                    'has_dependencies': any(rel['type'] == 'dependency' for rel in outgoing_relationships + incoming_relationships)
                }
                datasets_data.append(dataset_info)
                
            except Exception as e:
                logger.warning(f"Error gathering data for dataset {dataset.id}: {str(e)}")
                continue
        
        return datasets_data
    
    def _gather_datastore_data(self, related_datastores: Dict, datastore_ids: List[int] = None) -> List[Dict]:
        """Gather comprehensive datastore information"""
        datastores_data = []

        # extract datastore IDs from the request
        if isinstance(related_datastores, list):
            request_datastore_ids = [d.get('id') for d in related_datastores if d.get('id')]
        else:
            request_datastore_ids = [d.get('id') for d in related_datastores.get('datastores', []) if d.get('id')]
        
        # filter if specific datastore_ids provided
        if datastore_ids:
            request_datastore_ids = [id for id in request_datastore_ids if id in datastore_ids]
        
        # fetch detailed information from database
        for datastore_id in request_datastore_ids:
            try:
                datastore = Datastore.objects.get(id=datastore_id)
                
                datastore_info = {
                    'id': datastore.id,
                    'name': datastore.name,
                    'type': datastore.type,
                    'system': datastore.system,
                    'description': datastore.description,
                    'is_active': datastore.is_active,
                    'max_connections': datastore.max_connections,
                    'avg_response_time_ms': datastore.avg_response_time_ms,
                    'storage_capacity_gb': datastore.storage_capacity_gb,
                    'characteristics': datastore.characteristics,
                    'connection_info': datastore.get_masked_connection_info()
                }
                datastores_data.append(datastore_info)
            except Datastore.DoesNotExist:
                logger.warning(f"Datastore {datastore_id} not found in database")
                continue
        
        return datastores_data

    def _build_prompt(
        self, 
        datasets_data: List[Dict], 
        datastores_data: List[Dict],
        system_prompt: str,
        user_prompt: str
    ) -> str:
        """Build the complete prompt for Ollama with detailed queries and relationships"""
        
        # build detailed dataset summaries with queries and relationships
        datasets_summary = []
        for dataset in datasets_data:
            # basic dataset information
            summary = f"""
                Dataset ID: {dataset['id']}
                Name: {dataset['name']}
                Description: {dataset['description']}
                Structure: {dataset['data_structure']}
                Size: {dataset['estimated_size_gb']}GB
                Growth Rate: {dataset['growth_rate']}
                Access Patterns: {dataset['access_patterns']}
                Query Complexity: {dataset['query_complexity']}
                Queries per Day: {dataset['queries_per_day']}
                Avg Query Time: {dataset['avg_query_time_ms']}ms
                Current Datastore: {dataset['current_datastore_name'] or 'None'} (ID: {dataset['current_datastore']})""".strip()

            # add queries information
            if dataset['queries']:
                summary += f"\n\nQUERIES ({len(dataset['queries'])} total):"
                for i, query in enumerate(dataset['queries'][:5], 1):  #  limit 
                    summary += f"""
                        Query {i}: {query['name']}
                        Type: {query['query_type']}
                        Frequency: {query['frequency']}
                        Avg Execution Time: {query['avg_execution_time_ms']}ms
                        Description: {query['description']}"""
                    if query['query_text']:
                        summary += f"\n    SQL/Query: {query['query_text'][:200]}..."
                
                if len(dataset['queries']) > 5:
                    summary += f"\n  ... and {len(dataset['queries']) - 5} more queries"
            else:
                summary += f"\n\nQUERIES: None defined"

            # add relationships information
            all_relationships = dataset['outgoing_relationships'] + dataset['incoming_relationships']
            if all_relationships:
                summary += f"\n\nRELATIONSHIPS ({len(all_relationships)} total):"
                for i, rel in enumerate(all_relationships, 1):  # limit
                    if rel['direction'] == 'outgoing':
                        summary += f"""
                            Relationship {i}: {rel['type']} -> Dataset {rel['target_dataset_id']} ({rel['target_dataset_name']})
                            Strength: {rel['strength']}/10
                            Description: {rel['description']}"""
                    else:
                        summary += f"""
                            Relationship {i}: {rel['type']} <- Dataset {rel['source_dataset_id']} ({rel['source_dataset_name']})
                            Strength: {rel['strength']}/10
                            Description: {rel['description']}"""
                
                if len(all_relationships) > 5:
                    summary += f"\n  ... and {len(all_relationships) - 5} more relationships"
            else:
                summary += f"\n\nRELATIONSHIPS: None defined"

            # add analysis flags
            summary += f"\n\nANALYSIS FLAGS:"
            summary += f"\n  - Has Foreign Keys: {dataset['has_foreign_keys']}"
            summary += f"\n  - Has Dependencies: {dataset['has_dependencies']}"
            summary += f"\n  - Query Complexity Level: {dataset['query_complexity']}"
            summary += f"\n  - Relationship Dependencies: {dataset['relationship_count']} total"

            datasets_summary.append(summary)
        
        # build datastore summaries
        datastores_summary = []
        for datastore in datastores_data:
            summary = f"""
                Datastore ID: {datastore['id']}
                Name: {datastore['name']}
                Type: {datastore['type']} ({datastore['system']})
                Description: {datastore['description']}
                Active: {datastore['is_active']}
                Max Connections: {datastore['max_connections']}
                Avg Response Time: {datastore['avg_response_time_ms']}ms
                Storage Capacity: {datastore['storage_capacity_gb']}GB

                CAPABILITIES:
                - Supports: {datastore['type']} operations
                - System: {datastore['system']}
                - Performance: {datastore['avg_response_time_ms']}ms avg response
                - Scalability: {datastore['max_connections']} max connections""".strip()
            
            datastores_summary.append(summary)
        
        # build relationship dependency graph summary
        dependency_summary = self._build_dependency_summary(datasets_data)
        
        # build the complete prompt
        complete_prompt = f"""
                {system_prompt}

                {user_prompt}

                IMPORTANT CONSIDERATIONS FOR MATCHING:
                1. Query Types and Frequency: Consider the types of queries each dataset requires and how often they run
                2. Data Relationships: Account for foreign key relationships and dependencies between datasets
                3. Transaction Requirements: Datasets with dependencies may need ACID compliance
                4. Performance Requirements: Match query complexity and frequency with datastore capabilities
                5. Scalability: Consider growth rates and storage capacity requirements
                6. Data Structure Compatibility: Ensure datastore type matches data structure needs

                DATASET DEPENDENCY ANALYSIS:
                {dependency_summary}

                DATASETS TO MATCH:
                {chr(10).join('=' * 80 + chr(10) + ds for ds in datasets_summary)}

                AVAILABLE DATASTORES:
                {chr(10).join('=' * 80 + chr(10) + ds for ds in datastores_summary)}

                MATCHING REQUIREMENTS:
                - For datasets with foreign key relationships, consider keeping them in the same datastore or ensure cross-datastore referential integrity
                - High-frequency queries need low-latency datastores
                - Complex analytical queries benefit from column-store or analytical databases
                - Transactional datasets with dependencies need ACID-compliant systems
                - Consider data growth rates when matching to storage capacity

                Please provide your response in the following JSON format:
                {{
                    "recommendations": [
                        {{
                            "dataset_id": <dataset_id>,
                            "datastore_id": <recommended_datastore_id>,
                            "reason": "<detailed_explanation_including_query_and_relationship_analysis>",
                            "confidence": <score_between_0_and_1>
                        }}
                    ],
                    "dependency_considerations": [
                        {{
                            "datasets": [<list_of_related_dataset_ids>],
                            "reasoning": "<explanation_of_why_these_should_be_co_located_or_distributed>",
                            "recommendation": "<same_datastore|distributed_with_sync|independent>"
                        }}
                    ]
                }}

                Ensure that:
                1. Every dataset gets a recommendation
                2. Confidence scores reflect query complexity and relationship constraints
                3. Reasons include analysis of queries, relationships, and performance requirements
                4. Dependency considerations address related datasets
                5. Response is valid JSON
                        """.strip()
        
        return complete_prompt

    def _build_dependency_summary(self, datasets_data: List[Dict]) -> str:
        """Build a summary of dataset dependencies for the LLM"""
        dependencies = []
        
        # Collect all foreign key and dependency relationships
        for dataset in datasets_data:
            for rel in dataset['outgoing_relationships']:
                if rel['type'] in ['foreign_key', 'dependency']:
                    dependencies.append({
                        'source': dataset['name'],
                        'source_id': dataset['id'],
                        'target': rel['target_dataset_name'],
                        'target_id': rel['target_dataset_id'],
                        'type': rel['type'],
                        'strength': rel['strength']
                    })
        
        if not dependencies:
            return "No critical dependencies found between datasets."
        
        summary = f"CRITICAL DEPENDENCIES ({len(dependencies)} found):\n"
        for i, dep in enumerate(dependencies, 1):
            summary += f"{i}. {dep['source']} (ID:{dep['source_id']}) --{dep['type']}--> {dep['target']} (ID:{dep['target_id']}) [Strength: {dep['strength']}/10]\n"
        
        # Group related datasets
        groups = {}
        for dep in dependencies:
            key = f"{min(dep['source_id'], dep['target_id'])}_{max(dep['source_id'], dep['target_id'])}"
            if key not in groups:
                groups[key] = set()
            groups[key].add(dep['source_id'])
            groups[key].add(dep['target_id'])
        
        if groups:
            summary += f"\nRELATED DATASET GROUPS:\n"
            for i, (_, group) in enumerate(groups.items(), 1):
                summary += f"Group {i}: Datasets {list(group)} should be considered together\n"
        
        return summary
    
    def _call_ollama(self, prompt: str, model: str) -> str:
        """Make API call to Ollama"""
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": config("TEMPERATURE", default=0.1, cast=float),
                "top_p": config("TOP_P", default=0.9, cast=float),
                "top_k": config("TOP_K", default=40, cast=int)
            }
        }
        
        try:
            logger.info(f"Calling Ollama API with model: {model}")
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=self.default_timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get('response', '')
            
        except requests.RequestException as e:
            logger.error(f"Ollama API call failed: {str(e)}")
            raise RuntimeError(f"Failed to connect to Ollama: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Ollama: {str(e)}")
            raise RuntimeError(f"Invalid response from Ollama: {str(e)}")
        
    def _parse_ollama_response(self, response_text: str) -> Dict:
        """Parse Ollama response and extract structured data with dependency considerations"""
        try:
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            reason_length = config("MAX_REASON_LENGTH", 500, cast=int)
            
            if start_idx != -1 and end_idx != -1:
                json_str = response_text[start_idx:end_idx]
                parsed_response = json.loads(json_str)
                
                # Validate and process recommendations
                if 'recommendations' in parsed_response:
                    recommendations = parsed_response['recommendations']
                    
                    validated_recommendations = []
                    for rec in recommendations:
                        if all(key in rec for key in ['dataset_id', 'datastore_id', 'reason', 'confidence']):
                            confidence = max(0.0, min(1.0, float(rec['confidence'])))
                            
                            validated_recommendations.append({
                                'dataset_id': int(rec['dataset_id']),
                                'datastore_id': int(rec['datastore_id']),
                                'reason': str(rec['reason'])[:reason_length],  
                                'confidence': confidence
                            })
                    
                    result = {'recommendations': validated_recommendations}
                    
                    # Include dependency considerations if present
                    if 'dependency_considerations' in parsed_response:
                        result['dependency_considerations'] = parsed_response['dependency_considerations']
                    
                    return result
            
            # Fallback response
            logger.warning("Could not parse JSON from Ollama response, creating fallback")
            return {
                'recommendations': [],
                'raw_response': response_text,
                'parsing_error': 'Could not extract valid JSON from response'
            }
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Error parsing Ollama response: {str(e)}")
            return {
                'recommendations': [],
                'raw_response': response_text,
                'parsing_error': str(e)
            }
    
    def create_request_and_process(
        self,
        datasets_data: List[Dict],
        datastores_data: List[Dict],
        system_prompt: str = None,
        user_prompt: str = None,
        model: str = "qwen2.5:8b",
        description: str = ""
    ) -> Tuple[MatchingRequests, MatchingResponse]:
        """
        Create a new request and immediately process it
        
        Returns:
            Tuple of (MatchingRequest, MatchingResponse)
        """
        # Enhanced default prompts
        if not system_prompt:
            system_prompt = """You are an expert database architect with deep knowledge of different database systems, their strengths, limitations, and optimal use cases. You specialize in analyzing dataset queries, relationships, and dependencies to make informed datastore recommendations. Consider query patterns, data relationships, performance requirements, and scalability needs."""
        
        if not user_prompt:
            user_prompt = """Analyze each dataset's characteristics including queries, relationships, and dependencies. Recommend the optimal datastore considering:
            1. Query types and frequency patterns
            2. Data relationships and foreign key constraints
            3. Transaction requirements for dependent datasets
            4. Performance needs based on query complexity
            5. Scalability requirements based on growth rates
            6. Data structure compatibility
            
            Provide detailed reasoning that includes query analysis and relationship considerations."""
        
        try:
            with transaction.atomic():
                # Create the request
                request_obj = MatchingRequests.objects.create(
                    related_datasets={'datasets': datasets_data},
                    related_datastores={'datastores': datastores_data},
                    system_prompt=system_prompt,
                    prompt=user_prompt,
                    requested_model=model,
                    description=description or f"Enhanced request with {len(datasets_data)} datasets and {len(datastores_data)} datastores"
                )
                
                # Process the request
                response_obj = self.processing_matching_request(request_obj.id)
                logger.info("Successfully created enhanced response with query and relationship analysis!")
                return request_obj, response_obj
                
        except Exception as e:
            logger.error(f"Error in create_request_and_process: {str(e)}")
            raise