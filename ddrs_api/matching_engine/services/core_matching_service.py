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
    Bring Request, Ollama and Responses together
    """

    def __init__(self):
        self.ollama_url = getattr(settings, 'OLLAMA_API_URL', 'http://localhost:11434')
        self.default_timeout = config("OLLAMA_REQUEST_TIMEOUT", default=300, cast=int)  # 5 minutes default

    
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

            # Gather dataset and datastore information
            datasets_data = self._gather_datasets_data(
                matching_request.related_datasets, 
                dataset_ids
            )

            datastores_data = self._gather_datastore_data(
                matching_request.related_datastores, 
                datastore_ids
            )

            # Build the prompt
            full_prompt = self._build_prompt(
                datasets_data,
                datastores_data,
                matching_request.system_prompt,
                matching_request.prompt
            )

            # Call Ollama API
            ollama_response = self._call_ollama(
                full_prompt,
                matching_request.requested_model
            )

            # Parse the response
            parsed_results = self._parse_ollama_response(ollama_response)

            # Create and save the response
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

    def _gather_datasets_data(self, related_datasets: Dict, dataset_ids: List[int] = None) -> List[Dict]:
        """Gather comprehensive dataset information"""
        datasets_data = []

         # extract dataset IDs from the request
        if isinstance(related_datasets, list):
            request_dataset_ids = [d.get('id') for d in related_datasets if d.get('id')]
        else:
            request_dataset_ids = [d.get('id') for d in related_datasets.get('datasets', []) if d.get('id')]

        # Filter if specific dataset_ids provided
        if dataset_ids:
            request_dataset_ids = [id for id in request_dataset_ids if id in dataset_ids]

        # Fetch detailed information from database
        for dataset_id in request_dataset_ids:
            try:
                dataset = DatasetBaseModel.objects.prefetch_related(
                    'queries', 'relationships_from', 'relationships_to'
                ).get(id=dataset_id)
                
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
                    'queries': [
                        {
                            'id': q.id,
                            'name': q.name,
                            'query_type': q.query_type,
                            'frequency': q.frequency,
                            'avg_execution_time_ms': q.avg_execution_time_ms,
                            'description': q.description
                        } for q in dataset.queries.all()
                    ],
                    'relationships': [
                        {
                            'type': rel.relationship_type,
                            'strength': rel.strength,
                            'target_dataset_id': rel.to_dataset.id if rel.to_dataset else None,
                            'description': rel.description
                        } for rel in dataset.relationships_from.filter(is_active=True)
                    ]
                }
                datasets_data.append(dataset_info)
                
            except DatasetBaseModel.DoesNotExist:
                logger.warning(f"Dataset {dataset_id} not found in database")
                continue
        
        return datasets_data
    
    def _gather_datastore_data(self, related_datastores: Dict, datastore_ids: List[int] = None) -> List[Dict]:
        """Gather comprehensive datastore information"""
        datastores_data = []

        # Extract datastore IDs from the request
        if isinstance(related_datastores, list):
            request_datastore_ids = [d.get('id') for d in related_datastores if d.get('id')]
        else:
            request_datastore_ids = [d.get('id') for d in related_datastores.get('datastores', []) if d.get('id')]
        
        # Filter if specific datastore_ids provided
        if datastore_ids:
            request_datastore_ids = [id for id in request_datastore_ids if id in datastore_ids]
        
         # Fetch detailed information from database
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
        """Build the complete prompt for Ollama"""
        
        # Create structured data representation
        datasets_summary = []
        for dataset in datasets_data:
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
                Current Datastore: {dataset['current_datastore']}
                Number of Queries: {len(dataset['queries'])}
                Number of Relationships: {len(dataset['relationships'])}
            """.strip()
            datasets_summary.append(summary)
        
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
            """.strip()
            datastores_summary.append(summary)
        
        # Build the complete prompt
        complete_prompt = f"""
            {system_prompt}

            {user_prompt}

            DATASETS TO MATCH:
            {chr(10).join(datasets_summary)}

            AVAILABLE DATASTORES:
            {chr(10).join(datastores_summary)}

                Please provide your response in the following JSON format:
                {{
                    "recommendations": [
                        {{
                            "dataset_id": <dataset_id>,
                            "datastore_id": <recommended_datastore_id>,
                            "reason": "<detailed_explanation>",
                            "confidence": <score_between_0_and_1>
                        }}
                    ]
                }}

                Ensure that:
                1. Every dataset gets a recommendation
                2. Confidence scores are realistic (0.0 to 1.0)
                3. Reasons are detailed and technical
                4. Response is valid JSON
        """.strip()
        
        return complete_prompt
    
    def _call_ollama(self, prompt: str, model: str) -> str:
        """Make API call to Ollama"""
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": config("TEMPERATURE", default=0.1, cast=float),  # Low temperature for consistent responses
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
        """Parse Ollama response and extract structured data"""
        try:
            # Try to find JSON in the response
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            reason_lenght = config("MAX_REASON_LENGTH", 500, cast=int) # limit the reason to 500
            
            if start_idx != -1 and end_idx != -1:
                json_str = response_text[start_idx:end_idx]
                parsed_response = json.loads(json_str)
                
                # Validate the structure
                if 'recommendations' in parsed_response:
                    recommendations = parsed_response['recommendations']
                    
                    # Validate each recommendation
                    validated_recommendations = []
                    for rec in recommendations:
                        if all(key in rec for key in ['dataset_id', 'datastore_id', 'reason', 'confidence']):
                            # Ensure confidence is within valid range
                            confidence = max(0.0, min(1.0, float(rec['confidence'])))
                            
                            validated_recommendations.append({
                                'dataset_id': int(rec['dataset_id']),
                                'datastore_id': int(rec['datastore_id']),
                                'reason': str(rec['reason'])[:reason_lenght],  
                                'confidence': confidence
                            })
                    
                    return {'recommendations': validated_recommendations}
            
            # If JSON parsing fails, create a fallback response
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
        # Use defaults if not provided
        if not system_prompt:
            system_prompt = "You are an expert database architect with deep knowledge of different database systems, their strengths, limitations, and optimal use cases. Analyze the provided datasets and datastores to make informed recommendations."
        
        if not user_prompt:
            user_prompt = "Based on the dataset characteristics (structure, size, growth rate, access patterns, query complexity) and available datastores (type, system, performance, capacity), recommend the optimal datastore for each dataset. Provide clear reasoning and confidence scores."
        
        try:
            with transaction.atomic():
                # Create the request
                request_obj = MatchingRequests.objects.create(
                    related_datasets={'datasets': datasets_data},
                    related_datastores={'datastores': datastores_data},
                    system_prompt=system_prompt,
                    prompt=user_prompt,
                    requested_model=model,
                    description=description or f"Auto-generated request with {len(datasets_data)} datasets and {len(datastores_data)} datastores"
                )
                
                # Process the request
                response_obj = self.processing_matching_request(request_obj.id)
                logger.debug("Successfully created response!")
                return request_obj, response_obj
                
        except Exception as e:
            logger.error(f"Error in create_request_and_process: {str(e)}")
            raise


        

    





        




        



        











