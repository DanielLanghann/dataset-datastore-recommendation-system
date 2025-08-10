import logging
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from django.shortcuts import get_object_or_404

from ..models import MatchingRequests, MatchingResponse
from ..services.core_matching_service import CoreMatchingService
from ..serializers import (
    RequestSerializer,
    ResponseDetailSerializer,
)
from dataset_api.models import DatasetBaseModel
from datastore_api.models import Datastore

logger = logging.getLogger(__name__)

class MatchingViewSet(viewsets.ViewSet):
    """
    ViewSet for Ollama-powered matching operations
    """
    
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.matching_service = CoreMatchingService()
    
    @action(detail=False, methods=["post"])
    def process_request(self, request):
        """
        Process an existing matching request through Ollama

        Expected payload:
        {
            "request_id": 123,
            "dataset_ids": [1, 2, 3],  # optional
            "datastore_ids": [1, 2]   # optional
        }
        """
        request_id = request.data.get('request_id')
        dataset_ids = request.data.get('dataset_ids')
        datastore_ids = request.data.get('datastore_ids')
        
        if not request_id:
            return Response(
                {"error": "request_id is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Verify request exists
            get_object_or_404(MatchingRequests, id=request_id)
            
            # Process the request
            response_obj = self.matching_service.process_matching_request(
                request_id=request_id,
                dataset_ids=dataset_ids,
                datastore_ids=datastore_ids
            )
            
            # Return the response
            serializer = ResponseDetailSerializer(response_obj)
            return Response({
                "status": "success",
                "message": f"Successfully processed request {request_id}",
                "response": serializer.data
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error processing request {request_id}: {str(e)}")
            return Response(
                {"error": f"Failed to process request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    
    @action(detail=False, methods=["post"])
    def create_and_process(self, request):
        """
        Create a new request and immediately process it through Ollama
        
        Expected payload:
        {
            "dataset_ids": [1, 2, 3],
            "datastore_ids": [1, 2, 3],
            "system_prompt": "optional custom system prompt",
            "user_prompt": "optional custom user prompt", 
            "model": "qwen2.5:8b",
            "description": "optional description"
        }
        """
        dataset_ids = request.data.get('dataset_ids', [])
        datastore_ids = request.data.get('datastore_ids', [])
        system_prompt = request.data.get('system_prompt')
        user_prompt = request.data.get('user_prompt')
        model = request.data.get('model', 'qwen2.5:8b')
        description = request.data.get('description', '')
        
        if not dataset_ids:
            return Response(
                {"error": "dataset_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not datastore_ids:
            return Response(
                {"error": "datastore_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Fetch datasets and datastores
            datasets = DatasetBaseModel.objects.filter(id__in=dataset_ids)
            datastores = Datastore.objects.filter(id__in=datastore_ids, is_active=True)
            
            if not datasets.exists():
                return Response(
                    {"error": "No valid datasets found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            if not datastores.exists():
                return Response(
                    {"error": "No valid datastores found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Prepare data for service
            datasets_data = []
            for dataset in datasets:
                datasets_data.append({
                    'id': dataset.id,
                    'name': dataset.name,
                    'data_structure': dataset.data_structure,
                    'growth_rate': dataset.growth_rate,
                    'access_patterns': dataset.access_patterns,
                    'query_complexity': dataset.query_complexity,
                    'estimated_size_gb': dataset.estimated_size_gb,
                    'avg_query_time_ms': dataset.avg_query_time_ms,
                    'queries_per_day': dataset.queries_per_day
                })
            
            datastores_data = []
            for datastore in datastores:
                datastores_data.append({
                    'id': datastore.id,
                    'name': datastore.name,
                    'type': datastore.type,
                    'system': datastore.system,
                    'max_connections': datastore.max_connections,
                    'avg_response_time_ms': datastore.avg_response_time_ms,
                    'storage_capacity_gb': datastore.storage_capacity_gb
                })
            
            # Create and process
            request_obj, response_obj = self.ollama_service.create_request_and_process(
                datasets_data=datasets_data,
                datastores_data=datastores_data,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=model,
                description=description
            )
            
            # Serialize responses
            request_serializer = RequestSerializer(request_obj)
            response_serializer = ResponseDetailSerializer(response_obj)
            
            return Response({
                "status": "success",
                "message": f"Successfully created and processed request {request_obj.id}",
                "request": request_serializer.data,
                "response": response_serializer.data
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error in create_and_process: {str(e)}")
            return Response(
                {"error": f"Failed to create and process request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    
    @action(detail=False, methods=["post"])
    def quick_match(self, request):
        """
        Quick matching for all available datasets and datastores
        
        Expected payload:
        {
            "model": "qwen2.5:8b",  # optional
            "include_inactive_datastores": false,  # optional
            "description": "Quick match for all data"  # optional
        }
        """
        model = request.data.get('model', 'qwen2.5:8b')
        include_inactive = request.data.get('include_inactive_datastores', False)
        description = request.data.get('description', 'Quick match for all available data')
        
        try:
            # Get all datasets and datastores
            datasets = DatasetBaseModel.objects.all()
            
            if include_inactive:
                datastores = Datastore.objects.all()
            else:
                datastores = Datastore.objects.filter(is_active=True)
            
            if not datasets.exists():
                return Response(
                    {"error": "No datasets found in the system"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            if not datastores.exists():
                return Response(
                    {"error": "No datastores found in the system"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Prepare simplified data
            datasets_data = []
            for dataset in datasets[:20]:  # Limit to first 20 to avoid huge prompts
                datasets_data.append({
                    'id': dataset.id,
                    'name': dataset.name,
                    'data_structure': dataset.data_structure,
                    'growth_rate': dataset.growth_rate,
                    'access_patterns': dataset.access_patterns,
                    'estimated_size_gb': dataset.estimated_size_gb or 0
                })
            
            datastores_data = []
            for datastore in datastores[:10]:  # Limit to first 10
                datastores_data.append({
                    'id': datastore.id,
                    'name': datastore.name,
                    'type': datastore.type,
                    'system': datastore.system,
                    'storage_capacity_gb': datastore.storage_capacity_gb or 0
                })
            
            # Create and process
            request_obj, response_obj = self.ollama_service.create_request_and_process(
                datasets_data=datasets_data,
                datastores_data=datastores_data,
                model=model,
                description=description
            )
            
            # Serialize responses
            response_serializer = ResponseDetailSerializer(response_obj)
            
            return Response({
                "status": "success",
                "message": f"Quick match completed for {len(datasets_data)} datasets and {len(datastores_data)} datastores",
                "request_id": request_obj.id,
                "response": response_serializer.data,
                "summary": {
                    "datasets_processed": len(datasets_data),
                    "datastores_available": len(datastores_data),
                    "model_used": model
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error in quick_match: {str(e)}")
            return Response(
                {"error": f"Quick match failed: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=["get"])
    def matching_status(self, request):
        """
        Get status information about matching capabilities
        """
        try:
            # Count available data
            datasets_count = DatasetBaseModel.objects.count()
            active_datastores_count = Datastore.objects.filter(is_active=True).count()
            total_datastores_count = Datastore.objects.count()
            
            # Recent activity
            recent_requests = MatchingRequests.objects.count()
            recent_responses = MatchingResponse.objects.count()
            
            # Check Ollama health
            from ..services.ollama_model_validator_service import OllamaModelValidator
            health_info = OllamaModelValidator.health_check()
            available_models = OllamaModelValidator.get_available_models()
            
            return Response({
                "system_status": {
                    "datasets_available": datasets_count,
                    "active_datastores": active_datastores_count,
                    "total_datastores": total_datastores_count,
                    "total_requests": recent_requests,
                    "total_responses": recent_responses
                },
                "ollama_status": {
                    "health": health_info["status"],
                    "available_models": available_models,
                    "models_count": len(available_models)
                },
                "ready_for_matching": (
                    datasets_count > 0 and 
                    active_datastores_count > 0 and 
                    health_info["status"] == "healthy"
                )
            })
            
        except Exception as e:
            logger.error(f"Error getting matching status: {str(e)}")
            return Response(
                {"error": f"Failed to get status: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        


    


