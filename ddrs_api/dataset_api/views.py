from rest_framework.pagination import PageNumberPagination
from rest_framework import viewsets, status, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from django.db import transaction
from rest_framework.pagination import PageNumberPagination
import logging

logger = logging.getLogger(__name__)

from .models import DatasetBaseModel
from .serializers import (
    DatasetListSerializer, DatasetDetailSerializer, DatasetCreateSerializer,
    
)

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 200

class DatasetViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing datasets.
    Provides CRUD operations:
    - GET /datasets/ - List all datasets
    - POST /datasets/ - Create a new dataset
    - GET /datasets/{id}/ - Retrieve a specific dataset
    - PUT /datasets/{id}/ - Update a dataset
    - PATCH /datasets/{id}/ - Partially update a dataset
    - DELETE /datasets/{id}/ - Delete a dataset
    """
    queryset = DatasetBaseModel.objects.all()
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    # Filtering and search
    filterset_fields = ['name']
    search_fields = ["name"]
    ordering_fields = ['created_at', 'updated_at', 'name', 'id']
    ordering = ["id"]

    def get_serializer_class(self):
        """
        Return the appropriate serializer class based on the action.
        """
        if self.action == "list":
            return DatasetListSerializer
        elif self.action in ['create', 'update', 'partial_update']:
            return DatasetCreateSerializer
        elif self.action == "retrieve":
            return DatasetDetailSerializer
        
        return DatasetListSerializer
    
    def get_queryset(self):
        """
        Optionally restricts the returned datasets by filtering against
        query parameters in the URL.
        """
        queryset = DatasetBaseModel.objects.all()
        
        if self.action == "list":
            # Name filter (exact match)
            name = self.request.query_params.get("name")
            if name:
                queryset = queryset.filter(name__iexact=name)
            
        return queryset.order_by("id")
    
    def create(self, request, *args, **kwards):
        """
        Create a new dataset.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            with transaction.atomic():
                instance = serializer.save()
            
            headers = self.get_success_headers(serializer.data)
            response_serializer = DatasetDetailSerializer(instance)
            
            logger.info(f"Dataset '{instance.name}' created successfully with ID {instance.id}")
            
            return Response(
                response_serializer.data,
                status=status.HTTP_201_CREATED,
                headers=headers
            )
        
        except ValidationError as e:
            logger.error(f"Validation error during dataset creation: {str(e)}")
            return Response(
                {'error': f'Validation error: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        except Exception as e:
            logger.error(f"Unexpected error during dataset creation: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while creating the dataset.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    def update(self, request, *args, **kwargs):
        """
        Update a dataset.
        """
        partial = kwargs.pop("partial", False)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        try:
            with transaction.atomic():
                instance = serializer.save()
            
            response_serializer = DatasetDetailSerializer(instance)
            
            logger.info(f"Dataset '{instance.name}' updated successfully")
            
            return Response(response_serializer.data)
        
        except ValidationError as e:
            logger.error(f"Validation error during dataset update: {str(e)}")
            return Response(
                {'error': f'Validation error: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        except Exception as e:
            logger.error(f"Unexpected error during dataset update: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while updating the dataset.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve a dataset.
        """
        try:
            instance = self.get_object()
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        
        except Exception as e:
            logger.error(f"Unexpected error during dataset retrieval: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while retrieving the dataset.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete a dataset with confirmation message.
        """
        try:
            instance = self.get_object()
            dataset_name = instance.name
            dataset_id = instance.id
            
            with transaction.atomic():
                self.perform_destroy(instance)
            
            logger.info(f"Dataset '{dataset_name}' (ID: {dataset_id}) deleted successfully")
            
            return Response(
                {'message': f"Dataset '{dataset_name}' deleted successfully"},
                status=status.HTTP_204_NO_CONTENT
            )
        
        except Exception as e:
            logger.error(f"Unexpected error during dataset deletion: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while deleting the dataset.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    
    




