from rest_framework import viewsets, status, filters
from rest_framework.permissions import IsAuthenticatedOrReadOnly, IsAuthenticated
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db import transaction
from django_filters.rest_framework import DjangoFilterBackend
from django.core.exceptions import ValidationError, ImproperlyConfigured
from rest_framework.pagination import PageNumberPagination
import logging

from .models import Datastore
from .serializers import (
    DatestoreListSerializer,
    DatastoreDetailSerializer, 
    DatastoreCreateUpdateSerializer,
    DatastorePerformanceSerializer
)

logger = logging.getLogger(__name__)

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 200

class DatastoreViewSet(viewsets.ModelViewSet):
    queryset = Datastore.objects.all()
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    # Filtering and search options
    filterset_fields = ['type', 'system', 'is_active']
    search_fields = ["name", "type", "system"]
    ordering_fields =  ['created_at', 'name', 'is_active', 'avg_response_time_ms', 'storage_capacity_gb']
    ordering = ["id"]
    
    def get_serializer_class(self):
        if self.action == 'list':
            return DatestoreListSerializer
        elif self.action in ['create', 'update', 'partial_update']:
            return DatastoreCreateUpdateSerializer
        elif self.action == 'retrieve':
            return DatastoreDetailSerializer
        elif self.action == 'performance':
            return DatastorePerformanceSerializer
        
        return DatestoreListSerializer
    
    def get_queryset(self):
        queryset = Datastore.objects.all()
        if self.action == 'list':
            # Type filter
            datastore_type = self.request.query_params.get('type')
            if datastore_type:
                queryset = queryset.filter(type=datastore_type)

            # System filter    
            system = self.request.query_params.get('system')
            if system:
                queryset = queryset.filter(system=system)
                
            # Status filter
            is_active = self.request.query_params.get('is_active')
            if is_active is not None:
                is_active_bool = is_active.lower() in ['true', '1', 'yes']
                queryset = queryset.filter(is_active=is_active_bool)
        
        return queryset.order_by('id')
    
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            with transaction.atomic():
                instance = serializer.save()
            
            headers = self.get_success_headers(serializer.data)
            response_serializer = DatastoreDetailSerializer(instance)
            
            logger.info(f"Datastore '{instance.name}' created successfully with ID {instance.id}")
            
            return Response(
                response_serializer.data,
                status=status.HTTP_201_CREATED,
                headers=headers
            )
        except (ValueError, ImproperlyConfigured) as e:
            logger.error(f"Encryption error during datastore creation: {str(e)}")
            if "encrypt" in str(e).lower():
                return Response(
                    {'error': 'Failed to encrypt password. Please check your encryption configuration.'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return Response(
                {'error': 'Configuration error. Please contact administrator.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error(f"Unexpected error during datastore creation: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while creating the datastore.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        try:
            with transaction.atomic():
                instance = serializer.save()
            response_serializer = DatastoreDetailSerializer(instance)
            return Response (response_serializer.data)
        
        except (ValueError, ImproperlyConfigured) as e:
            logger.error(f"Encryption error during datastore update: {str(e)}")
            if "encrypt" in str(e).lower():
                return Response(
                    {'error': 'Failed to encrypt password. Please check your encryption configuration.'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            elif "decrypt" in str(e).lower():
                return Response(
                    {'error': 'Failed to decrypt existing password. Please contact administrator.'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return Response(
                {'error': 'Configuration error. Please contact administrator.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error(f"Unexpected error during datastore update: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while updating the datastore.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def retrieve(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            if instance.has_password():
                _ = instance.password # trigger decryption
            
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        
        except (ValueError, ImproperlyConfigured) as e:
            logger.error(f"Decryption error during datastore retrieval: {str(e)}")
            if "decrypt" in str(e).lower():
                return Response(
                    {'error': 'Failed to decrypt password. Data may be corrupted or encryption key changed.'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return Response(
                {'error': 'Configuration error. Please contact administrator.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error(f"Unexpected error during datastore retrieval: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while retrieving the datastore.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    def destroy(self, request, *args, **kwargs):
        """
        Delete a datastore with confirmation message and proper error handling.
        """
        try:
            instance = self.get_object()
            datastore_name = instance.name
            datastore_id = instance.id
            datastore_type = getattr(instance, 'type', 'unknown')
            
            # Check if datastore has any dependencies or is currently in use
            # (Add any custom business logic checks here if needed)
            
            with transaction.atomic():
                self.perform_destroy(instance)
            
            logger.info(f"Datastore '{datastore_name}' (ID: {datastore_id}, Type: {datastore_type}) deleted successfully")
            
            return Response(
                {
                    'message': f"Datastore '{datastore_name}' deleted successfully",
                    'deleted_datastore': {
                        'id': datastore_id,
                        'name': datastore_name,
                        'type': datastore_type
                    }
                },
                status=status.HTTP_204_NO_CONTENT
            )
        
        except ValidationError as e:
            logger.error(f"Validation error during datastore deletion: {str(e)}")
            return Response(
                {'error': f'Cannot delete datastore: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        except (ValueError, ImproperlyConfigured) as e:
            logger.error(f"Configuration error during datastore deletion: {str(e)}")
            if "decrypt" in str(e).lower():
                return Response(
                    {'error': 'Failed to access datastore data during deletion. Please contact administrator.'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return Response(
                {'error': 'Configuration error during deletion. Please contact administrator.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        except Exception as e:
            logger.error(f"Unexpected error during datastore deletion: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while deleting the datastore.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
            
        
    
    @action(detail=False, methods=['get'])
    def active(self, request):
       # GET /datastores/active/
        queryset = Datastore.objects.active()
        serializer = DatestoreListSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def performance(self, request):
        # GET /datastores/performance/
        queryset = Datastore.objects.all()
        serializer = DatastorePerformanceSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['patch'])
    def toggle_active(self, request, pk=None):
        # PATCH /datastores/{id}/toggle_active/
        datastore = self.get_object()
        
        datastore.is_active = not datastore.is_active
        
        try:
            with transaction.atomic():
                datastore.save()
            
            serializer = DatastoreDetailSerializer(datastore)
            return Response({
                'message': f'Datastore {"activated" if datastore.is_active else "deactivated"} successfully',
                'datastore': serializer.data
            })
            
        except ValidationError as e:
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        except Exception as e:
            logger.error(f"Unexpected error during toggle_active: {str(e)}")
            return Response(
                {'error': 'An unexpected error occurred while updating the datastore status.'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    

    
    
    