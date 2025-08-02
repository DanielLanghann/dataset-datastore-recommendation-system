from rest_framework import viewsets, status, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAuthenticatedOrReadOnly
from django_filters.rest_framework import DjangoFilterBackend
from django.db import transaction
from rest_framework.pagination import PageNumberPagination

from .models import DatasetBaseModel, DatasetRelationshipModel, DatasetQueriesModel
from .serializers import (
    DatasetBaseSerializer, DatasetDetailSerializer, DatasetListSerializer,
    DatasetCreateSerializer, DatasetCloneSerializer, BulkImportSerializer,
    DatasetAnalysisSerializer, DatasetRelationshipSerializer, DatasetQueriesSerializer
)

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 200

class DatasetViewSet(viewsets.ModelViewSet):
    # CRUD operations
    queryset = DatasetBaseModel.objects.all()
    permission_classes = [IsAuthenticatedOrReadOnly]
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]

    # Filtering options
    filterset_fields = ['data_structure', 'growth_rate', 'access_patterns', 'current_datastore_id', 'query_complexity']
    search_fields = ["name", "short_description"]
    ordering_fields =  ['created_at', 'name', 'estimated_size_gb', 'updated_at']
    ordering = ["id"]

    def get_serializer_class(self):
        if self.action == "list":
            return DatasetListSerializer
        elif self.action == 'retrieve':
            return DatasetDetailSerializer
        elif self.action == 'create':
            return DatasetCreateSerializer
        elif self.action == 'clone':
            return DatasetCloneSerializer
        elif self.action == 'bulk_import':
            return BulkImportSerializer
        elif self.action == 'analysis':
            return DatasetAnalysisSerializer
        return DatasetBaseSerializer
    
    def get_queryset(self):
        queryset = DatasetBaseModel.objects.all()

        if self.action == "retrieve":
            # prefetch related data for detail view
            queryset = queryset.prefetch_related(
                'relationships_from__to_dataset_id',
                'relationships_to__from_dataset_id',
                'queries'
            )
        elif self.action == "list":
            queryset = queryset.select_related("current_datastore_id")
        
        return queryset

    @action(detail=True, methods=['post'])
    def clone(self, request, pk=None):
        # Clone a dataset with optional relationships and queries
        dataset = self.get_object()
        serializer = self.get_serializer(data=request.data)
        
        if serializer.is_valid():
            with transaction.atomic():
                # Create new dataset
                new_dataset_data = {
                    'name': serializer.validated_data['new_name'],
                    'short_description': f"Clone of {dataset.short_description}",
                    'data_structure': dataset.data_structure,
                    'growth_rate': dataset.growth_rate,
                    'access_patterns': dataset.access_patterns,
                    'query_complexity': dataset.query_complexity,
                    'properties': dataset.properties,
                    'sample_data': dataset.sample_data,
                    'estimated_size_gb': dataset.estimated_size_gb,
                    'avg_query_time_ms': dataset.avg_query_time_ms,
                    'queries_per_day': dataset.queries_per_day,
                }
                
                new_dataset = DatasetBaseModel.objects.create(**new_dataset_data)
                
                # Clone queries if requested
                if serializer.validated_data['include_queries']:
                    for query in dataset.queries.all():
                        DatasetQueriesModel.objects.create(
                            dataset_id=new_dataset,
                            name=query.name,
                            query_text=query.query_text,
                            query_type=query.query_type,
                            frequency=query.frequency,
                            avg_execution_time_ms=query.avg_execution_time_ms,
                            description=query.description
                        )
                
                # Clone relationships if requested
                if serializer.validated_data['include_relationships']:
                    # Clone outgoing relationships
                    for rel in dataset.relationships_from.filter(is_active=True):
                        DatasetRelationshipModel.objects.create(
                            from_dataset_id=new_dataset,
                            to_dataset_id=rel.to_dataset_id,
                            relationship_type=rel.relationship_type,
                            strength=rel.strength,
                            description=rel.description
                        )
                
                # Return the cloned dataset
                response_serializer = DatasetDetailSerializer(new_dataset)
                return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['post'])
    def bulk_import(self, request):
        # Import multiple datasets from JSON
        serializer = self.get_serializer(data=request.data)
        
        if serializer.is_valid():
            created_datasets = []
            
            with transaction.atomic():
                for dataset_data in serializer.validated_data['datasets']:
                    dataset_serializer = DatasetCreateSerializer(data=dataset_data)
                    if dataset_serializer.is_valid():
                        dataset = dataset_serializer.save()
                        created_datasets.append(dataset)
                    else:
                        return Response({
                            'error': f"Invalid data for dataset {dataset_data.get('name', 'Unknown')}",
                            'details': dataset_serializer.errors
                        }, status=status.HTTP_400_BAD_REQUEST)
            
            response_serializer = DatasetListSerializer(created_datasets, many=True)
            return Response({
                'message': f'Successfully imported {len(created_datasets)} datasets',
                'datasets': response_serializer.data
            }, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['get'])
    def analysis(self, request, pk=None):
        """Get dataset analysis and recommendations"""
        dataset = self.get_object()
        
        # Calculate metrics
        total_relationships = (
            dataset.relationships_from.filter(is_active=True).count() + 
            dataset.relationships_to.filter(is_active=True).count()
        )
        total_queries = dataset.queries.count()

        
        analysis_data = {
            'dataset_id': dataset.id,
            'dataset_name': dataset.name,
            'total_relationships': total_relationships,
            'total_queries': total_queries,
        }
        
        serializer = DatasetAnalysisSerializer(analysis_data)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def relationships(self, request, pk=None):
        # Get all relationships for a dataset
        dataset = self.get_object()
        
        # Get both incoming and outgoing relationships
        outgoing = dataset.relationships_from.filter(is_active=True)
        incoming = dataset.relationships_to.filter(is_active=True)
        
        outgoing_serializer = DatasetRelationshipSerializer(outgoing, many=True)
        incoming_serializer = DatasetRelationshipSerializer(incoming, many=True)
        
        return Response({
            'outgoing_relationships': outgoing_serializer.data,
            'incoming_relationships': incoming_serializer.data
        })

    @action(detail=True, methods=['get'])
    def queries(self, request, pk=None):
        """Get all queries for a dataset"""
        dataset = self.get_object()
        queries = dataset.queries.all()
        
        # Apply filtering
        query_type = request.query_params.get('query_type')
        frequency = request.query_params.get('frequency')
        
        if query_type:
            queries = queries.filter(query_type=query_type)
        if frequency:
            queries = queries.filter(frequency=frequency)
        
        serializer = DatasetQueriesSerializer(queries, many=True)
        return Response(serializer.data)

class DatasetRelationshipViewSet(viewsets.ModelViewSet):

    queryset = DatasetRelationshipModel.objects.all()
    serializer_class = DatasetRelationshipSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = ['relationship_type', 'strength', 'is_active', 'from_dataset_id', 'to_dataset_id']
    ordering_fields = ['created_at', 'strength']
    ordering = ['-created_at']

    def get_queryset(self):
        """Select related datasets for performance"""
        return DatasetRelationshipModel.objects.select_related(
            'from_dataset_id', 'to_dataset_id'
        )


class DatasetQueriesViewSet(viewsets.ModelViewSet):
    queryset = DatasetQueriesModel.objects.all()
    serializer_class = DatasetQueriesSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['query_type', 'frequency', 'dataset_id']
    search_fields = ['name', 'description', 'query_text']
    ordering_fields = ['created_at', 'avg_execution_time_ms']
    ordering = ['-created_at']

    def get_queryset(self):
        # Select related dataset for performance
        return DatasetQueriesModel.objects.select_related('dataset_id')
