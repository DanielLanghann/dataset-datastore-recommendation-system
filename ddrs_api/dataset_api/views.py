from rest_framework.pagination import PageNumberPagination
from rest_framework import viewsets, status, filters
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.exceptions import ValidationError
from django_filters.rest_framework import DjangoFilterBackend
from django.db import transaction
from django.db import models
from django.http import Http404
from django.conf import settings

import logging

logger = logging.getLogger(__name__)

from .models import DatasetBaseModel, DatasetRelationshipModel, DatasetQueriesModel
from .serializers import (
    DatasetListSerializer,
    DatasetDetailSerializer,
    DatasetCreateSerializer,
    DatasetQueriesSerializer,
    DatasetRelationshipSerializer,
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
    filter_backends = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]

    # Filtering and search
    filterset_fields = [
        "data_structure",
        "growth_rate",
        "access_patterns",
        "current_datastore",
        "query_complexity",
    ]
    search_fields = ["name", "short_description"]
    ordering_fields = ["created_at", "name", "estimated_size_gb", "updated_at"]
    ordering = ["id"]

    def get_serializer_class(self):
        """
        Return the appropriate serializer class based on the action.
        """
        if self.action == "list":
            return DatasetListSerializer
        elif self.action in ["create", "update", "partial_update"]:
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

    def create(self, request, *args, **kwargs):
        """
        Create a new dataset.
        """
        logger.info(f"Dataset creation request data: {request.data}")
        
        serializer = self.get_serializer(data=request.data)
        
        try:
            serializer.is_valid(raise_exception=True)
        except ValidationError as ve:
            logger.error(f"Serializer validation error: {ve}")
            logger.error(f"Serializer errors: {serializer.errors}")
            return Response(
                {"error": f"Validation error: {str(ve)}", "details": serializer.errors},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            with transaction.atomic():
                instance = serializer.save()

            headers = self.get_success_headers(serializer.data)
            response_serializer = DatasetDetailSerializer(instance)

            logger.info(
                f"Dataset '{instance.name}' created successfully with ID {instance.id}"
            )

            return Response(
                response_serializer.data,
                status=status.HTTP_201_CREATED,
                headers=headers,
            )

        except ValidationError as e:
            logger.error(f"Validation error during dataset creation: {str(e)}")
            return Response(
                {"error": f"Validation error: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        except Exception as e:
            logger.error(f"Unexpected error during dataset creation: {str(e)}")
            return Response(
                {"error": "An unexpected error occurred while creating the dataset."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def update(self, request, *args, **kwargs):
        """
        Update a dataset.
        """
        partial = kwargs.pop("partial", False)

        try:
            instance = self.get_object()
        except Http404:
            logger.warning(
                f"Attempt to update non-existent dataset with ID: {kwargs.get('pk')}"
            )
            return Response(
                {"error": f'Dataset with ID {kwargs.get("pk")} not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

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
                {"error": f"Validation error: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        except Exception as e:
            logger.error(f"Unexpected error during dataset update: {str(e)}")
            return Response(
                {"error": "An unexpected error occurred while updating the dataset."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve a dataset.
        """
        try:
            instance = self.get_object()
            serializer = self.get_serializer(instance)
            return Response(serializer.data)

        except Http404:
            logger.warning(
                f"Attempt to retrieve non-existent dataset with ID: {kwargs.get('pk')}"
            )
            return Response(
                {"error": f'Dataset with ID {kwargs.get("pk")} not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        except Exception as e:
            logger.error(f"Unexpected error during dataset retrieval: {str(e)}")
            return Response(
                {"error": "An unexpected error occurred while retrieving the dataset."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def destroy(self, request, *args, **kwargs):
        """
        Delete a dataset with confirmation message and proper error handling.
        """
        dataset_id = kwargs.get("pk")

        try:
            instance = self.get_object()
            dataset_name = instance.name

            # Check if dataset has related objects that would prevent deletion
            related_queries = instance.queries.count()
            related_relationships_from = instance.relationships_from.count()
            related_relationships_to = instance.relationships_to.count()

            # Optional: Warn about cascading deletes
            if (
                related_queries > 0
                or related_relationships_from > 0
                or related_relationships_to > 0
            ):
                logger.info(
                    f"Deleting dataset '{dataset_name}' will also delete {related_queries} queries and {related_relationships_from + related_relationships_to} relationships"
                )

            with transaction.atomic():
                self.perform_destroy(instance)

            logger.info(
                f"Dataset '{dataset_name}' (ID: {dataset_id}) deleted successfully"
            )

            return Response(
                {
                    "message": f"Dataset '{dataset_name}' deleted successfully",
                    "deleted_id": dataset_id,
                    "cascade_info": {
                        "deleted_queries": related_queries,
                        "deleted_relationships": related_relationships_from
                        + related_relationships_to,
                    },
                },
                status=status.HTTP_204_NO_CONTENT,
            )

        except Http404:
            logger.warning(
                f"Attempt to delete non-existent dataset with ID: {dataset_id}"
            )
            return Response(
                {
                    "error": f"Dataset with ID {dataset_id} not found.",
                    "message": "The dataset may have already been deleted or never existed.",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        except Exception as e:
            logger.error(
                f"Unexpected error during dataset deletion (ID: {dataset_id}): {str(e)}"
            )
            return Response(
                {
                    "error": "An unexpected error occurred while deleting the dataset.",
                    "details": (
                        str(e)
                        if settings.DEBUG
                        else "Contact administrator for assistance."
                    ),
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class DatasetQueriesViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing dataset queries.
    Provides CRUD operations for queries associated with datasets.
    """

    queryset = DatasetQueriesModel.objects.all()
    serializer_class = DatasetQueriesSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    filter_backends = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]

    # Filtering and search
    filterset_fields = ["dataset", "query_type", "frequency"]
    search_fields = ["name", "description", "query_text"]
    ordering_fields = ["created_at", "name", "avg_execution_time_ms"]
    ordering = ["id"]

    def get_queryset(self):
        """
        Optionally filter queries by dataset.
        """
        queryset = DatasetQueriesModel.objects.all()
        dataset_id = self.request.query_params.get("dataset_id")
        if dataset_id:
            queryset = queryset.filter(dataset_id=dataset_id)
        return queryset.order_by("id")

    def destroy(self, request, *args, **kwargs):
        """
        Delete a query with proper error handling.
        """
        query_id = kwargs.get("pk")

        try:
            instance = self.get_object()
            query_name = instance.name
            dataset_name = instance.dataset.name

            with transaction.atomic():
                self.perform_destroy(instance)

            logger.info(
                f"Query '{query_name}' (ID: {query_id}) from dataset '{dataset_name}' deleted successfully"
            )

            return Response(
                {
                    "message": f"Query '{query_name}' deleted successfully",
                    "deleted_id": query_id,
                    "dataset": dataset_name,
                },
                status=status.HTTP_200_OK,
            )

        except Http404:
            logger.warning(f"Attempt to delete non-existent query with ID: {query_id}")
            return Response(
                {
                    "error": f"Query with ID {query_id} not found.",
                    "message": "The query may have already been deleted or never existed.",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        except Exception as e:
            logger.error(
                f"Unexpected error during query deletion (ID: {query_id}): {str(e)}"
            )
            return Response(
                {
                    "error": "An unexpected error occurred while deleting the query.",
                    "details": (
                        str(e)
                        if settings.DEBUG
                        else "Contact administrator for assistance."
                    ),
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class DatasetRelationshipViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing dataset relationships.
    Provides CRUD operations for relationships between datasets.
    """

    queryset = DatasetRelationshipModel.objects.all()
    serializer_class = DatasetRelationshipSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    filter_backends = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]

    # Filtering and search
    filterset_fields = [
        "from_dataset",
        "to_dataset",
        "relationship_type",
        "is_active",
        "strength",
    ]
    search_fields = ["description"]
    ordering_fields = ["created_at", "strength"]
    ordering = ["id"]

    def get_queryset(self):
        """
        Optionally filter relationships by dataset.
        """
        queryset = DatasetRelationshipModel.objects.all()

        # Filter by dataset (either from or to)
        dataset_id = self.request.query_params.get("dataset_id")
        if dataset_id:
            queryset = queryset.filter(
                models.Q(from_dataset=dataset_id) | models.Q(to_dataset=dataset_id)
            )

        # Filter only active relationships
        active_only = self.request.query_params.get("active_only")
        if active_only and active_only.lower() == "true":
            queryset = queryset.filter(is_active=True)

        return queryset.order_by("id")

    def destroy(self, request, *args, **kwargs):
        """
        Delete a relationship with proper error handling.
        """
        relationship_id = kwargs.get("pk")

        try:
            instance = self.get_object()
            from_dataset_name = (
                instance.from_dataset.name if instance.from_dataset else "Unknown"
            )
            to_dataset_name = (
                instance.to_dataset.name if instance.to_dataset else "Unknown"
            )
            relationship_type = instance.relationship_type

            with transaction.atomic():
                self.perform_destroy(instance)

            logger.info(
                f"Relationship '{relationship_type}' between '{from_dataset_name}' and '{to_dataset_name}' (ID: {relationship_id}) deleted successfully"
            )

            return Response(
                {
                    "message": f"Relationship deleted successfully",
                    "deleted_id": relationship_id,
                    "relationship_type": relationship_type,
                    "from_dataset": from_dataset_name,
                    "to_dataset": to_dataset_name,
                },
                status=status.HTTP_200_OK,
            )

        except Http404:
            logger.warning(
                f"Attempt to delete non-existent relationship with ID: {relationship_id}"
            )
            return Response(
                {
                    "error": f"Relationship with ID {relationship_id} not found.",
                    "message": "The relationship may have already been deleted or never existed.",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        except Exception as e:
            logger.error(
                f"Unexpected error during relationship deletion (ID: {relationship_id}): {str(e)}"
            )
            return Response(
                {
                    "error": "An unexpected error occurred while deleting the relationship.",
                    "details": (
                        str(e)
                        if settings.DEBUG
                        else "Contact administrator for assistance."
                    ),
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
