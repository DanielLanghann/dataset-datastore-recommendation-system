from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from django.utils import timezone
from datetime import timedelta
from django.db.models import Count, Q

from ..models import MatchingResponse, MatchingRequests
from ..serializers.response_serializer import (
    ResponseSerializer,
    ResponseListSerializer,
    ResponseCreateSerializer,
    ResponseDetailSerializer,
)


class ResponseViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing AI Matching Responses

    Provides CRUD operations for MatchingResponse with different serializers
    for different actions (list, create, detail, update)
    """

    queryset = MatchingResponse.objects.all()
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        """Return appropriate serializer based on action"""
        if self.action == "list":
            return ResponseListSerializer
        elif self.action == "create":
            return ResponseCreateSerializer
        elif self.action == "retrieve":
            return ResponseDetailSerializer
        return ResponseSerializer

    def get_queryset(self):
        """
        Optionally filter responses by query parameters
        """
        queryset = MatchingResponse.objects.select_related("request_id")

        # by model
        model = self.request.query_params.get("model")
        if model:
            queryset = queryset.filter(model=model)

        # by request_id
        request_id = self.request.query_params.get("request_id")
        if request_id:
            try:
                queryset = queryset.filter(request_id=int(request_id))
            except (ValueError, TypeError):
                pass

        # by date range
        created_after = self.request.query_params.get("created_after")
        if created_after:
            try:
                date = timezone.datetime.fromisoformat(
                    created_after.replace("Z", "+00:00")
                )
                queryset = queryset.filter(created_at__gte=date)
            except ValueError:
                pass

        created_before = self.request.query_params.get("created_before")
        if created_before:
            try:
                date = timezone.datetime.fromisoformat(
                    created_before.replace("Z", "+00:00")
                )
                queryset = queryset.filter(created_at__lte=date)
            except ValueError:
                pass  # Invalid date format, ignore filter

        return queryset.order_by("-created_at")

    @transaction.atomic
    def create(self, request, *args, **kwargs):
        """New matching response"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            instance = serializer.save()
            response_serializer = ResponseDetailSerializer(instance)
            return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {"error": f"Failed to create response: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @action(detail=True, methods=["get"])
    def summary(self, request, pk=None):
        """summary of the matching response"""
        instance = self.get_object()

        return Response(
            {
                "id": instance.id,
                "created_at": instance.created_at,
                "request_id": instance.request_id.id if instance.request_id else None,
                "model": instance.model,
                "description": instance.description,
                "datasets_count": instance.get_datasets_count(),
                "datastores_count": instance.get_datastores_count(),
                "result_structure": self._analyze_result_structure(instance.result),
            }
        )

    def _analyze_result_structure(self, result):
        """analyze result JSON"""
        if not result:
            return {"type": "empty"}

        if isinstance(result, dict):
            keys = list(result.keys())
            return {
                "type": "object",
                "keys": keys[:5],
                "total_keys": len(keys),
            }
        elif isinstance(result, list):
            return {
                "type": "array",
                "length": len(result),
                "sample_item_type": type(result[0]).__name__ if result else None,
            }
        else:
            return {"type": type(result).__name__}

    @action(detail=True, methods=["get"])
    def analysis(self, request, pk=None):
        """Provide detailed analysis of the response result"""
        instance = self.get_object()
        result = instance.result

        if not result:
            return Response({"error": "No result data to analyze"})

        analysis = {
            "structure": self._analyze_result_structure(result),
            "datasets_mentioned": instance.get_datasets_count(),
            "datastores_mentioned": instance.get_datastores_count(),
            "result_size_bytes": len(str(result)),
        }

        # Additional analysis for different result structures
        if isinstance(result, dict):
            analysis["keys_analysis"] = {
                "total_keys": len(result.keys()),
                "top_level_keys": list(result.keys()),
                "nested_objects": sum(
                    1 for v in result.values() if isinstance(v, dict)
                ),
                "nested_arrays": sum(1 for v in result.values() if isinstance(v, list)),
            }
        elif isinstance(result, list):
            analysis["array_analysis"] = {
                "length": len(result),
                "item_types": list(set(type(item).__name__ for item in result)),
                "unique_items": len(set(str(item) for item in result)),
            }

        return Response(analysis)
