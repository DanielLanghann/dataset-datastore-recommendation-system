from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from django.utils import timezone
from datetime import timedelta

from ..models import MatchingRequests
from ..serializers import (
    RequestSerializer,
    RequestListSerializer,
    RequestCreateSerializer,
)


class RequestViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing AI Matching Requests

    Provides CRUD operations for MatchingRequests with different serializers
    for different actions (list, create, detail, update)
    """

    queryset = MatchingRequests.objects.all()
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        """Return appropriate serializer based on action"""
        if self.action == "list":
            return RequestListSerializer
        elif self.action == "create":
            return RequestCreateSerializer
        return RequestSerializer

    def get_queryset(self):
        """
        Optionally filter requests by query parameters
        """
        queryset = MatchingRequests.objects.all()

        # Filter by requested model
        model = self.request.query_params.get("model")
        if model:
            queryset = queryset.filter(requested_model=model)

        # Filter by date range
        created_after = self.request.query_params.get("created_after")
        if created_after:
            try:
                date = timezone.datetime.fromisoformat(
                    created_after.replace("Z", "+00:00")
                )
                queryset = queryset.filter(created_at__gte=date)
            except ValueError:
                pass  # Invalid date format, ignore filter

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
        """
        Create a new matching request with atomic transaction
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            instance = serializer.save()

            # Return full details of created instance
            response_serializer = RequestSerializer(instance)
            return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {"error": f"Failed to create request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def update(self, request, *args, **kwargs):
        """
        Update an existing matching request
        """
        partial = kwargs.pop("partial", False)
        instance = self.get_object()

        # Use CreateSerializer validation logic for updates
        serializer = RequestCreateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)

        try:
            updated_instance = serializer.save()

            # Return full details of updated instance
            response_serializer = RequestSerializer(updated_instance)
            return Response(response_serializer.data)
        except Exception as e:
            return Response(
                {"error": f"Failed to update request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @action(detail=True, methods=["get"])
    def summary(self, request, pk=None):
        """
        Get a summary of the matching request
        """
        instance = self.get_object()

        return Response(
            {
                "id": instance.id,
                "created_at": instance.created_at,
                "requested_model": instance.requested_model,
                "description": instance.description,
                "datasets_count": instance.get_datasets_count(),
                "datastores_count": instance.get_datastores_count(),
                "system_prompt_length": len(instance.system_prompt),
                "prompt_length": len(instance.prompt),
            }
        )

    @action(detail=False, methods=["get"])
    def recent(self, request):
        """
        Get recent requests (last 7 days)
        """
        seven_days_ago = timezone.now() - timedelta(days=7)
        recent_requests = self.queryset.filter(created_at__gte=seven_days_ago)

        serializer = RequestListSerializer(recent_requests, many=True)
        return Response({"count": recent_requests.count(), "results": serializer.data})

    @action(detail=False, methods=["get"])
    def stats(self, request):
        """
        Get statistics about matching requests
        """
        total_requests = self.queryset.count()

        # Count by model
        models_stats = {}
        for req in self.queryset.values("requested_model"):
            model = req["requested_model"]
            models_stats[model] = models_stats.get(model, 0) + 1

        # Recent activity (last 30 days)
        thirty_days_ago = timezone.now() - timedelta(days=30)
        recent_count = self.queryset.filter(created_at__gte=thirty_days_ago).count()

        return Response(
            {
                "total_requests": total_requests,
                "recent_requests_30_days": recent_count,
                "models_usage": models_stats,
                "most_used_model": (
                    max(models_stats.items(), key=lambda x: x[1])[0]
                    if models_stats
                    else None
                ),
            }
        )

    @action(detail=True, methods=["post"])
    def duplicate(self, request, pk=None):
        """
        Create a duplicate of an existing request
        """
        original = self.get_object()

        # Create a copy with updated data if provided
        duplicate_data = {
            "related_datasets": original.related_datasets,
            "related_datastores": original.related_datastores,
            "system_prompt": original.system_prompt,
            "prompt": original.prompt,
            "requested_model": original.requested_model,
            "description": (
                f"Copy of: {original.description}"
                if original.description
                else "Duplicated request"
            ),
        }

        # Override with any provided data
        duplicate_data.update(request.data)

        serializer = RequestCreateSerializer(data=duplicate_data)
        serializer.is_valid(raise_exception=True)

        try:
            new_instance = serializer.save()
            response_serializer = RequestSerializer(new_instance)
            return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {"error": f"Failed to duplicate request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
