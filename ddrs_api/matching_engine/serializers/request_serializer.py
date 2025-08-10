from rest_framework import serializers
from django.conf import settings
from ..models import MatchingRequests
from ..services.ollama_model_validator_service import OllamaModelValidator


class RequestSerializer(serializers.ModelSerializer):
    """
    Main serializer for MatchingRequests with full validation
    """

    datasets_count = serializers.ReadOnlyField(source="get_datasets_count")
    datastores_count = serializers.ReadOnlyField(source="get_datastores_count")

    class Meta:
        model = MatchingRequests
        fields = "__all__"
        read_only_fields = ("id", "created_at")

    def validate_requested_model(self, value):
        """Validate model name and availability with Ollama"""
        if not value or not value.strip():
            raise serializers.ValidationError("Model name cannot be empty")

        value = value.strip()
        is_valid, error_message = OllamaModelValidator.is_model_valid(value)

        if not is_valid:
            raise serializers.ValidationError(error_message)

        return value

    def validate_related_datasets(self, value):
        """Validate datasets structure and content"""
        if not value:
            raise serializers.ValidationError("At least one dataset is required")

        # Handle both list and dict formats
        datasets = value if isinstance(value, list) else value.get("datasets", [])

        if not datasets:
            raise serializers.ValidationError("No datasets found in the provided data")

        if len(datasets) > 50:  # Reasonable limit
            raise serializers.ValidationError("Too many datasets (max 50)")

        # Validate each dataset has minimum required fields
        for i, dataset in enumerate(datasets):
            if not isinstance(dataset, dict):
                raise serializers.ValidationError(f"Dataset {i+1} must be an object")
            if not dataset.get("name"):
                raise serializers.ValidationError(
                    f"Dataset {i+1} must have a 'name' field"
                )

        return value

    def validate_related_datastores(self, value):
        """Validate datastores structure and content"""
        if not value:
            raise serializers.ValidationError("At least one datastore is required")

        # Handle both list and dict formats
        datastores = value if isinstance(value, list) else value.get("datastores", [])

        if not datastores:
            raise serializers.ValidationError(
                "No datastores found in the provided data"
            )

        if len(datastores) > 20:  # Reasonable limit
            raise serializers.ValidationError("Too many datastores (max 20)")

        # Validate each datastore has minimum required fields
        for i, datastore in enumerate(datastores):
            if not isinstance(datastore, dict):
                raise serializers.ValidationError(f"Datastore {i+1} must be an object")
            if not datastore.get("name"):
                raise serializers.ValidationError(
                    f"Datastore {i+1} must have a 'name' field"
                )
            if not datastore.get("type"):
                raise serializers.ValidationError(
                    f"Datastore {i+1} must have a 'type' field"
                )

        return value

    def validate_system_prompt(self, value):
        """Validate system prompt"""
        if not value or not value.strip():
            raise serializers.ValidationError("System prompt cannot be empty")
        if len(value) > 5000:
            raise serializers.ValidationError(
                "System prompt too long (max 5000 characters)"
            )
        return value.strip()

    def validate_prompt(self, value):
        """Validate user prompt"""
        if not value or not value.strip():
            raise serializers.ValidationError("User prompt cannot be empty")
        if len(value) > 5000:
            raise serializers.ValidationError(
                "User prompt too long (max 5000 characters)"
            )
        return value.strip()

    def validate_description(self, value):
        """Validate description field"""
        if value and len(value) > 1000:
            raise serializers.ValidationError(
                "Description too long (max 1000 characters)"
            )
        return value.strip() if value else value


class RequestListSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for listing requests - excludes heavy JSON fields
    """

    datasets_count = serializers.ReadOnlyField(source="get_datasets_count")
    datastores_count = serializers.ReadOnlyField(source="get_datastores_count")

    class Meta:
        model = MatchingRequests
        fields = [
            "id",
            "created_at",
            "requested_model",
            "description",
            "datasets_count",
            "datastores_count",
        ]


class RequestCreateSerializer(serializers.ModelSerializer):
    """
    Optimized serializer for creating new requests
    """

    class Meta:
        model = MatchingRequests
        fields = [
            "related_datasets",
            "related_datastores",
            "system_prompt",
            "prompt",
            "requested_model",
            "description",
        ]

    # Inherit all validation methods from RequestSerializer
    def validate_requested_model(self, value):
        return RequestSerializer().validate_requested_model(value)

    def validate_related_datasets(self, value):
        return RequestSerializer().validate_related_datasets(value)

    def validate_related_datastores(self, value):
        return RequestSerializer().validate_related_datastores(value)

    def validate_system_prompt(self, value):
        return RequestSerializer().validate_system_prompt(value)

    def validate_prompt(self, value):
        return RequestSerializer().validate_prompt(value)

    def validate_description(self, value):
        return RequestSerializer().validate_description(value)

    def validate(self, attrs):
        """
        Cross-field validation for business logic
        """
        datasets = attrs.get("related_datasets", {})
        datastores = attrs.get("related_datastores", {})

        # Get actual counts for comparison
        dataset_count = (
            len(datasets)
            if isinstance(datasets, list)
            else len(datasets.get("datasets", []))
        )
        datastore_count = (
            len(datastores)
            if isinstance(datastores, list)
            else len(datastores.get("datastores", []))
        )

        # Business rule: reasonable ratio of datasets to datastores
        if dataset_count > datastore_count * 10:
            raise serializers.ValidationError(
                "Too many datasets relative to datastores. Consider adding more datastore options."
            )

        return attrs
