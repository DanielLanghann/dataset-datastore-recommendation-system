from rest_framework import serializers
from ..models import MatchingResponse, MatchingRequests
from decouple import config


class ResponseSerializer(serializers.ModelSerializer):
    """
    Main serializer for MatchingResponse with full validation
    """

    MAX_DESC_LENGTH = config("DESC_LIMIT", default=1000, cast=int)

    datasets_count = serializers.ReadOnlyField(source="get_datasets_count")
    datastores_count = serializers.ReadOnlyField(source="get_datastores_count")

    class Meta:
        model = MatchingResponse
        fields = "__all__"
        read_only_fields = ("id", "created_at")

    def validate_request_id(self, value):
        """Validate that the request exists"""
        if value and not MatchingRequests.objects.filter(id=value.id).exists():
            raise serializers.ValidationError("Referenced request does not exist")
        return value

    def validate_result(self, value):
        """Validate result structure and content"""
        if not value:
            raise serializers.ValidationError("Result cannot be empty")

        if not isinstance(value, (dict, list)):
            raise serializers.ValidationError("Result must be a JSON object or array")

        # dict check
        if isinstance(value, dict):
            expected_fields = [
                "datasets",
                "dataset_ids",
                "dataset_id",
                "datastores",
                "datastore_ids",
                "datastore_id",
                "matched_datastores",
                "matched_datastore_ids",
                "matched_datastore_id",
            ]

            if not any(field in value for field in expected_fields):
                raise serializers.ValidationError(
                    "Result should contain dataset and datastore information"
                )
        # list check
        elif isinstance(value, list):
            if not value:
                raise serializers.ValidationError("Result array cannot be empty")

            for i, item in enumerate(value):
                if not isinstance(item, dict):
                    raise serializers.ValidationError(
                        f"Result item {i+1} must be an object"
                    )

        return value

    def validate_model(self, value):
        """Validates the model name and make sure that is lower cased"""
        if not value or not value.strip():
            raise serializers.ValidationError("Model name cannot be empty")

        return value.strip()

    def validate_description(self, value):
        if value and len(value) > self.MAX_DESC_LENGTH:
            raise serializers.ValidationError(
                f"Description too long (max {self.MAX_DESC_LENGTH} characters)"
            )
        return value.strip() if value else value


class ResponseListSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for listing responses
    """

    datasets_count = serializers.ReadOnlyField(source="get_datasets_count")
    datastores_count = serializers.ReadOnlyField(source="get_datastores_count")
    request_summary = serializers.SerializerMethodField()

    class Meta:
        model = MatchingResponse
        fields = [
            "id",
            "created_at",
            "request_id",
            "model",
            "description",
            "datasets_count",
            "datastores_count",
            "request_summary",
        ]

    def get_request_summary(self, obj):
        """Get summary info about the related request"""
        if obj.request_id:
            return {
                "id": obj.request_id.id,
                "created_at": obj.request_id.created_at,
                "description": obj.request_id.description,
            }
        return None


class ResponseCreateSerializer(serializers.ModelSerializer):
    """
    Optimized serializer for creating new responses
    """

    class Meta:
        model = MatchingResponse
        fields = [
            "request_id",
            "result",
            "model",
            "description",
        ]

    # Inherit validation methods from ResponseSerializer
    def validate_request_id(self, value):
        return ResponseSerializer().validate_request_id(value)

    def validate_result(self, value):
        return ResponseSerializer().validate_result(value)

    def validate_model(self, value):
        return ResponseSerializer().validate_model(value)

    def validate_description(self, value):
        return ResponseSerializer().validate_description(value)

    def validate(self, attrs):
        """
        Cross-field validation for business logic
        """
        request_obj = attrs.get("request_id")
        result = attrs.get("result", {})

        if request_obj:
            # Validate that result makes sense with the request
            request_datasets_count = request_obj.get_datasets_count()
            request_datastores_count = request_obj.get_datastores_count()

            # Get result counts
            response_instance = MatchingResponse(result=result)
            result_datasets_count = response_instance.get_datasets_count()
            result_datastores_count = response_instance.get_datastores_count()

            # Business rule: response should reference datasets from request
            if result_datasets_count > request_datasets_count:
                raise serializers.ValidationError(
                    "Response references more datasets than available in the request"
                )

            # TODO: If we have less datasets in the response then in the request we should throw
            # a warning and list all datasets not part of the response

            if result_datastores_count > request_datastores_count:
                raise serializers.ValidationError(
                    "Response references more datastores than available in the request"
                )

        return attrs


class ResponseDetailSerializer(serializers.ModelSerializer):
    """
    Detailed serializer including related request information
    """

    datasets_count = serializers.ReadOnlyField(source="get_datasets_count")
    datastores_count = serializers.ReadOnlyField(source="get_datastores_count")
    request_details = serializers.SerializerMethodField()

    class Meta:
        model = MatchingResponse
        fields = "__all__"

    def get_request_details(self, obj):
        """Get detailed info about the related request"""
        if obj.request_id:
            return {
                "id": obj.request_id.id,
                "created_at": obj.request_id.created_at,
                "requested_model": obj.request_id.requested_model,
                "description": obj.request_id.description,
                "datasets_count": obj.request_id.get_datasets_count(),
                "datastores_count": obj.request_id.get_datastores_count(),
            }
        return None
