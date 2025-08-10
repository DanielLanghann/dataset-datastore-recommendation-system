from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ..services.ollama_model_validator_service import OllamaModelValidator


class OllamaValidationViewSet(viewsets.ViewSet):
    """
    ViewSet for Ollama model validation operations
    """

    permission_classes = [IsAuthenticated]

    @action(detail=False, methods=["get"])
    def models(self, request):
        """Get currently available Ollama models"""
        try:
            cache_info = OllamaModelValidator.get_cache_info()
            return Response(
                {
                    "available_models": cache_info["models"],
                    "count": cache_info["count"],
                    "cached": cache_info["cached"],
                    "status": "success",
                }
            )
        except Exception as e:
            return Response(
                {"error": str(e), "status": "error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @action(detail=False, methods=["post"])
    def refresh(self, request):
        """Force refresh of Ollama models cache"""
        try:
            models = OllamaModelValidator.refresh_cache()
            return Response(
                {"refreshed_models": models, "count": len(models), "status": "success"}
            )
        except Exception as e:
            return Response(
                {"error": str(e), "status": "error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @action(detail=False, methods=["get"])
    def health(self, request):
        """Check Ollama service health"""
        try:
            health_info = OllamaModelValidator.health_check()
            response_status = (
                status.HTTP_200_OK
                if health_info["status"] == "healthy"
                else status.HTTP_503_SERVICE_UNAVAILABLE
            )

            return Response(health_info, status=response_status)
        except Exception as e:
            return Response(
                {"status": "error", "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @action(detail=False, methods=["post"])
    def validate(self, request):
        """Validate a specific model name"""
        model_name = request.data.get("model_name")

        if not model_name:
            return Response(
                {"error": "model_name is required", "status": "error"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            is_valid, error_message = OllamaModelValidator.is_model_valid(model_name)

            return Response(
                {
                    "model_name": model_name,
                    "is_valid": is_valid,
                    "error_message": error_message,
                    "status": "success",
                }
            )
        except Exception as e:
            return Response(
                {"error": str(e), "status": "error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
