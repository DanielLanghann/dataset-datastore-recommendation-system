"""
Ollama Model Validator Service

Handles validation of Ollama models with real-time API checks,
caching, and graceful fallback strategies.
"""

import requests
import logging
from django.conf import settings
from django.core.cache import cache
from decouple import config

logger = logging.getLogger(__name__)


class OllamaModelValidator:
    """Centralized Ollama model validation with fallback strategies"""

    # Configuration from environment
    CACHE_KEY = "ollama_available_models"
    DEFAULT_CACHE_TIMEOUT = config(
        "OLLAMA_CACHE_TIMEOUT", default=600, cast=int
    )  # 10 minutes
    FALLBACK_CACHE_TIMEOUT = config(
        "OLLAMA_FALLBACK_CACHE_TIMEOUT", default=120, cast=int
    )  # 2 minutes
    DEFAULT_TIMEOUT = config("OLLAMA_REQUEST_TIMEOUT", default=5, cast=int)  # seconds

    @classmethod
    def get_available_models(cls):
        """Get available models from Ollama with caching and fallback"""
        cached_models = cache.get(cls.CACHE_KEY)

        if cached_models is not None:
            logger.debug(f"Using cached models: {len(cached_models)} models")
            return cached_models

        # Try to fetch fresh data from Ollama
        models = cls._fetch_models_from_ollama()

        if models:
            cache.set(cls.CACHE_KEY, models, cls.DEFAULT_CACHE_TIMEOUT)
            logger.info(f"Fetched and cached {len(models)} models from Ollama")
            return models

        # Fallback to configured models if Ollama is unreachable
        return cls._get_fallback_models()

    @classmethod
    def _fetch_models_from_ollama(cls):
        """Attempt to fetch models from Ollama API"""
        try:
            ollama_url = getattr(settings, "OLLAMA_API_URL", "http://localhost:11434")
            response = requests.get(
                f"{ollama_url}/api/tags", timeout=cls.DEFAULT_TIMEOUT
            )

            if response.status_code == 200:
                data = response.json()
                models = [model["name"] for model in data.get("models", [])]
                return models
            else:
                logger.warning(f"Ollama API returned status {response.status_code}")

        except requests.RequestException as e:
            logger.warning(f"Failed to connect to Ollama: {e}")

        return []

    @classmethod
    def _get_fallback_models(cls):
        """Get fallback models when Ollama is unreachable"""
        fallback_models = getattr(settings, "FALLBACK_OLLAMA_MODELS", [])

        if fallback_models:
            logger.info(f"Using fallback models: {len(fallback_models)} models")
            # Cache fallback for shorter time
            cache.set(cls.CACHE_KEY, fallback_models, cls.FALLBACK_CACHE_TIMEOUT)
            return fallback_models

        logger.error("No fallback models configured and Ollama unreachable")
        return []

    @classmethod
    def is_model_valid(cls, model_name):
        """
        Check if model is valid with graceful degradation

        Returns:
            tuple: (is_valid: bool, error_message: str|None)
        """
        if not model_name or not isinstance(model_name, str):
            return False, "Model name must be a non-empty string"

        model_name = model_name.strip()

        # Basic format validation for Ollama models
        if ":" not in model_name:
            return (
                False,
                "Invalid model format. Expected 'model:size' (e.g., 'llama3.1:8b')",
            )

        # Skip validation if disabled in settings
        if not getattr(settings, "VALIDATE_OLLAMA_MODELS", True):
            logger.debug(f"Model validation disabled, accepting: {model_name}")
            return True, None

        available_models = cls.get_available_models()

        if not available_models:
            # No models available and no fallback - allow anything with correct format
            logger.warning("No Ollama models available, skipping validation")
            return True, None

        if model_name in available_models:
            return True, None

        # Generate helpful error message with suggestions
        error_message = cls._generate_error_message(model_name, available_models)
        return False, error_message

    @classmethod
    def _generate_error_message(cls, requested_model, available_models):
        """Generate helpful error message with model suggestions"""
        # Find similar models (same base name)
        base_name = requested_model.split(":")[0]
        similar_models = [
            model
            for model in available_models
            if base_name in model or model.split(":")[0] == base_name
        ][:3]

        error_msg = f"Model '{requested_model}' is not available in Ollama"

        if similar_models:
            error_msg += f". Similar models: {', '.join(similar_models)}"
        elif available_models:
            # Show first few available models
            sample_models = available_models[:3]
            error_msg += f". Available models include: {', '.join(sample_models)}"
            if len(available_models) > 3:
                error_msg += f" (and {len(available_models) - 3} more)"

        return error_msg

    @classmethod
    def refresh_cache(cls):
        """Force refresh of model cache"""
        cache.delete(cls.CACHE_KEY)
        return cls.get_available_models()

    @classmethod
    def get_cache_info(cls):
        """Get information about cached models"""
        cached_models = cache.get(cls.CACHE_KEY)
        return {
            "cached": cached_models is not None,
            "count": len(cached_models) if cached_models else 0,
            "models": cached_models or [],
        }

    @classmethod
    def health_check(cls):
        """Check if Ollama service is reachable"""
        try:
            ollama_url = getattr(settings, "OLLAMA_API_URL", "http://localhost:11434")
            response = requests.get(
                f"{ollama_url}/api/tags", timeout=cls.DEFAULT_TIMEOUT
            )
            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "status_code": response.status_code,
                "response_time_ms": int(response.elapsed.total_seconds() * 1000),
            }
        except requests.RequestException as e:
            return {"status": "unreachable", "error": str(e), "response_time_ms": None}
