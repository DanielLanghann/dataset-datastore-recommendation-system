from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


class MatchingRequests(models.Model):
    """
    Stores AI matching requests with datasets, datastores, and prompts
    """

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)

    related_datasets = models.JSONField(
        help_text="JSON object with all datasets including related queries and relationships"
    )
    related_datastores = models.JSONField(
        help_text="JSON object with all related datastores"
    )
    # Prompt configuration
    system_prompt = models.TextField(
        help_text="The system prompt used for the LLM",
        default="You are an expert database architect with deep knowledge of different database systems, their strengths, limitations, and optimal use cases. Analyze the provided datasets and datastores to make informed recommendations.",
    )
    prompt = models.TextField(
        help_text="The user prompt used for the request",
        default="Based on the dataset characteristics (structure, size, growth rate, access patterns, query complexity) and available datastores (type, system, performance, capacity), recommend the optimal datastore for each dataset. Provide clear reasoning and confidence scores.",
    )

    # Model configuration
    requested_model = models.CharField(
        max_length=100,
        help_text="The Ollama model used for processing",
        default="llama3.1:8b",
    )

    description = models.TextField(
        blank=True, help_text="Optional description of the matching request"
    )

    class Meta:
        db_table = "ai_matching_requests"
        ordering = ["-created_at"]
        verbose_name = "AI Matching Request"
        verbose_name_plural = "AI Matching Requests"

    def __str__(self):
        return f"Request {self.id} - {self.created_at.strftime('%Y-%m-%d %H:%M')}"

    def get_datasets_count(self):
        """Returns the number of datasets in this request"""
        if isinstance(self.related_datasets, list):
            return len(self.related_datasets)
        elif (
            isinstance(self.related_datasets, dict)
            and "datasets" in self.related_datasets
        ):
            return len(self.related_datasets["datasets"])
        return 0

    def get_datastores_count(self):
        """Returns the number of datastores in this request"""
        if isinstance(self.related_datastores, list):
            return len(self.related_datastores)
        elif (
            isinstance(self.related_datastores, dict)
            and "datastores" in self.related_datastores
        ):
            return len(self.related_datastores["datastores"])
        return 0
