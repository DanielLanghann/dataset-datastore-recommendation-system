from django.db import models

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from cryptography.fernet import Fernet
from django.conf import settings
import base64

class DatasetQueries(models.Model):
    """
    We store query examples in this model.
    """

    QUERY_TYPES = [
        ('select', 'Select'),
        ('insert', 'Insert'),
        ('update', 'Update'),
        ('delete', 'Delete'),
        ('complex', 'Complex'),
        ('aggregate', 'Aggregate')
    ]

    FREQUENCY = [
        ('high', 'High'),
        ('medium', 'Medium'),
        ('low', 'Low')
    ]

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    dataset_id = models.ForeignKey(
        'dataset_api.DatasetBaseModel',
        on_delete=models.CASCADE,
        related_name='queries',
        help_text="Related dataset ID"
    )
    name = models.CharField(max_length=255, help_text="Descriptive name for query")
    query_text = models.TextField(help_text="The actual query/command text")
    query_type = models.CharField(max_length=20, choices=QUERY_TYPES, help_text="Type of query")
    frequency = models.CharField(max_length=20, choices=FREQUENCY, help_text="How often this query runs")
    avg_execution_time_ms = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Average execution time in milliseconds"
    )
    description = models.TextField(blank=True, help_text="Optional query description")

    class Meta:
        db_table = "dataset_api_dataset_queries"
        ordering = ["id"]
        indexes = [
            models.Index(fields=['dataset_id']),
            models.Index(fields=['query_type']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"{self.name} ({self.dataset_id.name})"

