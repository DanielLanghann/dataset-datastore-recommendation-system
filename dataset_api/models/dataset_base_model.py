from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from cryptography.fernet import Fernet
from django.conf import settings
import base64

class DatasetBaseModel(models.Model):
    DATA_STRUCTURE_CHOICES = [
        ('structured', 'Structured'),
        ('semi_structured', 'Semi-Structured'),
        ('unstructured', 'Unstructured')
    ]

    GROWTH_RATE_CHOICES = [
        ('high', 'High'),
        ('medium', 'Medium'),
        ('low', 'Low')
    ]

    ACCESS_PATTERN_CHOICES = [
        ('read_heavy', 'Read Heavy'),
        ('write_heavy', 'Write Heavy'),
        ('read_write_heavy', 'Read/Write Heavy'),
        ('analytical', 'Analytical'),
        ('transactional', 'Transactional')
    ]

    QUERY_COMPLEXITY_CHOICES = [
        ('high', 'High'),
        ('medium', 'Medium'),
        ('low', 'Low')
    ]

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Required fields
    name = models.CharField(max_length=255, unique=True, help_text="Name of the dataset typically the name of the table or collection.")
    short_description = models.CharField(max_length=1000, help_text="Brief text description of the dataset's purpose")
    current_datastore_id = models.ForeignKey(
        'datastore_api.Datastore',
        on_delete=models.CASCADE,
        related_name='datasets',
        null=True,
        blank=True,
        help_text="The datastore where this dataset is currently stored"
    )
    data_structure = models.CharField(max_length=20, choices=DATA_STRUCTURE_CHOICES, help_text="The data structure of the dataset")
    growth_rate = models.CharField(max_length=20, choices=GROWTH_RATE_CHOICES, help_text="The growth rate of the dataset")
    access_patterns = models.CharField(max_length=20, choices=ACCESS_PATTERN_CHOICES, help_text="The access pattern of the dataset")
    query_complexity = models.CharField(max_length=20, choices=QUERY_COMPLEXITY_CHOICES, help_text="The related query complexity")
    properties = models.JSONField(
        default=list,
        blank=True,
        help_text="JSON list of column names or data properties"
    )
    sample_data = models.JSONField(
        default=list,
        blank=True,
        help_text="JSON list containing sample data rows"
    )
    estimated_size_gb = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Estimated dataset size in gigabytes"
    )
    avg_query_time_ms = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Average query execution time in milliseconds"
    )
    queries_per_day = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Estimated number of queries per day"
    )

    class Meta:
        db_table = "dataset_api_datasets"
        ordering = ["id"]
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['data_structure']),
            models.Index(fields=['current_datastore_id']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return self.name