from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from cryptography.fernet import Fernet
from django.conf import settings
import base64

class Dataset(models.Model):
    DATA_STRUCTURE_CHOICES = [
        "structured", "semi_structured", "unstructured"
    ]

    GROWTH_RATE_CHOICES = [
        "high", "medium", "low"
    ]

    ACCESS_PATTERN_CHOICES = [
        "read_heavy", "write_heavy", "read_write_heavy", "analytical", "transactional"
    ]

    QUERY_COMPLEXITY_CHOICES = [
        "high", "medium", "low"
    ]

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Required fields
    name = models.CharField(max_length=255, unique=True, help_text="Name of the dataset typically the name of the table or collection.")
    description = models.TextField(help_text="Detailed description of the dataset")
    current_datastore = models.ForeignKey(
        'datastore_api.Datastore',
        on_delete=models.CASCADE,
        related_name='datasets',
        null=True,
        blank=True,
        help_text="The datastore where this dataset is currently stored"
    )
    data_structure = models.CharField(max_length=20, choices=DATA_STRUCTURE_CHOICES, help_text="The data structure of the dataset")
    growth_rate = models.CharField(max_length=20, choices=GROWTH_RATE_CHOICES, help_text="The growth rate of the dataset")
    access_pattern = models.CharField(max_length=20, choices=ACCESS_PATTERN_CHOICES, help_text="The access pattern of the dataset")
    query_complexity = models.CharField(max_length=20, choices=QUERY_COMPLEXITY_CHOICES, help_text="The related query complexity")
    properties = models.JSONField(
        default=dict,
        blank=True,
        help_text="JSON object defining dataset columns and their data types (e.g., {'id': 'integer', 'name': 'string', 'created_at': 'datetime'})"
    )
    sample_data = models.JSONField(
        default=list,
        blank=True,
        help_text="JSON array with sample data records from the dataset (e.g., [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}])"
    )
    estimated_size_gb = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Used Storage capacity in Gigabyte of the dataset."
    )
    average_query_time_ms = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Average response time of related queries."
    )
    average_number_of_queries_per_day = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Average number of queries per day."
    )
    



