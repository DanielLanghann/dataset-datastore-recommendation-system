from django.db import models

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator

class DatasetRelationshipModel(models.Model):

    RELATIONSHIP_TYPES = [
        ('foreign_key', 'Foreign Key'),
        ('one_to_many', 'One to Many'), 
        ('many_to_many', 'Many to Many'),
        ('dependency', 'Dependency'),
        ('similarity', 'Similarity')
    ]

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    from_dataset = models.ForeignKey(
        'dataset_api.DatasetBaseModel',
        on_delete=models.CASCADE,
        related_name='relationships_from',
        null=True,
        blank=True,
        help_text="Source dataset ID"
    )
    to_dataset = models.ForeignKey(
        'dataset_api.DatasetBaseModel',
        on_delete=models.CASCADE,
        related_name='relationships_to',
        null=True,
        blank=True,
        help_text="Target dataset ID"
    )
    relationship_type = models.CharField(max_length=20, choices=RELATIONSHIP_TYPES, help_text="Type of the relationship between the datasets.")
    strength = models.PositiveIntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(10)],
        help_text="Relationship importance (1-10 scale)"
    )
    description = models.TextField(help_text="Optional description of the relationship")
    is_active = models.BooleanField(default=True, help_text="Boolean Flag indicating if relationship is active")

    class Meta:
        db_table = "dataset_api_dataset_relationships"
        ordering = ["id"]
        indexes = [
            models.Index(fields=['from_dataset_id']),
            models.Index(fields=['to_dataset_id']),
            models.Index(fields=['created_at']),
        ]








