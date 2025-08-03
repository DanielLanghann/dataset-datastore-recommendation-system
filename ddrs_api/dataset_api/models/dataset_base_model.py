from django.db import models

# Create your models here.
class DatasetBaseModel(models.Model):

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Required fields
    name = models.CharField(max_length=255, unique=True, help_text="Descriptive name for the datastore instance")

    class Meta:
        db_table = "dataset_api_datasets"
        ordering = ["id"]
        indexes = [
            models.Index(fields=["name"])
        ]