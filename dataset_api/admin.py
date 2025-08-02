from django.contrib import admin
from .models import DatasetBaseModel, DatasetRelationshipModel, DatasetQueriesModel

@admin.register(DatasetBaseModel)
class DatasetBaseModelAdmin(admin.ModelAdmin):
    list_display = ('name', 'data_structure', 'growth_rate', 'access_patterns', 'query_complexity', 'created_at')
    list_filter = ('data_structure', 'growth_rate', 'access_patterns', 'query_complexity', 'created_at')
    search_fields = ('name', 'short_description')
    readonly_fields = ('id', 'created_at', 'updated_at')

@admin.register(DatasetRelationshipModel)
class DatasetRelationshipModelAdmin(admin.ModelAdmin):
    list_display = ('from_dataset_id', 'to_dataset_id', 'relationship_type', 'strength', 'is_active', 'created_at')
    list_filter = ('relationship_type', 'strength', 'is_active', 'created_at')
    readonly_fields = ('id', 'created_at', 'updated_at')

@admin.register(DatasetQueriesModel)
class DatasetQueriesModelAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset_id', 'query_type', 'frequency', 'avg_execution_time_ms', 'created_at')
    list_filter = ('query_type', 'frequency', 'created_at')
    search_fields = ('name', 'description', 'query_text')
    readonly_fields = ('id', 'created_at', 'updated_at')
