from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
import json

from .models import DatasetBaseModel, DatasetRelationshipModel, DatasetQueriesModel


class DatasetQueriesInline(admin.TabularInline):
    model = DatasetQueriesModel
    extra = 0
    fields = ['name', 'query_type', 'frequency', 'avg_execution_time_ms']
    readonly_fields = ['created_at']


class DatasetRelationshipInline(admin.TabularInline):
    model = DatasetRelationshipModel
    fk_name = 'from_dataset_id'
    extra = 0
    fields = ['to_dataset_id', 'relationship_type', 'strength', 'is_active']
    readonly_fields = ['created_at']


@admin.register(DatasetRelationshipModel)
class DatasetRelationshipModelAdmin(admin.ModelAdmin):
    list_display = [
        'from_dataset_id', 'to_dataset_id', 'relationship_type', 
        'strength', 'is_active', 'created_at'
    ]
    list_filter = ['relationship_type', 'strength', 'is_active', 'created_at']
    search_fields = [
        'from_dataset_id__name', 'to_dataset_id__name', 'description'
    ]
    readonly_fields = ['id', 'created_at', 'updated_at']
    
    fieldsets = (
        ('Relationship', {
            'fields': ('from_dataset_id', 'to_dataset_id', 'relationship_type')
        }),
        ('Properties', {
            'fields': ('strength', 'is_active', 'description')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def get_queryset(self, request):
        # Optimize queryset with select_related
        return super().get_queryset(request).select_related(
            'from_dataset_id', 'to_dataset_id'
        )


@admin.register(DatasetQueriesModel)
class DatasetQueriesAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'dataset_id', 'query_type', 'frequency', 
        'avg_execution_time_ms', 'created_at'
    ]
    list_filter = ['query_type', 'frequency', 'created_at']
    search_fields = ['name', 'description', 'dataset_id__name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'formatted_query_text']
    
    fieldsets = (
        ('Query Information', {
            'fields': ('dataset_id', 'name', 'query_type', 'frequency')
        }),
        ('Query Details', {
            'fields': ('formatted_query_text', 'description')
        }),
        ('Performance', {
            'fields': ('avg_execution_time_ms',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def formatted_query_text(self, obj):
        # Display query text in a formatted way
        if obj.query_text:
            return format_html('<pre style="white-space: pre-wrap; max-width: 500px;">{}</pre>', obj.query_text)
        return 'No query text'
    formatted_query_text.short_description = 'Query Text'
    
    def get_queryset(self, request):
        # Optimize queryset with select_related
        return super().get_queryset(request).select_related('dataset_id')

@admin.register(DatasetBaseModel)
class DatasetBaseModelAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'data_structure', 'growth_rate', 'access_patterns', 
        'query_complexity', 'current_datastore_id', 'estimated_size_gb', 
        'relationships_count', 'queries_count', 'created_at'
    ]
    list_filter = [
        'data_structure', 'growth_rate', 'access_patterns', 
        'query_complexity', 'created_at'
    ]
    search_fields = ['name', 'short_description']
    readonly_fields = ['id', 'created_at', 'updated_at', 'formatted_properties', 'formatted_sample_data']
    inlines = [DatasetQueriesInline, DatasetRelationshipInline]
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'name', 'short_description', 'current_datastore_id')
        }),
        ('Dataset Characteristics', {
            'fields': ('data_structure', 'growth_rate', 'access_patterns', 'query_complexity')
        }),
        ('Data Structure', {
            'fields': ('formatted_properties', 'formatted_sample_data')
        }),
        ('Performance Metrics', {
            'fields': ('estimated_size_gb', 'avg_query_time_ms', 'queries_per_day')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def relationships_count(self, obj):
        # Display count of active relationships
        count = (
            obj.relationships_from.filter(is_active=True).count() + 
            obj.relationships_to.filter(is_active=True).count()
        )
        if count > 0:
            url = reverse('admin:dataset_api_datasetrelationshipmodel_changelist')
            return format_html('<a href="{}?from_dataset_id={}">{}</a>', url, obj.id, count)
        return count
    relationships_count.short_description = 'Relationships'
    
    def queries_count(self, obj):
        # Display count of queries with link
        count = obj.queries.count()
        if count > 0:
            url = reverse('admin:dataset_api_datasetqueries_changelist')
            return format_html('<a href="{}?dataset_id={}">{}</a>', url, obj.id, count)
        return count
    queries_count.short_description = 'Queries'
    
    def formatted_properties(self, obj):
        # Display properties as formatted JSON
        if obj.properties:
            formatted = json.dumps(obj.properties, indent=2)
            return format_html('<pre style="background: #f8f8f8; padding: 10px; border-radius: 4px;">{}</pre>', formatted)
        return 'No properties'
    formatted_properties.short_description = 'Properties'
    
    def formatted_sample_data(self, obj):
        # Display sample data as formatted JSON
        if obj.sample_data:
            formatted = json.dumps(obj.sample_data, indent=2)
            return format_html('<pre style="background: #f8f8f8; padding: 10px; border-radius: 4px; max-height: 200px; overflow-y: auto;">{}</pre>', formatted)
        return 'No sample data'
    formatted_sample_data.short_description = 'Sample Data'