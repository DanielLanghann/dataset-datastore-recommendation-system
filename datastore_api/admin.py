from django.contrib import admin
from .models import Datastore

@admin.register(Datastore)
class DatastoreAdmin(admin.ModelAdmin):
    list_display = ('name', 'type', 'system', 'is_active', 'created_at', 'avg_response_time_ms')
    list_filter = ('type', 'system', 'is_active', 'created_at')
    search_fields = ('name', 'description')
    readonly_fields = ('id', 'created_at', 'updated_at')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'type', 'system', 'description', 'is_active')
        }),
        ('Connection Details', {
            'fields': ('server', 'port', 'username', 'password', 'connection_string'),
            'classes': ('collapse',)
        }),
        ('Performance Metrics', {
            'fields': ('max_connections', 'avg_response_time_ms', 'storage_capacity_gb'),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('id', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        # hide password
        if 'password' in form.base_fields:
            form.base_fields['password'].widget.attrs['type'] = 'password'
        return form