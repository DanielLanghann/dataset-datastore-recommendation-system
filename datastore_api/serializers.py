from django.forms import ValidationError
from rest_framework import serializers
from .models import Datastore
from .validators import DatastoreValidator

class DatestoreListSerializer(serializers.ModelSerializer):

    type_display = serializers.CharField(source="get_type_display", read_only=True)
    system_display = serializers.CharField(source="get_system_display", read_only=True)

    class Meta:
        model = Datastore
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'type', 'type_display',
            'system', 'system_display', 'description', 'is_active',
            'max_connections', 'avg_response_time_ms', 'storage_capacity_gb'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'type_display', 'system_display']

class DatastoreDetailSerializer(serializers.ModelSerializer):
    type_display = serializers.CharField(source='get_type_display', read_only=True)
    system_display = serializers.CharField(source='get_system_display', read_only=True)
    connection_info = serializers.SerializerMethodField()
    characteristics = serializers.ReadOnlyField()
    class Meta:
        model = Datastore
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'type', 'type_display',
            'system', 'system_display', 'description', 'connection_info',
            'is_active', 'max_connections', 'avg_response_time_ms', 
            'storage_capacity_gb', 'characteristics'
        ]
        read_only_fields = [
            'id', 'created_at', 'updated_at', 'type_display', 
            'system_display', 'connection_info', 'characteristics'
        ]
    
    def get_connection_info(self, obj):
        return obj.get_masked_connection_info()

     
    
class DatastoreCreateUpdateSerializer(serializers.ModelSerializer):
    password_confirm = serializers.CharField(write_only=True, required=False, allow_blank=True)
    
    class Meta:
        model = Datastore
        fields = [
            'name', 'type', 'system', 'description', 'server', 'port',
            'username', 'password', 'password_confirm', 'connection_string',
            'is_active', 'max_connections', 'avg_response_time_ms', 
            'storage_capacity_gb'
        ]
        extra_kwargs = {
            'password': {'write_only': True},
        }

    def validate(self, data):
        try:
            DatastoreValidator.validate_datastore_data(data)
        except ValidationError as e:
            raise serializers.ValidationError(str(e))
        
        if self.instance is None:
            password = data.get("password")
            password_confirm = data.get("password_confirm")

            if password and password != password_confirm:
                raise serializers.ValidationError({
                    'password_confirm': 'Password confirmation does not match.'
                })
        
        data.pop("password_confirm", None)

        return data
    
    def validate_name(self, value):
        # Check if name is unique
        if self.instance:
            if Datastore.objects.exclude(pk=self.instance.pk).filter(name=value).exists():
                raise serializers.ValidationError("A datastore with this name already exists.")
        else:
            if Datastore.objects.filter(name=value).exists():
                raise serializers.ValidationError("A datastore with this name already exists.")
        return value
    
class DatastoreTypeSerializer(serializers.Serializer):
    value = serializers.CharField()
    label = serializers.CharField()
    systems = serializers.ListField(child=serializers.DictField())
    characteristics = serializers.DictField()

class ConnectionTestSerializer(serializers.Serializer):
    server = serializers.CharField(required=False, allow_blank=True)
    port = serializers.IntegerField(required=False, min_value=1, max_value=65535)
    username = serializers.CharField(required=False, allow_blank=True)
    password = serializers.CharField(required=False, allow_blank=True)
    connection_string = serializers.CharField(required=False, allow_blank=True)

class ConnectionTestResponseSerializer(serializers.Serializer):
    success = serializers.BooleanField()
    response_time_ms = serializers.FloatField(required=False)
    message = serializers.CharField()
    details = serializers.DictField(required=False)
    error_code = serializers.CharField(required=False)

class DatastorePerformanceSerializer(serializers.ModelSerializer):
    # get datastore performance metriks
    class Meta:
        model = Datastore
        fields = [
            'id', 'name', 'max_connections', 'avg_response_time_ms', 
            'storage_capacity_gb', 'is_active'
        ]
        read_only_fields = ['id', 'name']

class DatastoreMinimalSerializer(serializers.ModelSerializer):
    # reduced serializer for third pary communication
    type_display = serializers.CharField(source='get_type_display', read_only=True)
    system_display = serializers.CharField(source='get_system_display', read_only=True)
    
    class Meta:
        model = Datastore
        fields = ['id', 'name', 'type', 'type_display', 'system', 'system_display']
        read_only_fields = ['id', 'name', 'type', 'type_display', 'system', 'system_display']

            
        



