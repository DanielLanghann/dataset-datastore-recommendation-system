from rest_framework import serializers
from .models import Datastore

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
    password = serializers.CharField(write_only=True, required=False, allow_blank=True)
    password_confirm = serializers.CharField(write_only=True, required=False, allow_blank=True)

    def validate(self, data):
        password = data.get("password")
        password_confirm = data.get("password_confirm")

        if password and password != password_confirm:
            raise serializers.ValidationError({
                "password_confirm": "Passwords do not match"
            })
        # remove password_confirm
        data.pop("password_confirm", None)
        return data
    
    def create(self, validated_data):
        password = validated_data.pop('password', None)
        instance = super().create(validated_data)
        if password:
            instance.set_password(password)
            instance.save()
        return instance
    
    def update(self, instance, validated_data):
        password = validated_data.pop('password', None)
        instance = super().update(instance, validated_data)
        if password:
            instance.set_password(password)
            instance.save()
        return instance
    
    class Meta:
        model = Datastore
        fields = [
            'name', 'type', 'system', 'description', 'server', 'port',
            'username', 'password', 'password_confirm', 'connection_string',
            'is_active', 'max_connections', 'avg_response_time_ms', 
            'storage_capacity_gb'
        ]

class DatastorePerformanceSerializer(serializers.ModelSerializer):
    # get datastore performance metriks
    class Meta:
        model = Datastore
        fields = [
            'id', 'name', 'max_connections', 'avg_response_time_ms', 
            'storage_capacity_gb', 'is_active'
        ]
        read_only_fields = ['id', 'name']

