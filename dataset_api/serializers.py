from rest_framework import serializers
from django.core.exceptions import ValidationError
from django.db import models
import json
from .models import DatasetBaseModel, DatasetRelationshipModel, DatasetQueriesModel

class DatasetQueriesSerializer(serializers.ModelSerializer):

    class Meta:
        model = DatasetQueriesModel
        fields = [
            "id", "created_at", "updated_at", "dataset_id", "name", 
            "query_text", "query_type", "frequency", "avg_execution_time_ms", "description"
        ]
        read_only_fields = ["id", "created_at", "updated_at"]
    
class DatasetRelationshipSerializer(serializers.ModelSerializer):
    from_dataset_name = serializers.CharField(source="from_dataset_id.name", read_only=True)
    to_dataset_name = serializers.CharField(source="to_dataset_id.name", read_only=True)

    class Meta:
        model = DatasetRelationshipModel
        fields = [
            'id', 'created_at', 'updated_at', 'from_dataset_id', 'to_dataset_id',
            'from_dataset_name', 'to_dataset_name', 'relationship_type', 
            'strength', 'description', 'is_active'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'from_dataset_name', 'to_dataset_name']

class DatasetBaseSerializer(serializers.ModelSerializer):

    class Meta:
        model = DatasetBaseModel
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'short_description',
            'current_datastore_id', 'data_structure', 'growth_rate',
            'access_patterns', 'query_complexity', 'properties', 'sample_data',
            'estimated_size_gb', 'avg_query_time_ms', 'queries_per_day'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

        def validate_properties(self, value):
            if not isinstance(value, list):
                raise serializers.ValidationError("Properties must be a list")
            return value
        
        def validate_sample_data(self, value):
            # Validate if sample data is a nested list
            if not isinstance(value, list):
                raise serializers.ValidationError("Sample data must be a list")
            
            for row in value:
                if not isinstance(row, list):
                    raise serializers.ValidationError("Each sample data must be a list")
            return value

class DatasetDetailSerializer(DatasetBaseSerializer):
    relationships_from = DatasetRelationshipSerializer(many=True, read_only=True)
    relationships_to = DatasetRelationshipSerializer(many=True, read_only = True)
    queries = DatasetQueriesSerializer(many=True, read_only=True)

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + [
            'relationships_from', 'relationships_to', 'queries'
        ]

class DatasetCreateSerializer(DatasetBaseSerializer):
    relationships = DatasetRelationshipSerializer(many=True, required=False)
    queries = DatasetQueriesSerializer(many=True, required=False)

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + ["relationships", "queries"]
    
    def create(self, validated_data):
        relationships_data = validated_data.pop("relationships", [])
        queries_data = validated_data.pop("queries", [])
        
        # Create dataset
        dataset = DatasetBaseModel.objects.create(**validated_data)

        # Create relationships
        for relationship_data in relationships_data:
            relationship_data["from_dataset_id"] = dataset
            DatasetRelationshipModel.objects.create(**relationship_data)
        # Create Queries
        for query_data in queries_data:
            query_data["dataset_id"] = dataset
            DatasetQueriesModel.objects.create(**query_data)
        
        return dataset
    
class DatasetListSerializer(serializers.ModelSerializer):
    datastore_name = serializers.CharField(source="current_datastore_id.name", read_only=True)
    relationships_count = serializers.SerializerMethodField()
    queries_count = serializers.SerializerMethodField()

    class Meta:
        model = DatasetBaseModel
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'short_description',
            'current_datastore_id', 'datastore_name', 'data_structure', 
            'growth_rate', 'access_patterns', 'query_complexity',
            'estimated_size_gb', 'relationships_count', 'queries_count'
        ]
    
    def get_relationships_count(self, obj):
        return obj.relationships_from.filter(is_active=True).count() + obj.relationships_to.filter(is_active=True).count()
    
    def get_queries_count(self, obj):
        return obj.queries.count()
    
class DatasetCloneSerializer(serializers.Serializer):
    new_name = serializers.CharField(max_length=255)
    include_relationships = serializers.BooleanField(default=False)
    include_queries = serializers.BooleanField(default=True)
    
    def validate_new_name(self, value):
        # ensure new name is unique
        if DatasetBaseModel.objects.filter(name=value).exists():
            raise serializers.ValidationError("A dataset with this name already exists")
        return value
    
class BulkImportSerializer(serializers.Serializer):
    datasets = serializers.ListField(
        child=DatasetCreateSerializer(),
        min_length=1,
        max_length=100  # limit bulk imports
    )
    
    def validate_datasets(self, value):
        # validate that all dataset names are unique
        names = [dataset_data.get('name') for dataset_data in value]
        if len(names) != len(set(names)):
            raise serializers.ValidationError("All dataset names must be unique")
        
        # Check against existing datasets
        existing_names = set(DatasetBaseModel.objects.filter(name__in=names).values_list('name', flat=True))
        duplicate_names = set(names) & existing_names
        if duplicate_names:
            raise serializers.ValidationError(f"These dataset names already exist: {', '.join(duplicate_names)}")
        
        return value

class DatasetAnalysisSerializer(serializers.Serializer):
    dataset_id = serializers.IntegerField(read_only=True)
    dataset_name = serializers.CharField(read_only=True)
    total_relationships = serializers.IntegerField(read_only=True)
    total_queries = serializers.IntegerField(read_only=True)
    performance_score = serializers.FloatField(read_only=True)
    recommendations = serializers.ListField(
        child=serializers.CharField(),
        read_only=True
    )
    optimization_suggestions = serializers.ListField(
        child=serializers.DictField(),
        read_only=True
    )