from rest_framework import serializers
from .models import DatasetBaseModel, DatasetQueriesModel, DatasetRelationshipModel

class DatasetBaseSerializer(serializers.ModelSerializer):
    """
    Serializer class for handling the base dataset model.
    """

    class Meta:
        model = DatasetBaseModel
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'short_description',
            'current_datastore', 'data_structure', 'growth_rate',
            'access_patterns', 'query_complexity', 'properties', 'sample_data',
            'estimated_size_gb', 'avg_query_time_ms', 'queries_per_day'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

    def validate_properties(self, value):
        """
        Validate that the properties field is a list.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("Properties must be a list")
        return value

    def validate_sample_data(self, value):
        """
        Validate that the sample_data field is a nested list.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("Sample data must be a list")

        for row in value:
            if not isinstance(row, list):
                raise serializers.ValidationError("Each sample data must be a list")
        return value

class DatasetRelationshipSerializer(serializers.ModelSerializer):
    """
    Serializer class for handling dataset relationships.
    """
    from_dataset_name = serializers.CharField(source="from_dataset.name", read_only=True)
    to_dataset_name = serializers.CharField(source="to_dataset.name", read_only=True)

    class Meta:
        model = DatasetRelationshipModel
        fields = [
            'id', 'created_at', 'updated_at', 'from_dataset', 'to_dataset',
            'from_dataset_name', 'to_dataset_name', 'relationship_type',
            'strength', 'description', 'is_active'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'from_dataset_name', 'to_dataset_name']

class DatasetQueriesSerializer(serializers.ModelSerializer):
    """
    Serializer class for handling dataset queries.
    """

    class Meta:
        model = DatasetQueriesModel
        fields = [
            "id", "created_at", "updated_at", "dataset", "name",
            "query_text", "query_type", "frequency", "avg_execution_time_ms", "description"
        ]
        read_only_fields = ["id", "created_at", "updated_at"]

class DatasetDetailSerializer(DatasetBaseSerializer):
    """
    Serializer class for detailed dataset information.
    Extends the DatasetBaseSerializer to include relationships and queries associated with the dataset.
    """
    relationships_from = DatasetRelationshipSerializer(many=True, read_only=True)
    relationships_to = DatasetRelationshipSerializer(many=True, read_only=True)
    queries = DatasetQueriesSerializer(many=True, read_only=True)

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + [
            'relationships_from', 'relationships_to', 'queries'
        ]

class DatasetCreateSerializer(DatasetBaseSerializer):
    """
    Serializer class for creating datasets.
    Extends the DatasetBaseSerializer to include relationships and queries that can be created
    along with the dataset.
    """
    relationships = DatasetRelationshipSerializer(many=True, required=False)
    queries = DatasetQueriesSerializer(many=True, required=False)

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + ["relationships", "queries"]

    def create(self, validated_data):
        """
        Create a new dataset along with its relationships and queries.
        """
        relationships_data = validated_data.pop("relationships", [])
        queries_data = validated_data.pop("queries", [])

        # Create dataset
        dataset = DatasetBaseModel.objects.create(**validated_data)

        # Create relationships
        for relationship_data in relationships_data:
            relationship_data["from_dataset"] = dataset
            DatasetRelationshipModel.objects.create(**relationship_data)

        # Create Queries
        for query_data in queries_data:
            query_data["dataset"] = dataset
            DatasetQueriesModel.objects.create(**query_data)

        return dataset

class DatasetListSerializer(serializers.ModelSerializer):
    """
    Serializer class for listing datasets.
    Includes fields for basic dataset information along with counts of relationships and queries.
    """
    datastore_name = serializers.CharField(source="current_datastore.name", read_only=True)
    relationships_count = serializers.SerializerMethodField()
    queries_count = serializers.SerializerMethodField()

    class Meta:
        model = DatasetBaseModel
        fields = [
            'id', 'created_at', 'updated_at', 'name', 'short_description',
            'current_datastore', 'datastore_name', 'data_structure',
            'growth_rate', 'access_patterns', 'query_complexity',
            'estimated_size_gb', 'relationships_count', 'queries_count'
        ]

    def get_relationships_count(self, obj):
        return obj.relationships_from.filter(is_active=True).count() + obj.relationships_to.filter(is_active=True).count()

    def get_queries_count(self, obj):
        return obj.queries.count()