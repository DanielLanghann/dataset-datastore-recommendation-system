from rest_framework import serializers


from .models import DatasetBaseModel

class DatasetBaseSerializer(serializers.ModelSerializer):

    class Meta:
        model = DatasetBaseModel
        fields = [
            'id', 'created_at', 'updated_at'
        ]
        read_only_fields =  ['id', 'created_at', 'updated_at']

class DatasetDetailSerializer(DatasetBaseSerializer):

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + [
            "name"
        ]

class DatasetCreateSerializer(DatasetBaseSerializer):

    class Meta(DatasetBaseSerializer.Meta):
        fields = DatasetBaseSerializer.Meta.fields + [
            "name"
        ]  

    

class DatasetListSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetBaseModel
        fields = ["id", "created_at", "updated_at", "name"]
      

