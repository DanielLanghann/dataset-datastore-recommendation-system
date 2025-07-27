from django.core.exceptions import ValidationError
from rest_framework import serializers

class DatastoreValidator:

    # Valid System Combis
    VALID_TYPE_SYSTEM_COMBINATIONS = {
        'sql': ['postgres', 'mysql'],
        'document': ['mongodb'],
        'keyvalue': ['redis'],
        'graph': ['neo4j'],
        'column': ['cassandra'],
    }

    @classmethod
    def validate_type_system_combination(cls, datastore_type, system):
        if datastore_type and system:
            valid_systems = cls.VALID_TYPE_SYSTEM_COMBINATIONS.get(datastore_type, [])
            if system not in valid_systems:
                raise ValidationError(
                    f"System '{system}' is not valid for type '{datastore_type}'. "
                    f"Valid systems: {', '.join(valid_systems)}"
                )
        else:
            print("Both type and system must be given!")

    @classmethod
    def validate_connection_parameters(cls, server, port, username, connection_string):
         
        has_individual_params = all([server, port, username])
        has_connection_string = bool(connection_string)

        if not has_individual_params and not connection_string:
            raise ValidationError(
                "Either provide individual server connection params or a connection string!"
            )
        
    @classmethod
    def validate_port_range(cls, port):
        if port is not None and (port < 1 or port > 65535):
            raise ValidationError(
                f"Port must be between 1 and 65535, got port {port}"
            )
        
    @classmethod
    def validate_performance_metrics(cls, avg_response_time, storage_capacity_gb, max_connections):
        if avg_response_time is not None and avg_response_time < 0:
            raise ValidationError("Average Response TIme must be positive.")
        
        if storage_capacity_gb is not None and storage_capacity_gb < 0:
            raise ValidationError("Storage Capacity must be positive.")
        
        if max_connections is not None and max_connections < 1:
            raise ValidationError("Maximum connections must be at least 1.")
        
    @classmethod
    def validate_datastore_data(cls, data):
        # Complete datastore validation

        cls.validate_type_system_combination(
            data.get("type"), data.get("system")
        )

        cls.validate_connection_parameters(
            data.get('server'),
            data.get('port'),
            data.get('username'),
            data.get('connection_string')
        )

        cls.validate_port_range(data.get('port'))
        
        cls.validate_performance_metrics(
            data.get('avg_response_time_ms'),
            data.get('storage_capacity_gb'),
            data.get('max_connections')
        )

    @classmethod
    def get_valid_systems_for_type(cls, datastore_type):
        return cls.VALID_TYPE_SYSTEM_COMBINATIONS.get(datastore_type, [])
    
    @classmethod
    def is_valid_type_system_combination(cls, datastore_type, system):
        if not datastore_type or not system:
            return False
        
        valid_systems = cls.VALID_TYPE_SYSTEM_COMBINATIONS.get(datastore_type, [])
        return system in valid_systems
    
class DatastoreBuisnessRules:

    @staticmethod
    def get_default_port_for_system(system):
        default_ports = {
            'postgres': 5432,
            'mysql': 3306,
            'mongodb': 27017,
            'redis': 6379,
            'neo4j': 7687,
            'cassandra': 9042,
        }
        return default_ports.get(system)
    
    @staticmethod
    def get_recommended_max_connections(system, storage_capacity_gb=None):
        base_recommendations = {
            'postgres': 100,
            'mysql': 150,
            'mongodb': 200,
            'redis': 10000,
            'neo4j': 50,
            'cassandra': 100,
        }
        
        base = base_recommendations.get(system, 100)
        if storage_capacity_gb:
            if storage_capacity_gb < 10:
                return max(base // 2, 10)
            elif storage_capacity_gb > 1000:
                return base * 2
        
        return base
    
    @staticmethod
    def is_high_performance_system(system):
        high_performance_systems = ['redis', 'cassandra']
        return system in high_performance_systems
    


   

      



