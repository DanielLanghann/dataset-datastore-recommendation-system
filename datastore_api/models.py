from django.db import models

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from cryptography.fernet import Fernet
from django.conf import settings
import base64
from validators import DatastoreValidator

class DataStoreManager(models.Manager):
    def active(self):
        return self.filter(is_active=True)
    
    def by_type(self, datastore_type):
        return self.filter(type=datastore_type)
    
class Datastore(models.Model):

    TYPE_CHOICES = [
        ('sql', 'SQL Database'),
        ('document', 'Document Store'),
        ('keyvalue', 'Key-Value Store'),
        ('graph', 'Graph Database'),
        ('column', 'Column Store'),
    ]

    SYSTEM_CHOICES = [
        ('postgres', 'PostgreSQL'),
        ('mysql', 'MySQL'),
        ('mongodb', 'MongoDB'),
        ('redis', 'Redis'),
        ('neo4j', 'Neo4J'),
        ('cassandra', 'Cassandra'),
    ]

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Required fields
    name = models.CharField(max_length=255, unique=True, help_text="Descriptive name for the datastore instance")
    type = models.CharField(max_length=20, choices=TYPE_CHOICES, help_text="Type of datastore")
    system = models.CharField(max_length=20, choices=SYSTEM_CHOICES, help_text="Specific database system")
    description = models.TextField(help_text="Detailed description of the datastore capabilities and use cases")

    # Connection details
    server = models.CharField(max_length=255, blank=True, null=True, help_text="Server hostname or IP address")
    port = models.PositiveIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1), MaxValueValidator(65535)],
        help_text="Port number for database connection"
    )
    username = models.CharField(max_length=255, blank=True, null=True, help_text="Database username for authentication")
    password = models.TextField(blank=True, null=True, help_text="Database password for authentication")
    connection_string = models.TextField(blank=True, null=True, help_text="Alternative to individual connection parameters")

    # Status and performance
    is_active = models.BooleanField(default=True, help_text="Boolean Flag indicating if datastore is available for matching")
    max_connections = models.PositiveIntegerField(
        blank=True,
        null=True,
        help_text="Maximum concurrent connections supported"
    )
    avg_response_time_ms = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Average response time in milliseconds"
    )
    storage_capacity_gb = models.FloatField(
        blank=True,
        null=True,
        validators=[MinValueValidator(0)],
        help_text="Storage capacity in gigabytes"
    )

    objects = DataStoreManager()

    class Meta:
        db_table = "datastore_api_datastore"
        ordering = ["name"]
        indexes = [
            models.Index(fields=['type']),
            models.Index(fields=['system']),
            models.Index(fields=['is_active']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"{self.name ({self.get_type_display()} - {self.get_system_display()})}"
    
    def clean(self):
        super().clean()

        # Use centralized validation for business rules
        data = {
            'type': self.type,
            'system': self.system,
            'server': self.server,
            'port': self.port,
            'username': self.username,
            'connection_string': self.connection_string,
            'avg_response_time_ms': self.avg_response_time_ms,
            'storage_capacity_gb': self.storage_capacity_gb,
            'max_connections': self.max_connections,
        }
        try:
            DatastoreValidator.validate_datastore_data(data=data)
        except ValidationError as e:
            raise ValidationError({'__all__': str(e)})
            
    
    def save(self, *args, **kwargs):
        self.full_clean()

        # Encrypt password
        if self.password and not self._is_password_encrypted():
            self.password = self._encrypt_password(self.password)
        
        super().save(*args, **kwargs)
    
    def _encrypt_password(self, password):
        if not hasattr(settings, "DATASTORE_ENCRYPTION_KEY"):
            raise ValueError("DATASTORE_ENCRYPTION_KEY not found in settings")
        
        key = settings.DATASTORE_ENCRYPTION_KEY.encode()
        f = Fernet(key=key)
        encrypted_password = f.encrypt(password.encode())
        return base64.b64encode(encrypted_password).decode()
    
    def decrypt_password(self):
        if not self.password or not self._is_password_encrypted():
            return self.password
        
        try:
            key = settings.DATASTORE_ENCRYPTION_KEY.encode()
            f = Fernet(key=key)
            encrypted_password = base64.b64decode(self.password.encode())
            return f.decrypt(encrypted_password).decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {e}")
        
    def get_masked_connection_info(self):
        return {
            'server': self.server if self.server else None,
            'port': self.port if self.port else None,
            'username': self.username if self.username else None,
            'password': '***masked***' if self.password else None,
            'connection_string': '***masked***' if self.connection_string else None,
        }
    
    @classmethod
    def get_type_characteristics(cls):
        return {
            'sql': {
                'strengths': ['ACID compliance', 'Complex queries', 'Data integrity'],
                'limitations': ['Vertical scaling', 'Schema rigidity'],
                'best_for': ['Structured data', 'Financial systems', 'CRM applications']
            },
            'document': {
                'strengths': ['Flexible schemas', 'Horizontal scaling', 'JSON support', 'Write heavy access pattern'],
                'limitations': ['Limited complex queries', 'Eventual consistency', 'Data integrity'],
                'best_for': ['Content management', 'Catalogs', 'Real-time analytics']
            },
            'keyvalue': {
                'strengths': ['High performance', 'Simple operations', 'Caching'],
                'limitations': ['No complex queries', 'Limited data types', 'Data integrity'],
                'best_for': ['Session storage', 'Caching', 'Real-time recommendations']
            },
            'graph': {
                'strengths': ['Relationship traversal', 'Connected data', 'Pattern matching'],
                'limitations': ['Specialized queries', 'Learning curve'],
                'best_for': ['Social networks', 'Recommendation engines', 'Fraud detection']
            },
            'column': {
                'strengths': ['Time-series data', 'High write throughput', 'Compression'],
                'limitations': ['Limited query flexibility', 'Eventual consistency'],
                'best_for': ['Analytics', 'IoT data', 'Log aggregation']
            }
        }
    
    @property
    def charateristics(self):
        return self.get_type_characteristics().get(self.type, {})
    
    @classmethod
    def get_valid_type_system_combinations(cls):
        return DatastoreValidator.VALID_TYPE_SYSTEM_COMBINATIONS

        








