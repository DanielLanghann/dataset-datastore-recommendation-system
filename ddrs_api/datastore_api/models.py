from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from cryptography.fernet import Fernet

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
    _encrypted_password = models.TextField(blank=True, null=True, help_text="Encrypted database password")
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

    def _get_encryption_key(self):
        encryption_key = getattr(settings, 'DATASTORE_ENCRYPTION_KEY', None)
        
        if not encryption_key:
            raise ImproperlyConfigured(
                "DATASTORE_ENCRYPTION_KEY must be set in Django settings."
            )
        
        # ensure key is bytes
        if isinstance(encryption_key, str):
            encryption_key = encryption_key.encode()
            
        return encryption_key
    
    def _get_cipher(self):
        key = self._get_encryption_key()
        return Fernet(key)
    
    @property
    def password(self):
        if not self._encrypted_password:
            return None
        
        try:
            cipher = self._get_cipher()
            decrypted_bytes = cipher.decrypt(self._encrypted_password.encode())
            return decrypted_bytes.decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {str(e)}")
    
    @password.setter
    def password(self, value):
        if value is None:
            self._encrypted_password = None
        else:
            try:
                cipher = self._get_cipher()
                encrypted_bytes = cipher.encrypt(value.encode())
                self._encrypted_password = encrypted_bytes.decode()
            except Exception as e:
                raise ValueError(f"Failed to encrypt password: {str(e)}")
    
    def set_password(self, password):
        self.password = password
    
    def get_decrypted_password(self):
        return self.password
    
    def has_password(self):
        return bool(self._encrypted_password)
    
    def get_masked_connection_info(self):
        # return connection info with masked data
        info = {
            "server": self.server,
            "port": self.port,
            "username": self.username,
            "has_password": self.has_password(),
            "connection_string_provided": bool(self.connection_string)
        }
        return info
    
    @property
    def characteristics(self):
        return {
            "type": self.get_type_display(), # will be added by Django thx to the choices prop
            "system": self.get_system_display(), # same
            "max_connections": self.max_connections,
            "avg_response_time": self.avg_response_time_ms,
            "storage_capacity_gb": self.storage_capacity_gb, 
        }


    
    def __str__(self):
        return f"{self.name} ({self.get_type_display()} - {self.get_system_display()})"
    
    class Meta:
        verbose_name = "Datastore"
        verbose_name_plural = "Datastores"
        ordering = ['name']