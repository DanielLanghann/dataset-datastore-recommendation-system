# Datastore API Requirements

## Context and Purpose

The Datastore API is an app that is part of the Django project ddrs: `ddrs_api/datastore_api`.
The Datastore API provides endpoints where users can create, read, update, and delete datastores. All stored datastores can be matched with datasets through the AI Matching Engine.

Datasets are managed by `ddrs_api/dataset_api` and matching is performed by `ddrs_api/matching_engine`.

## Properties of a Datastore

A Datastore contains the following properties:

- **id**: Auto-generated identifier of the datastore
- **created_at**: Auto-generated timestamp when datastore was created
- **updated_at**: Auto-generated timestamp when datastore was last modified
- **name**: Descriptive name for the datastore instance
- **type**: Type of datastore - choices: `sql`, `document`, `keyvalue`, `graph`, `column`
- **system**: Specific database system - choices: `postgres`, `mysql`, `mongodb`, `redis`, `neo4j`, `cassandra`
- **description**: Detailed description of the datastore capabilities and use cases
- **server**: Server hostname or IP address
- **port**: Port number for database connection
- **username**: Database username for authentication
- **password**: Database password (stored encrypted)
- **connection_string**: Alternative to individual connection parameters (optional)
- **is_active**: Boolean flag indicating if datastore is available for matching
- **max_connections**: Maximum concurrent connections supported (optional)
- **avg_response_time_ms**: Average response time in milliseconds (optional)
- **storage_capacity_gb**: Total storage capacity in gigabytes (optional)

## Datastore Type Characteristics

### SQL Databases (`sql`)
- **Systems**: `postgres`, `mysql`
- **Best for**: Structured data, ACID transactions, complex queries, relationships
- **Limitations**: Vertical scaling, schema rigidity

### Document Stores (`document`)  
- **Systems**: `mongodb`
- **Best for**: Semi-structured data, flexible schemas, rapid development
- **Limitations**: Limited complex queries, eventual consistency

### Key-Value Stores (`keyvalue`)
- **Systems**: `redis`
- **Best for**: Caching, session storage, simple data structures
- **Limitations**: No complex queries, limited data types

### Graph Databases (`graph`)
- **Systems**: `neo4j`
- **Best for**: Highly connected data, relationship traversal, social networks
- **Limitations**: Steep learning curve, specialized use cases

### Column Stores (`column`)
- **Systems**: `cassandra`
- **Best for**: Time-series data, analytics, high write throughput
- **Limitations**: Limited query flexibility, eventual consistency

## API Endpoints

### Datastore CRUD Operations
- **GET** `/api/v1/datastores/` - List all datastores with filtering and pagination
- **POST** `/api/v1/datastores/` - Create a new datastore
- **GET** `/api/v1/datastores/{id}/` - Retrieve specific datastore details
- **PUT** `/api/v1/datastores/{id}/` - Update entire datastore
- **PATCH** `/api/v1/datastores/{id}/` - Partially update datastore
- **DELETE** `/api/v1/datastores/{id}/` - Delete datastore

### Additional Features
- **POST** `/api/v1/datastores/{id}/test-connection/` - Test database connectivity
- **GET** `/api/v1/datastores/types/` - Get available datastore types and systems
- **GET** `/api/v1/datastores/{id}/current-datasets/` - List datasets currently assigned to this datastore
- **GET** `/api/v1/datastores/{id}/performance/` - Get performance metrics and capacity info

## Filtering and Search
- **Filter by**: `type`, `system`, `is_active`
- **Search in**: `name`, `description`
- **Order by**: `created_at`, `name`, `type`, `avg_response_time_ms`

## Validation Rules

1. **Required fields**: `name`, `type`, `system`, `description`
2. **Unique constraints**: `name` must be unique across all datastores
3. **Type-system validation**: Ensure `system` is valid for the selected `type`
4. **Connection validation**: Either provide `server`/`port`/`username`/`password` OR `connection_string`
5. **Port validation**: Must be between 1 and 65535
6. **Performance metrics**: All optional numeric fields must be positive

## Connection Testing

The test connection endpoint validates:
- Network connectivity to the datastore
- Authentication credentials
- Basic query execution (e.g., `SELECT 1` for SQL databases)
- Response time measurement

## Security Considerations

1. **Password encryption**: Store passwords using symmetric encryption
2. **Connection string masking**: Never return full connection strings in API responses
3. **Credential validation**: Validate credentials during creation/update
4. **Access control**: Only return sensitive connection details to authorized users

## Example Datastore

```json
{
  "id": 1,
  "created_at": "2025-01-15T10:00:00Z",
  "updated_at": "2025-01-15T10:00:00Z",
  "name": "Primary PostgreSQL",
  "type": "sql",
  "system": "postgres",
  "description": "Main relational database for structured data with ACID compliance. Optimized for complex queries and data integrity.",
  "server": "db.company.com",
  "port": 5432,
  "username": "app_user",
  "password": "***encrypted***",
  "connection_string": null,
  "is_active": true,
  "max_connections": 100,
  "avg_response_time_ms": 15.2,
  "storage_capacity_gb": 1000
}
```

## Datastore Types Response

```json
{
  "types": [
    {
      "value": "sql",
      "label": "SQL Database",
      "systems": [
        {"value": "postgres", "label": "PostgreSQL"},
        {"value": "mysql", "label": "MySQL"}
      ],
      "characteristics": {
        "strengths": ["ACID compliance", "Complex queries", "Data integrity"],
        "limitations": ["Vertical scaling", "Schema rigidity"],
        "best_for": ["Structured data", "Financial systems", "CRM applications"]
      }
    },
    {
      "value": "graph",
      "label": "Graph Database", 
      "systems": [
        {"value": "neo4j", "label": "Neo4J"}
      ],
      "characteristics": {
        "strengths": ["Relationship traversal", "Connected data", "Pattern matching"],
        "limitations": ["Specialized queries", "Learning curve"],
        "best_for": ["Social networks", "Recommendation engines", "Fraud detection"]
      }
    }
  ]
}
```

## Connection Test Response

```json
{
  "success": true,
  "response_time_ms": 12.5,
  "message": "Connection successful",
  "details": {
    "server_version": "PostgreSQL 14.2",
    "database_name": "production_db",
    "connected_at": "2025-01-15T10:30:00Z"
  }
}
```

## Error Handling

- **Connection failed**: Return specific error message (timeout, auth failure, etc.)
- **Invalid configuration**: Detailed validation errors
- **Datastore in use**: Prevent deletion if datasets are assigned
- **Encryption errors**: Generic error message for security

## Implementation Notes

1. Use Django's encryption utilities for password storage
2. Implement connection pooling for test connections
3. Cache datastore types and systems data
4. Add monitoring for datastore health and performance
5. Consider implementing connection string templates for different systems
6. Validate datastore configurations before saving
7. Implement soft delete to preserve historical matching data