# Dataset API Requirements

## Context and Purpose

The Dataset API is an app that is part of the Django project ddrs: `ddrs_api/dataset_api`.
The Dataset API provides endpoints where users can create, read, update, and delete datasets. All stored datasets can be compared and matched with available datastores through the AI Matching Engine.

Datastores are managed by `ddrs_api/datastore_api` and matching is performed by `ddrs_api/matching_engine`.

## Properties of a Dataset

A Dataset contains the following properties:

- **id**: Auto-generated identifier of the dataset
- **created_at**: Auto-generated timestamp when dataset was created
- **updated_at**: Auto-generated timestamp when dataset was last modified
- **name**: Name of the dataset (typically table or collection name)
- **short_description**: Brief text description of the dataset's purpose
- **current_datastore_id**: Foreign key to the currently assigned datastore (nullable for new datasets)
- **data_structure**: Structure type - choices: `structured`, `semi_structured`, `unstructured`
- **growth_rate**: Data growth rate - choices: `high`, `medium`, `low`
- **access_patterns**: Primary access pattern - choices: `read_heavy`, `write_heavy`, `read_write_heavy`, `analytical`, `transactional`
- **query_complexity**: Typical query complexity - choices: `high`, `medium`, `low`
- **properties**: JSON list of column names or data properties
- **sample_data**: JSON list containing sample data rows
- **estimated_size_gb**: Estimated dataset size in gigabytes (optional)
- **avg_query_time_ms**: Average query execution time in milliseconds (optional)
- **queries_per_day**: Estimated number of queries per day (optional)

## Dataset Relationships

Relationships between datasets are managed through a separate model:

- **from_dataset_id**: Source dataset ID
- **to_dataset_id**: Target dataset ID  
- **relationship_type**: Type of relationship - choices: `foreign_key`, `one_to_many`, `many_to_many`, `dependency`, `similarity`
- **strength**: Relationship importance (1-10 scale)
- **description**: Optional description of the relationship

## Dataset Queries

Query examples are stored separately to allow multiple queries per dataset:

- **dataset_id**: Foreign key to the dataset
- **name**: Descriptive name for the query
- **query_text**: The actual query/command text
- **query_type**: Type of query - choices: `select`, `insert`, `update`, `delete`, `complex`, `aggregate`
- **frequency**: How often this query runs - choices: `high`, `medium`, `low`
- **avg_execution_time_ms**: Average execution time (optional)
- **description**: Optional query description

## API Endpoints

### Dataset CRUD Operations
- **GET** `/api/v1/datasets/` - List all datasets with filtering and pagination
- **POST** `/api/v1/datasets/` - Create a new dataset
- **GET** `/api/v1/datasets/{id}/` - Retrieve specific dataset with relationships and queries
- **PUT** `/api/v1/datasets/{id}/` - Update entire dataset
- **PATCH** `/api/v1/datasets/{id}/` - Partially update dataset
- **DELETE** `/api/v1/datasets/{id}/` - Delete dataset

### Dataset Relationships
- **GET** `/api/v1/datasets/{id}/relationships/` - Get all relationships for a dataset
- **POST** `/api/v1/relationships/` - Create new relationship between datasets
- **PUT** `/api/v1/relationships/{id}/` - Update relationship
- **DELETE** `/api/v1/relationships/{id}/` - Delete relationship

### Dataset Queries  
- **GET** `/api/v1/datasets/{id}/queries/` - Get all queries for a dataset
- **POST** `/api/v1/queries/` - Add new query to a dataset
- **PUT** `/api/v1/queries/{id}/` - Update query
- **DELETE** `/api/v1/queries/{id}/` - Delete query

### Additional Features
- **POST** `/api/v1/datasets/{id}/clone/` - Clone dataset with new name
- **GET** `/api/v1/datasets/{id}/analysis/` - Get dataset analysis and basic recommendations
- **POST** `/api/v1/datasets/bulk-import/` - Import multiple datasets from JSON/CSV

## Filtering and Search
- **Filter by**: `data_structure`, `growth_rate`, `access_patterns`, `current_datastore_id`
- **Search in**: `name`, `short_description`
- **Order by**: `created_at`, `name`, `estimated_size_gb`

## Validation Rules

1. **Required fields**: `name`, `short_description`, `data_structure`, `growth_rate`, `access_patterns`, `query_complexity`
2. **Unique constraints**: `name` must be unique across all datasets
3. **JSON validation**: `properties` and `sample_data` must be valid JSON lists
4. **Relationship validation**: Cannot create circular dependencies
5. **Query validation**: Basic SQL syntax validation for `query_text`
6. **Size constraints**: `name` max 255 characters, `short_description` max 1000 characters

## Example Dataset

```json
{
  "id": 1,
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "name": "employees",
  "short_description": "Employee master data with personal and employment information",
  "current_datastore_id": 1,
  "data_structure": "structured",
  "growth_rate": "medium",
  "access_patterns": "read_heavy",
  "query_complexity": "medium",
  "properties": ["emp_id", "first_name", "last_name", "email", "department_id", "hire_date"],
  "sample_data": [
    [1, "John", "Doe", "john.doe@company.com", 2, "2022-01-15"],
    [2, "Jane", "Smith", "jane.smith@company.com", 1, "2021-06-01"]
  ],
  "estimated_size_gb": 0.5,
  "avg_query_time_ms": 25.5,
  "queries_per_day": 1500
}
```

## Response Formats

All API responses follow consistent format:
- **Success**: Return data with HTTP 200/201
- **Validation Error**: HTTP 400 with detailed field errors
- **Not Found**: HTTP 404 with error message
- **Server Error**: HTTP 500 with generic error message

## Implementation Notes

1. Use Django REST Framework ViewSets for consistent CRUD operations
2. Implement proper serializers with validation
3. Add pagination for list endpoints (default 50 items per page)
4. Include related data in detail views (relationships, queries)
5. Ensure proper foreign key constraints and cascading deletes