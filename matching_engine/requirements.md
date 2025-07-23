# Matching Engine Requirements

## Context and Purpose

The Matching Engine is the core component of the Django project ddrs: `ddrs_api/matching_engine`.
It provides AI-powered recommendations for optimal dataset-datastore combinations using Ollama models. The engine analyzes dataset characteristics and datastore capabilities to suggest the best matches for performance optimization.

Input consists of datasets from `ddrs_api/dataset_api` and datastores from `ddrs_api/datastore_api`. Output is a ranked list of dataset-datastore combinations with confidence scores and reasoning.

## Matching Request Properties

A Matching Request contains the following properties:

- **id**: Auto-generated identifier for the matching request
- **created_at**: Auto-generated timestamp when request was created
- **updated_at**: Auto-generated timestamp when request was last modified
- **status**: Request processing status - choices: `pending`, `processing`, `completed`, `failed`
- **system_prompt**: System prompt sent to the AI model defining its role and constraints
- **prompt**: User prompt containing dataset and datastore information for analysis
- **model_name**: Name of the Ollama model used for matching
- **datasets**: Many-to-many relationship to selected datasets
- **datastores**: Many-to-many relationship to available datastores
- **matching_result**: JSON array of recommended [dataset_id, datastore_id] pairs
- **confidence**: Overall confidence score (0-100) for the recommendations
- **reason**: AI-generated explanation for the recommendations
- **processing_time_seconds**: Time taken to process the request
- **error_message**: Error details if processing failed

## Individual Recommendation Properties

Each recommendation within a matching request contains:

- **request_id**: Foreign key to the matching request
- **dataset_id**: Foreign key to the recommended dataset
- **datastore_id**: Foreign key to the recommended datastore
- **confidence**: Confidence score (0-100) for this specific recommendation
- **reason**: Detailed reasoning for this dataset-datastore pairing
- **priority**: Ranking within the recommendation set (1 = highest priority)

## AI Model Integration

### Ollama Configuration
- **Base URL**: Configurable Ollama server endpoint
- **Timeout**: Maximum processing time (default: 300 seconds)
- **Available Models**: List of models supported for matching
- **Default Model**: Fallback model if none specified

### Model Requirements
Models must be capable of:
- Analyzing structured data about datasets and datastores
- Understanding database performance characteristics
- Generating JSON-formatted recommendations
- Providing confidence scores and reasoning

## API Endpoints

### Matching Operations
- **POST** `/api/v1/matching/match/` - Create new matching request
- **GET** `/api/v1/matching/requests/` - List all matching requests
- **GET** `/api/v1/matching/requests/{id}/` - Get specific request details
- **DELETE** `/api/v1/matching/requests/{id}/` - Delete matching request
- **GET** `/api/v1/matching/requests/{id}/status/` - Get current processing status

### Model Management
- **GET** `/api/v1/matching/models/available/` - List available Ollama models
- **GET** `/api/v1/matching/system-prompt/default/` - Get default system prompt
- **PUT** `/api/v1/matching/system-prompt/default/` - Update default system prompt

### Recommendations
- **GET** `/api/v1/matching/recommendations/` - List all recommendations with filtering
- **GET** `/api/v1/matching/requests/{id}/recommendations/` - Get recommendations for specific request

## Default System Prompt

```text
You are an expert database architect and performance engineer. Your task is to analyze datasets and datastores to recommend optimal combinations for performance, scalability, and maintainability.

Consider these factors:
1. Data structure compatibility (structured vs unstructured)
2. Query patterns and complexity 
3. Growth rate and scalability requirements
4. Relationship complexity between datasets
5. Performance characteristics of each datastore type

Provide recommendations as JSON with this structure:
{
  "recommendations": [
    {
      "dataset_id": <id>,
      "datastore_id": <id>, 
      "confidence": <0-100>,
      "reason": "<detailed explanation>"
    }
  ],
  "overall_confidence": <0-100>,
  "summary": "<overall reasoning>"
}

Prioritize data integrity, query performance, and future scalability.
```

## Typical Workflow

1. **Request Creation**
   - User selects datasets (minimum 1 required)
   - User selects datastores (minimum 1 required)  
   - User chooses AI model from available options
   - User accepts default system prompt or provides custom one

2. **Validation**
   - Verify all required fields are present
   - Ensure selected datasets and datastores exist
   - Validate model availability
   - Check system prompt is not empty

3. **Processing**
   - Status changes to `processing`
   - Generate detailed prompt with dataset/datastore characteristics
   - Send request to Ollama model
   - Parse AI response into structured recommendations

4. **Response Handling**
   - **Success**: Parse recommendations, calculate confidence, save results
   - **Failure**: Log error, set status to `failed`, store error message

5. **Result Delivery**
   - Return recommendations ranked by priority
   - Include confidence scores and reasoning
   - Provide overall recommendation summary

## Request Payload Example

```json
{
  "dataset_ids": [1, 2, 3],
  "datastore_ids": [1, 2],
  "model_name": "qwen2.5:32b",
  "system_prompt": "You are a database engineer...",
  "custom_prompt": "Focus on performance optimization for high-traffic applications"
}
```

## Response Example

```json
{
  "id": 1,
  "status": "completed",
  "created_at": "2025-01-15T10:30:00Z",
  "processing_time_seconds": 45.2,
  "model_name": "qwen2.5:32b",
  "overall_confidence": 78.5,
  "summary": "Neo4J recommended for hierarchical data due to complex relationships, PostgreSQL for transactional employee data",
  "recommendations": [
    {
      "dataset_id": 1,
      "datastore_id": 2,
      "dataset_name": "employees", 
      "datastore_name": "Neo4J Graph DB",
      "confidence": 85.0,
      "reason": "Employee hierarchy data with manager relationships is ideal for graph traversal queries",
      "priority": 1
    },
    {
      "dataset_id": 2,
      "datastore_id": 1,
      "dataset_name": "departments",
      "datastore_name": "PostgreSQL Primary", 
      "confidence": 72.0,
      "reason": "Structured department data with ACID requirements fits SQL database model",
      "priority": 2
    }
  ]
}
```

## Error Handling

### Common Error Scenarios
- **Model unavailable**: Return list of available models
- **Ollama timeout**: Retry with exponential backoff (max 3 attempts)
- **Invalid AI response**: Log raw response, return parsing error
- **No datasets/datastores selected**: Validation error with clear message
- **Model response malformed**: Attempt to extract partial results

### Error Response Format
```json
{
  "error": "Model timeout after 300 seconds",
  "error_code": "OLLAMA_TIMEOUT",
  "retry_after": 60,
  "available_models": ["qwen2.5:7b", "llama3.1:8b"]
}
```

## Performance Considerations

1. **Async Processing**: Use background tasks for long-running requests
2. **Caching**: Cache model availability and system prompts
3. **Rate Limiting**: Limit concurrent Ollama requests
4. **Timeout Handling**: Graceful degradation for slow models
5. **Result Storage**: Persist results for analysis and comparison

## Validation Rules

1. **Minimum selections**: At least 1 dataset and 1 datastore required
2. **Model validation**: Verify model exists in Ollama before processing
3. **Prompt length**: System prompt maximum 2000 characters
4. **JSON validation**: Ensure AI response is valid JSON
5. **Confidence range**: All confidence scores must be 0-100
6. **Dataset/datastore existence**: Validate all referenced IDs exist

## Implementation Notes

1. Use async HTTP client for Ollama communication
2. Implement proper JSON schema validation for AI responses
3. Add comprehensive logging for debugging AI interactions
4. Consider implementing request queuing for high load
5. Store raw AI responses for analysis and model improvement
6. Implement backup/fallback strategies for Ollama unavailability