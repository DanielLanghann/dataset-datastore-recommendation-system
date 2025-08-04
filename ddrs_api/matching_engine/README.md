# AI Matching Engine

The AI Matching Engine takes as input a set of datasets and datastores and provides recommendations on how to distribute the datasets to available datastores.

## API

### Input
- **Dataset Input**: Datasets with related queries and relationships
- **Datastore Input**: Datastores with related properties

### Output
For every dataset in the request:
- `dataset`: ID of the dataset
- `datastore`: ID of the recommended datastore
- `reason`: A short description of why this recommendation was made
- `confidence`: A confidence score (0.0-1.0) related to the recommendation

### Example Request
```json
{
    "datasets": [
        {
            "id": 1,
            "name": "User Analytics Data",
            "data_structure": "structured",
            "size_gb": 150,
            "growth_rate": "high",
            "access_patterns": ["read_heavy", "analytical"],
            "query_complexity": "medium",
            "queries": [
                {
                    "id": 1,
                    "query_type": "SELECT",
                    "frequency": "high",
                    "description": "Daily user metrics aggregation"
                }
            ]
        },
        {
            "id": 2,
            "name": "Product Catalog",
            "data_structure": "semi_structured",
            "size_gb": 25,
            "growth_rate": "low",
            "access_patterns": ["read_write_heavy"],
            "query_complexity": "low"
        }
    ],
    "datastores": [
        {
            "id": 1,
            "name": "Main PostgreSQL",
            "type": "sql",
            "system": "postgres",
            "performance_tier": "high",
            "storage_capacity_gb": 1000,
            "current_usage_gb": 300
        },
        {
            "id": 2,
            "name": "MongoDB Cluster",
            "type": "document",
            "system": "mongodb",
            "performance_tier": "medium",
            "storage_capacity_gb": 500,
            "current_usage_gb": 100
        }
    ],
    "system_prompt": "You are an expert database architect...",
    "prompt": "Recommend the best datastore for each dataset based on performance and compatibility.",
    "model": "llama3.1:8b"
}
```

### Example Response
```json
{
    "id": 123,
    "created_at": "2025-08-04T10:30:00Z",
    "request_id": 456,
    "result": [
        {
            "dataset": 1,
            "datastore": 1,
            "reason": "PostgreSQL is optimal for structured analytical workloads with high read frequency",
            "confidence": 0.92
        },
        {
            "dataset": 2,
            "datastore": 2,
            "reason": "MongoDB handles semi-structured data efficiently for read-write operations",
            "confidence": 0.85
        }
    ],
    "model": "llama3.1:8b"
}
```

## Persistence

### Requests
Every request will be stored in the database with the following information:
- `id`: Unique identifier
- `created_at`: Timestamp of request creation
- `related_datasets`: JSON object with all datasets including related queries and relationships
- `related_datastores`: JSON object with all related datastores
- `system_prompt`: The system prompt used for the LLM
- `prompt`: The user prompt used for the request
- `requested_model`: The Ollama model used for processing

### Responses
Every response related to a request will be stored in the database with the following information:
- `id`: Unique identifier of the response
- `created_at`: Timestamp of response creation
- `request_id`: ID of the related request
- `result`: The matching results as a JSON object containing a list with:
  - Dataset IDs
  - Matched datastore IDs
  - Reasons for recommendations
  - Confidence scores
- `model`: The model used to generate the response

## Ollama Integration

We use Ollama as our gateway to LLM models. The following API features are provided:

### Features
- **Model Discovery**: Retrieve available models from Ollama API
- **Model Selection**: Select a downloaded model from Ollama for your request
- **System Prompt Configuration**: Define the system prompt or use the default prompt
- **Prompt Customization**: Define the user prompt or use the default prompt
- **Dataset and Datastore Selection**: Select datasets and datastores to match
- **Request Processing**: Make a request with the system prompt, user prompt, selected datasets and datastores (see Requests section above)
- **Response Retrieval**: Get the matching recommendations

### Ollama API Endpoints
- **List Models**: `GET http://localhost:11434/api/tags`
- **Generate Completion**: `POST http://localhost:11434/api/generate`
- **Chat Completion**: `POST http://localhost:11434/api/chat`
- **Model Information**: `POST http://localhost:11434/api/show`

### Example Integration Code
```python
import requests

def get_available_models():
    """Get list of available Ollama models"""
    try:
        response = requests.get("http://localhost:11434/api/tags")
        response.raise_for_status()
        return response.json()["models"]
    except requests.RequestException as e:
        print(f"Error fetching models: {e}")
        return []

def generate_recommendation(prompt, model="llama3.1:8b"):
    """Generate recommendation using Ollama"""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        return response.json()["response"]
    except requests.RequestException as e:
        print(f"Error generating recommendation: {e}")
        return None
```

### Supported Models
The system supports any Ollama-compatible model. You can retrieve available models using the Ollama API:

**GET Available Models:**
```bash
curl http://localhost:11434/api/tags
```

**Example Response:**
```json
{
  "models": [
    {
      "name": "llama3.1:8b",
      "model": "llama3.1:8b",
      "modified_at": "2025-08-04T10:30:00Z",
      "size": 4661224676,
      "digest": "sha256:abc123...",
      "details": {
        "parent_model": "",
        "format": "gguf",
        "family": "llama",
        "families": ["llama"],
        "parameter_size": "8B",
        "quantization_level": "Q4_0"
      }
    },
    {
      "name": "llama3.1:70b",
      "model": "llama3.1:70b",
      "modified_at": "2025-08-04T09:15:00Z",
      "size": 39019716179,
      "digest": "sha256:def456...",
      "details": {
        "parent_model": "",
        "format": "gguf",
        "family": "llama",
        "families": ["llama"],
        "parameter_size": "70B",
        "quantization_level": "Q4_0"
      }
    }
  ]
}
```

**Common Models:**
- `llama3.1:8b` - General purpose, good balance of speed and accuracy
- `llama3.1:70b` - High accuracy for complex matching scenarios
- `codellama:13b` - Optimized for technical/structured data analysis

### Default Prompts
- **System Prompt**: "You are an expert database architect with deep knowledge of different database systems, their strengths, limitations, and optimal use cases. Analyze the provided datasets and datastores to make informed recommendations."
- **User Prompt**: "Based on the dataset characteristics (structure, size, growth rate, access patterns, query complexity) and available datastores (type, system, performance, capacity), recommend the optimal datastore for each dataset. Provide clear reasoning and confidence scores."

## Implementation Considerations

### Requirements
1. **Data Validation**: Input datasets and datastores must be validated against existing models
2. **Ollama Integration**: Requires Ollama service to be running and accessible (default: `http://localhost:11434`)
3. **Model Management**: Selected models must be downloaded and available in Ollama (use `ollama pull <model>`)
4. **Model Discovery**: Implement endpoint to fetch available models from Ollama API
5. **Response Parsing**: LLM responses need structured parsing to extract recommendations
6. **Error Handling**: Graceful handling of LLM failures, timeouts, and invalid responses

### Performance Considerations
- **Request Timeout**: Implement reasonable timeouts for LLM requests (30-60 seconds)
- **Batch Processing**: Support processing multiple dataset-datastore combinations
- **Caching**: Consider caching similar requests to improve response times
- **Model Selection**: Balance between model accuracy and response time

### Confidence Score Interpretation
- **0.9-1.0**: High confidence - Strong match based on multiple factors
- **0.7-0.8**: Medium confidence - Good match with some trade-offs
- **0.5-0.6**: Low confidence - Acceptable match but consider alternatives
- **0.0-0.4**: Very low confidence - Recommendation may not be optimal





 


