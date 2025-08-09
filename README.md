# Database
- Postgres is running in docker, you have access on http://localhost:5432
- PGADMIN is running in docker, you have access on http://localhost:5050

# Datastore API
## Features
- GET /datastores/ - List all datastores
- POST /datastores/ - Create new datastore
- GET /datastores/{id}/ - Get specific datastore
- PUT /datastores/{id}/ - Update datastore (full)
- PATCH /datastores/{id}/ - Update datastore (partial)
- DELETE /datastores/{id}/ - Delete datastore

# Ollama Model Synchronization

The system includes a Django management command to synchronize available models from Ollama and update the local cache.

## Usage

Navigate to the Django project directory and run:

```bash
# Basic synchronization (uses cache if available)
python manage.py sync_ollama_models

# Force refresh cache
python manage.py sync_ollama_models --force
```

## Features

- **Model Discovery**: Automatically discovers available models from Ollama
- **Health Check**: Verifies Ollama service status and response time
- **Cache Management**: Maintains a cache of available models for performance
- **Force Refresh**: Option to bypass cache and fetch fresh model list

## Command Output

The command provides detailed feedback:
- Number of models found
- List of available model names
- Ollama health status and response time
- Error messages if issues occur

## Prerequisites

- Ollama service must be running and accessible
- Virtual environment should be activated
- Django project properly configured

Example output:
```
Found 1 models: qwen3:8b
Ollama is healthy (response time: 1ms)
```

