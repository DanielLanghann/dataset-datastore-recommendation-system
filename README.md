# Dataset Datastore Recommendation System (DDRS)

A Django REST API system for managing and recommending datastores for datasets.

## Quick Start with Docker

### Prerequisites
- Docker
- Docker Compose

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ddrs
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` file with your preferred settings. The default values should work for development.

3. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```

   This will:
   - Start a PostgreSQL database
   - Run Django migrations
   - Create a superuser (admin/admin123 by default)
   - Start the Django development server on port 8000

4. **Access the application**
   - API: http://localhost:8000/api/
   - Admin interface: http://localhost:8000/admin/
   - API Documentation: http://localhost:8000/api/datastores/

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DEBUG` | Django debug mode | `True` |
| `SECRET_KEY` | Django secret key | Generated |
| `ALLOWED_HOSTS` | Allowed hosts | `localhost,127.0.0.1,0.0.0.0` |
| `DB_HOST` | Database host | `db` |
| `DB_PORT` | Database port | `5432` |
| `DB_NAME` | Database name | `ddrs_db` |
| `DB_USER` | Database user | `ddrs_user` |
| `DB_PASSWORD` | Database password | `ddrs_password` |
| `DJANGO_SUPERUSER_USERNAME` | Admin username | `admin` |
| `DJANGO_SUPERUSER_EMAIL` | Admin email | `admin@example.com` |
| `DJANGO_SUPERUSER_PASSWORD` | Admin password | `admin123` |
| `DATASTORE_ENCRYPTION_KEY` | Encryption key for sensitive data | Auto-generated |

### API Endpoints

- `GET /api/datastores/` - List all datastores
- `POST /api/datastores/` - Create a new datastore
- `GET /api/datastores/{id}/` - Get datastore details
- `PUT /api/datastores/{id}/` - Update datastore
- `DELETE /api/datastores/{id}/` - Delete datastore
- `POST /api/datastores/{id}/test_connection/` - Test datastore connection

### Development

For development without Docker:

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL database**
   - Install PostgreSQL
   - Create database and user as specified in `.env`

3. **Run migrations**
   ```bash
   python manage.py migrate
   ```

4. **Create superuser**
   ```bash
   python manage.py createsuperuser
   ```

5. **Run development server**
   ```bash
   python manage.py runserver
   ```

## Project Structure

- `datastore_api/` - Main API application for datastore management
- `dataset_api/` - Dataset management (planned)
- `matching_engine/` - Recommendation engine (planned)
- `ddrs_api/` - Django project settings

## Current Status

Currently, only the `datastore_api` is implemented, which provides:
- Full CRUD operations for datastore management
- Connection testing capabilities
- Performance metrics
- Data encryption for sensitive connection details
