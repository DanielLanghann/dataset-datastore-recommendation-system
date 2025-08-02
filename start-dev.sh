#!/bin/bash

# DDRS Local Development Startup Script
# This script helps you start both backend and frontend for local development

echo "🚀 Starting DDRS Local Development Environment"
echo "=============================================="

# Function to check if a port is in use
check_port() {
    if lsof -Pi :$1 -sTCP:LISTEN -t >/dev/null ; then
        echo "⚠️  Port $1 is already in use"
        return 1
    else
        return 0
    fi
}

# Check if required ports are available
echo "🔍 Checking if ports are available..."
check_port 8000 || (echo "   Django usually runs on port 8000" && echo)
check_port 3000 || (echo "   React usually runs on port 3000" && echo)

# Check if virtual environment exists
if [ -d "venv" ] || [ -d ".venv" ] || [ ! -z "$VIRTUAL_ENV" ]; then
    echo "✅ Python virtual environment detected"
else
    echo "⚠️  No Python virtual environment detected"
    echo "   Consider creating one: python -m venv venv && source venv/bin/activate"
    echo
fi

# Setup local environment file for Django
echo "🔧 Setting up local environment..."
if [ ! -f ".env.local" ]; then
    echo "   Creating .env.local for local development..."
    cp .env .env.local
    # Update database settings for local development
    sed -i.bak 's/DB_HOST=db/DB_ENGINE=sqlite3/' .env.local
    sed -i.bak 's/DB_PORT=5432/DB_NAME=db.sqlite3/' .env.local
    rm .env.local.bak
    echo "   ✅ Created .env.local with SQLite configuration"
else
    echo "   ✅ .env.local already exists"
fi

# Setup frontend if needed
echo "📦 Setting up frontend..."
cd frontend
if [ ! -f ".env.local" ]; then
    echo "   Creating frontend .env.local for local development..."
    ./setup-local.sh
else
    echo "   ✅ Frontend already configured for local development"
fi

if [ ! -d "node_modules" ]; then
    echo "   Installing npm dependencies..."
    npm install
else
    echo "   ✅ npm dependencies already installed"
fi

cd ..

echo
echo "🎯 Ready to start development!"
echo
echo "To start the development environment:"
echo
echo "1. Start Django backend (in one terminal):"
echo "   export DJANGO_SETTINGS_MODULE=ddrs_api.settings"
echo "   export DJANGO_CONFIGURATION=Local"
echo "   python manage.py migrate  # Run this first time only"
echo "   python manage.py runserver"
echo
echo "2. Start React frontend (in another terminal):"
echo "   cd frontend && npm start"
echo
echo "📱 Access URLs:"
echo "   Frontend:     http://localhost:3000"
echo "   Backend API:  http://localhost:8000/api"
echo "   Django Admin: http://localhost:8000/admin"
echo
echo "💡 Tips:"
echo "   - Uses SQLite database for local development (simpler setup)"
echo "   - Database file will be created as db.sqlite3"
echo "   - You can also run everything with Docker: docker-compose up"
echo
echo "📖 For more details, see frontend/DEVELOPMENT.md"
