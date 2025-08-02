# management/commands/check_db_config.py
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = 'Check current database configuration'

    def handle(self, *args, **options):
        db_config = settings.DATABASES['default']
        
        self.stdout.write("Current Database Configuration:")
        self.stdout.write(f"ENGINE: {db_config.get('ENGINE')}")
        self.stdout.write(f"NAME: {db_config.get('NAME')}")
        self.stdout.write(f"USER: {db_config.get('USER')}")
        self.stdout.write(f"HOST: {db_config.get('HOST')}")
        self.stdout.write(f"PORT: {db_config.get('PORT')}")
        
        # Check if .env file is being loaded
        from decouple import config
        self.stdout.write("\nEnvironment Variables:")
        try:
            self.stdout.write(f"DB_HOST from env: {config('DB_HOST', default='NOT_SET')}")
            self.stdout.write(f"DB_NAME from env: {config('DB_NAME', default='NOT_SET')}")
            self.stdout.write(f"DB_USER from env: {config('DB_USER', default='NOT_SET')}")
        except Exception as e:
            self.stdout.write(f"Error reading env vars: {e}")