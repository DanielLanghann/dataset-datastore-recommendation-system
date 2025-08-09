from django.core.management.base import BaseCommand
from matching_engine.services.ollama_model_validator import OllamaModelValidator


class Command(BaseCommand):
    help = "Sync available models from Ollama and update cache"

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force refresh even if cache exists",
        )

    def handle(self, *args, **options):
        if options["force"]:
            self.stdout.write("Forcing cache refresh...")
            models = OllamaModelValidator.refresh_cache()
        else:
            models = OllamaModelValidator.get_available_models()

        self.stdout.write(
            self.style.SUCCESS(f'Found {len(models)} models: {", ".join(models)}')
        )

        # Also check health
        health = OllamaModelValidator.health_check()
        if health["status"] == "healthy":
            self.stdout.write(
                self.style.SUCCESS(
                    f'Ollama is healthy (response time: {health["response_time_ms"]}ms)'
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    f'Ollama is {health["status"]}: {health.get("error", "Unknown error")}'
                )
            )
