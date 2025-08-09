from rest_framework.routers import DefaultRouter
from django.urls import path, include
from . import views

router = DefaultRouter()
router.register(r"requests", views.RequestViewSet)
router.register(r"ollama", views.OllamaValidationViewSet, basename="ollama")

urlpatterns = [path("", include(router.urls))]
