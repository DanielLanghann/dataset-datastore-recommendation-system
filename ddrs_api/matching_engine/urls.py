from rest_framework.routers import DefaultRouter
from django.urls import path, include
from . import views

router = DefaultRouter()
router.register(r"requests", viewset=views.RequestViewSet)
router.register(r"ollama", viewset=views.OllamaValidationViewSet, basename="ollama")
router.register(r"responses", viewset=views.ResponseViewSet)
router.register(r"matching", viewset=views.MatchingViewSet, basename="matching")

urlpatterns = [path("", include(router.urls))]
