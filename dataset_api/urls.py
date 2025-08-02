from rest_framework.routers import DefaultRouter
from django.urls import path, include 
from . import views

router = DefaultRouter()
# router.register(r"datasets", views.DatasetViewSet)
# router.register(r"relationships", views.DatasetRelationshipViewSet)
# router.register(r"queries", views.DatasetQueriesViewSet)

urlpatterns = [
    path("", include(router.urls))
]
