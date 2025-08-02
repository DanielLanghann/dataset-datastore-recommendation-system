from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path("api/", include("datastore_api.urls")),
    path("api/", include("dataset_api.urls"))
]

