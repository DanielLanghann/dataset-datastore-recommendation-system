from rest_framework import viewsets, status, permissions
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from django.db import transaction
from django.core.exceptions import ValidationError
import time
import socket
from contextlib import closing

from .models import Datastore
from .serializers import (
    DatestoreListSerializer,
    DatastoreDetailSerializer, 
    DatastoreCreateUpdateSerializer,
    DatastoreTypeSerializer,
    ConnectionTestSerializer,
    ConnectionTestResponseSerializer,
    DatastorePerformanceSerializer,
    DatastoreMinimalSerializer
)


class DatastoreViewSet(viewsets.ModelViewSet):
    # CRUD
    queryset = Datastore.objects.all()
    permission_classes = [permissions.IsAuthenticated]
    
    def get_serializer_class(self):
        if self.action == 'list':
            return DatestoreListSerializer
        elif self.action in ['create', 'update', 'partial_update']:
            return DatastoreCreateUpdateSerializer
        elif self.action == 'retrieve':
            return DatastoreDetailSerializer
        elif self.action == 'performance':
            return DatastorePerformanceSerializer
        elif self.action == 'minimal':
            return DatastoreMinimalSerializer
        return DatestoreListSerializer
    
    def get_queryset(self):
        queryset = Datastore.objects.all()
        if self.action == 'list':
            # Type filter
            datastore_type = self.request.query_params.get('type')
            if datastore_type:
                queryset = queryset.filter(type=datastore_type)

            # System filter    
            system = self.request.query_params.get('system')
            if system:
                queryset = queryset.filter(system=system)
                
            # Status filter
            is_active = self.request.query_params.get('is_active')
            if is_active is not None:
                is_active_bool = is_active.lower() in ['true', '1', 'yes']
                queryset = queryset.filter(is_active=is_active_bool)
        
        return queryset.order_by('name')
    
    @action(detail=False, methods=['get'])
    def active(self, request):
       # GET /datastores/active/
        queryset = Datastore.objects.active()
        serializer = DatestoreListSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def minimal(self, request):
        # GET /datastores/minimal/
        queryset = Datastore.objects.active()
        serializer = DatastoreMinimalSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def performance(self, request):
        # GET /datastores/performance/
        queryset = Datastore.objects.all()
        serializer = DatastorePerformanceSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def types(self, request):
        # Get available datastore types with their characteristics and valid systems
        # GET /datastores/types/
        types_data = []
        valid_combinations = Datastore.get_valid_type_system_combinations()
        characteristics = Datastore.get_type_characteristics()
        
        for type_value, type_label in Datastore.TYPE_CHOICES:
            systems = []
            for system_value, system_label in Datastore.SYSTEM_CHOICES:
                if (type_value, system_value) in valid_combinations:
                    systems.append({
                        'value': system_value,
                        'label': system_label
                    })
            
            types_data.append({
                'value': type_value,
                'label': type_label,
                'systems': systems,
                'characteristics': characteristics.get(type_value, {})
            })
        
        serializer = DatastoreTypeSerializer(types_data, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['post'])
    def test_connection(self, request):
        # POST /datastores/test_connection/
        serializer = ConnectionTestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        data = serializer.validated_data
        test_result = self._test_connection(data)
        
        response_serializer = ConnectionTestResponseSerializer(test_result)
        return Response(response_serializer.data)
    
    @action(detail=True, methods=['post'])
    def test_connection_existing(self, request, pk=None):
        # Test connection for an existing datastore
        # POST /datastores/{id}/test_connection_existing/
        datastore = self.get_object()
        
        connection_data = {
            'server': datastore.server,
            'port': datastore.port,
            'username': datastore.username,
            'password': datastore.decrypt_password() if datastore.password else None,
        }
        
        test_result = self._test_connection(connection_data)
        response_serializer = ConnectionTestResponseSerializer(test_result)
        return Response(response_serializer.data)
    
    @action(detail=True, methods=['patch'])
    def toggle_active(self, request, pk=None):
        # PATCH /datastores/{id}/toggle_active/
        datastore = self.get_object()
        
        datastore.is_active = not datastore.is_active
        
        try:
            with transaction.atomic():
                datastore.save()
            
            serializer = DatastoreDetailSerializer(datastore)
            return Response({
                'message': f'Datastore {"activated" if datastore.is_active else "deactivated"} successfully',
                'datastore': serializer.data
            })
            
        except ValidationError as e:
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_400_BAD_REQUEST
            )
    
    @action(detail=False, methods=['get'])
    def stats(self, request):
        # GET /datastores/stats/
        total_count = Datastore.objects.count()
        active_count = Datastore.objects.active().count()
        
        # Count by type
        type_counts = {}
        for type_value, type_label in Datastore.TYPE_CHOICES:
            count = Datastore.objects.filter(type=type_value).count()
            type_counts[type_value] = {
                'label': type_label,
                'count': count
            }
        
        # Count by system
        system_counts = {}
        for system_value, system_label in Datastore.SYSTEM_CHOICES:
            count = Datastore.objects.filter(system=system_value).count()
            system_counts[system_value] = {
                'label': system_label,
                'count': count
            }
        
        return Response({
            'total_datastores': total_count,
            'active_datastores': active_count,
            'inactive_datastores': total_count - active_count,
            'by_type': type_counts,
            'by_system': system_counts
        })
    
    @action(detail=False, methods=['post'])
    def bulk_toggle(self, request):
        # Bulk toggle active status for multiple datastores
        # POST /datastores/bulk_toggle/
        datastore_ids = request.data.get('datastore_ids', [])
        action_type = request.data.get('action', 'toggle')
        
        if not datastore_ids:
            return Response(
                {'error': 'datastore_ids is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        datastores = Datastore.objects.filter(id__in=datastore_ids)
        
        if not datastores.exists():
            return Response(
                {'error': 'No datastores found with provided IDs'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        try:
            with transaction.atomic():
                updated_count = 0
                for datastore in datastores:
                    if action_type == 'toggle':
                        datastore.is_active = not datastore.is_active
                    elif action_type == 'activate':
                        datastore.is_active = True
                    elif action_type == 'deactivate':
                        datastore.is_active = False
                    
                    datastore.save()
                    updated_count += 1
            
            return Response({
                'message': f'Successfully updated {updated_count} datastores',
                'updated_count': updated_count
            })
            
        except ValidationError as e:
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': f'Bulk operation failed: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _test_connection(self, connection_data):
        # basic connection test
        server = connection_data.get('server')
        port = connection_data.get('port')
        
        if not server or not port:
            return {
                'success': False,
                'message': 'Server and port are required for connection test',
                'error_code': 'MISSING_PARAMS'
            }
        
        try:
            start_time = time.time()
            
            # socket connection test
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(5)
                result = sock.connect_ex((server, port))
                
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            
            if result == 0:
                return {
                    'success': True,
                    'response_time_ms': round(response_time, 2),
                    'message': 'Connection successful',
                    'details': {
                        'server': server,
                        'port': port
                    }
                }
            else:
                return {
                    'success': False,
                    'message': f'Connection failed to {server}:{port}',
                    'error_code': 'CONNECTION_FAILED',
                    'details': {
                        'server': server,
                        'port': port
                    }
                }
                
        except socket.gaierror as e:
            return {
                'success': False,
                'message': f'DNS resolution failed: {str(e)}',
                'error_code': 'DNS_ERROR'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Connection test failed: {str(e)}',
                'error_code': 'UNKNOWN_ERROR'
            }