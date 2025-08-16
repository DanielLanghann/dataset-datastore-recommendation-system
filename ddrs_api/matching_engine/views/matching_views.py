import logging
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from django.shortcuts import get_object_or_404

from ..models import MatchingRequests, MatchingResponse
from ..services.core_matching_service import CoreMatchingService
from ..serializers import (
    RequestSerializer,
    ResponseDetailSerializer,
)
from dataset_api.models import DatasetBaseModel
from datastore_api.models import Datastore

logger = logging.getLogger(__name__)

class MatchingViewSet(viewsets.ViewSet):
    """
    Enhanced ViewSet for Ollama-powered matching operations with query and relationship analysis
    """
    
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.matching_service = CoreMatchingService()
    
    @action(detail=False, methods=["post"])
    def process_request(self, request):
        """
        Process an existing matching request through Ollama with enhanced analysis

        Expected payload:
        {
            "request_id": 123,
            "dataset_ids": [1, 2, 3],  # optional
            "datastore_ids": [1, 2]   # optional
        }
        """
        request_id = request.data.get('request_id')
        dataset_ids = request.data.get('dataset_ids')
        datastore_ids = request.data.get('datastore_ids')
        
        if not request_id:
            return Response(
                {"error": "request_id is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Verify request exists
            matching_request = get_object_or_404(MatchingRequests, id=request_id)
            
            # Process the request with enhanced analysis
            response_obj = self.matching_service.processing_matching_request(
                request_id=request_id,
                dataset_ids=dataset_ids,
                datastore_ids=datastore_ids
            )
            
            # Return the response with additional analysis
            serializer = ResponseDetailSerializer(response_obj)
            
            # Add analysis summary
            analysis_summary = self._create_analysis_summary(
                matching_request, response_obj, dataset_ids, datastore_ids
            )
            
            return Response({
                "status": "success",
                "message": f"Successfully processed request {request_id} with enhanced analysis",
                "response": serializer.data,
                "analysis_summary": analysis_summary
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error processing request {request_id}: {str(e)}")
            return Response(
                {"error": f"Failed to process request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=["post"])
    def create_and_process(self, request):
        """
        Create a new request and immediately process it with enhanced query and relationship analysis
        
        Expected payload:
        {
            "dataset_ids": [1, 2, 3],
            "datastore_ids": [1, 2, 3],
            "system_prompt": "optional custom system prompt",
            "user_prompt": "optional custom user prompt", 
            "model": "qwen2.5:8b",
            "description": "optional description",
            "include_query_analysis": true,  # optional, default true
            "include_relationship_analysis": true  # optional, default true
        }
        """
        dataset_ids = request.data.get('dataset_ids', [])
        datastore_ids = request.data.get('datastore_ids', [])
        system_prompt = request.data.get('system_prompt')
        user_prompt = request.data.get('user_prompt')
        model = request.data.get('model', 'qwen2.5:8b')
        description = request.data.get('description', '')
        include_query_analysis = request.data.get('include_query_analysis', True)
        include_relationship_analysis = request.data.get('include_relationship_analysis', True)
        
        if not dataset_ids:
            return Response(
                {"error": "dataset_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not datastore_ids:
            return Response(
                {"error": "datastore_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Fetch datasets and datastores with related data
            datasets = DatasetBaseModel.objects.prefetch_related(
                'queries', 'relationships_from', 'relationships_to'
            ).filter(id__in=dataset_ids)
            
            datastores = Datastore.objects.filter(id__in=datastore_ids, is_active=True)
            
            if not datasets.exists():
                return Response(
                    {"error": "No valid datasets found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            if not datastores.exists():
                return Response(
                    {"error": "No valid datastores found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Prepare enhanced data for service
            datasets_data = []
            for dataset in datasets:
                dataset_info = {
                    'id': dataset.id,
                    'name': dataset.name,
                    'data_structure': dataset.data_structure,
                    'growth_rate': dataset.growth_rate,
                    'access_patterns': dataset.access_patterns,
                    'query_complexity': dataset.query_complexity,
                    'estimated_size_gb': dataset.estimated_size_gb,
                    'avg_query_time_ms': dataset.avg_query_time_ms,
                    'queries_per_day': dataset.queries_per_day
                }
                
                # Add query information if requested
                if include_query_analysis:
                    dataset_info['queries'] = [
                        {
                            'id': q.id,
                            'name': q.name,
                            'query_type': q.query_type,
                            'frequency': q.frequency,
                            'avg_execution_time_ms': q.avg_execution_time_ms
                        } for q in dataset.queries.all()
                    ]
                
                # Add relationship information if requested
                if include_relationship_analysis:
                    dataset_info['relationships'] = []
                    for rel in dataset.relationships_from.filter(is_active=True):
                        dataset_info['relationships'].append({
                            'type': rel.relationship_type,
                            'target_dataset_id': rel.to_dataset.id if rel.to_dataset else None,
                            'strength': rel.strength,
                            'direction': 'outgoing'
                        })
                    for rel in dataset.relationships_to.filter(is_active=True):
                        dataset_info['relationships'].append({
                            'type': rel.relationship_type,
                            'source_dataset_id': rel.from_dataset.id if rel.from_dataset else None,
                            'strength': rel.strength,
                            'direction': 'incoming'
                        })
                
                datasets_data.append(dataset_info)
            
            datastores_data = []
            for datastore in datastores:
                datastores_data.append({
                    'id': datastore.id,
                    'name': datastore.name,
                    'type': datastore.type,
                    'system': datastore.system,
                    'max_connections': datastore.max_connections,
                    'avg_response_time_ms': datastore.avg_response_time_ms,
                    'storage_capacity_gb': datastore.storage_capacity_gb
                })
            
            # Use enhanced prompts if not provided
            if not system_prompt:
                system_prompt = self._get_enhanced_system_prompt(
                    include_query_analysis, include_relationship_analysis
                )
            
            if not user_prompt:
                user_prompt = self._get_enhanced_user_prompt(
                    include_query_analysis, include_relationship_analysis
                )
            
            # Create and process with enhanced service
            request_obj, response_obj = self.matching_service.create_request_and_process(
                datasets_data=datasets_data,
                datastores_data=datastores_data,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=model,
                description=description or f"Enhanced analysis: {len(datasets_data)} datasets, {len(datastores_data)} datastores"
            )
            
            # Serialize responses
            request_serializer = RequestSerializer(request_obj)
            response_serializer = ResponseDetailSerializer(response_obj)
            
            # Create analysis summary
            analysis_summary = self._create_analysis_summary(
                request_obj, response_obj, dataset_ids, datastore_ids
            )
            
            return Response({
                "status": "success",
                "message": f"Successfully created and processed enhanced request {request_obj.id}",
                "request": request_serializer.data,
                "response": response_serializer.data,
                "analysis_summary": analysis_summary
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error in enhanced create_and_process: {str(e)}")
            return Response(
                {"error": f"Failed to create and process request: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=["post"])
    def analyze_dependencies(self, request):
        """
        Analyze dataset dependencies without creating a full matching request
        
        Expected payload:
        {
            "dataset_ids": [1, 2, 3]
        }
        """
        dataset_ids = request.data.get('dataset_ids', [])
        
        if not dataset_ids:
            return Response(
                {"error": "dataset_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Fetch datasets with relationships
            datasets = DatasetBaseModel.objects.prefetch_related(
                'relationships_from__to_dataset',
                'relationships_to__from_dataset'
            ).filter(id__in=dataset_ids)
            
            if not datasets.exists():
                return Response(
                    {"error": "No valid datasets found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Analyze dependencies
            dependency_analysis = self._analyze_dataset_dependencies(datasets)
            
            return Response({
                "status": "success",
                "dataset_count": len(dataset_ids),
                "dependency_analysis": dependency_analysis
            })
            
        except Exception as e:
            logger.error(f"Error analyzing dependencies: {str(e)}")
            return Response(
                {"error": f"Failed to analyze dependencies: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=["post"])
    def analyze_queries(self, request):
        """
        Analyze dataset query patterns without creating a full matching request
        
        Expected payload:
        {
            "dataset_ids": [1, 2, 3]
        }
        """
        dataset_ids = request.data.get('dataset_ids', [])
        
        if not dataset_ids:
            return Response(
                {"error": "dataset_ids is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Fetch datasets with queries
            datasets = DatasetBaseModel.objects.prefetch_related('queries').filter(id__in=dataset_ids)
            
            if not datasets.exists():
                return Response(
                    {"error": "No valid datasets found"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Analyze query patterns
            query_analysis = self._analyze_query_patterns(datasets)
            
            return Response({
                "status": "success",
                "dataset_count": len(dataset_ids),
                "query_analysis": query_analysis
            })
            
        except Exception as e:
            logger.error(f"Error analyzing queries: {str(e)}")
            return Response(
                {"error": f"Failed to analyze queries: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=["get"])
    def matching_status(self, request):
        """
        Get enhanced status information about matching capabilities
        """
        try:
            # Count available data
            datasets_count = DatasetBaseModel.objects.count()
            datasets_with_queries = DatasetBaseModel.objects.filter(queries__isnull=False).distinct().count()
            datasets_with_relationships = DatasetBaseModel.objects.filter(
                models.Q(relationships_from__isnull=False) | 
                models.Q(relationships_to__isnull=False)
            ).distinct().count()
            
            active_datastores_count = Datastore.objects.filter(is_active=True).count()
            total_datastores_count = Datastore.objects.count()
            
            # Recent activity
            recent_requests = MatchingRequests.objects.count()
            recent_responses = MatchingResponse.objects.count()
            
            # Check Ollama health
            from ..services.ollama_model_validator_service import OllamaModelValidator
            health_info = OllamaModelValidator.health_check()
            available_models = OllamaModelValidator.get_available_models()
            
            return Response({
                "system_status": {
                    "datasets_available": datasets_count,
                    "datasets_with_queries": datasets_with_queries,
                    "datasets_with_relationships": datasets_with_relationships,
                    "active_datastores": active_datastores_count,
                    "total_datastores": total_datastores_count,
                    "total_requests": recent_requests,
                    "total_responses": recent_responses
                },
                "ollama_status": {
                    "health": health_info["status"],
                    "available_models": available_models,
                    "models_count": len(available_models)
                },
                "analysis_capabilities": {
                    "query_analysis": datasets_with_queries > 0,
                    "relationship_analysis": datasets_with_relationships > 0,
                    "dependency_analysis": datasets_with_relationships > 0
                },
                "ready_for_matching": (
                    datasets_count > 0 and 
                    active_datastores_count > 0 and 
                    health_info["status"] == "healthy"
                )
            })
            
        except Exception as e:
            logger.error(f"Error getting enhanced matching status: {str(e)}")
            return Response(
                {"error": f"Failed to get status: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_enhanced_system_prompt(self, include_query_analysis, include_relationship_analysis):
        """Generate enhanced system prompt based on analysis options"""
        base_prompt = """You are an expert database architect with deep knowledge of different database systems, their strengths, limitations, and optimal use cases."""
        
        if include_query_analysis:
            base_prompt += """ You specialize in analyzing query patterns, frequencies, and performance characteristics to match datasets with optimal datastores."""
        
        if include_relationship_analysis:
            base_prompt += """ You understand dataset relationships, foreign key constraints, and dependency patterns that influence datastore distribution decisions."""
        
        base_prompt += """ Consider performance requirements, scalability needs, and data consistency requirements when making recommendations."""
        
        return base_prompt
    
    def _get_enhanced_user_prompt(self, include_query_analysis, include_relationship_analysis):
        """Generate enhanced user prompt based on analysis options"""
        prompt_parts = [
            "Analyze each dataset's characteristics and recommend the optimal datastore considering:"
        ]
        
        if include_query_analysis:
            prompt_parts.extend([
                "1. QUERY PATTERNS: Examine query types, frequencies, and performance requirements",
                "2. PERFORMANCE NEEDS: Match query complexity with datastore capabilities"
            ])
        
        if include_relationship_analysis:
            prompt_parts.extend([
                "3. DATA RELATIONSHIPS: Consider foreign keys and dependencies between datasets",
                "4. CONSISTENCY REQUIREMENTS: Ensure related datasets can maintain referential integrity"
            ])
        
        prompt_parts.extend([
            "5. SCALABILITY: Consider growth rates and storage capacity requirements",
            "6. COMPATIBILITY: Ensure data structure compatibility with datastore types"
        ])
        
        if include_relationship_analysis:
            prompt_parts.append(
                "\nFor datasets with strong relationships, consider co-location vs. distributed approaches."
            )
        
        prompt_parts.append(
            "\nProvide detailed reasoning that includes specific analysis of the provided data characteristics."
        )
        
        return "\n".join(prompt_parts)
    
    def _create_analysis_summary(self, request_obj, response_obj, dataset_ids, datastore_ids):
        """Create a summary of the analysis performed"""
        try:
            # Get dataset information
            datasets = DatasetBaseModel.objects.prefetch_related(
                'queries', 'relationships_from', 'relationships_to'
            ).filter(id__in=dataset_ids or [])
            
            # Count queries and relationships
            total_queries = sum(dataset.queries.count() for dataset in datasets)
            total_relationships = sum(
                dataset.relationships_from.filter(is_active=True).count() + 
                dataset.relationships_to.filter(is_active=True).count() 
                for dataset in datasets
            )
            
            # Analyze query patterns
            query_types = {}
            query_frequencies = {}
            
            for dataset in datasets:
                for query in dataset.queries.all():
                    query_types[query.query_type] = query_types.get(query.query_type, 0) + 1
                    query_frequencies[query.frequency] = query_frequencies.get(query.frequency, 0) + 1
            
            # Analyze relationships
            relationship_types = {}
            dependency_groups = []
            
            for dataset in datasets:
                for rel in dataset.relationships_from.filter(is_active=True):
                    relationship_types[rel.relationship_type] = relationship_types.get(rel.relationship_type, 0) + 1
                    
                    # Identify dependency groups
                    if rel.relationship_type in ['foreign_key', 'dependency']:
                        group = sorted([dataset.id, rel.to_dataset.id if rel.to_dataset else None])
                        if group not in dependency_groups and None not in group:
                            dependency_groups.append(group)
            
            # Analyze response quality
            recommendations_count = 0
            avg_confidence = 0
            dependency_considerations = []
            
            if response_obj and response_obj.result:
                result = response_obj.result
                if 'recommendations' in result:
                    recommendations = result['recommendations']
                    recommendations_count = len(recommendations)
                    if recommendations:
                        confidences = [rec.get('confidence', 0) for rec in recommendations]
                        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
                
                if 'dependency_considerations' in result:
                    dependency_considerations = result['dependency_considerations']
            
            return {
                "analysis_scope": {
                    "datasets_analyzed": len(datasets),
                    "datastores_considered": len(datastore_ids or []),
                    "queries_analyzed": total_queries,
                    "relationships_analyzed": total_relationships
                },
                "query_analysis": {
                    "total_queries": total_queries,
                    "query_types_distribution": query_types,
                    "frequency_distribution": query_frequencies,
                    "datasets_with_queries": sum(1 for d in datasets if d.queries.exists())
                },
                "relationship_analysis": {
                    "total_relationships": total_relationships,
                    "relationship_types_distribution": relationship_types,
                    "dependency_groups_identified": len(dependency_groups),
                    "datasets_with_relationships": sum(
                        1 for d in datasets 
                        if d.relationships_from.exists() or d.relationships_to.exists()
                    )
                },
                "matching_results": {
                    "recommendations_generated": recommendations_count,
                    "average_confidence": round(avg_confidence, 3),
                    "dependency_considerations": len(dependency_considerations),
                    "model_used": response_obj.model if response_obj else None
                },
                "quality_indicators": {
                    "all_datasets_matched": recommendations_count == len(datasets),
                    "high_confidence_matches": sum(
                        1 for rec in (response_obj.result.get('recommendations', []) if response_obj else [])
                        if rec.get('confidence', 0) > 0.8
                    ),
                    "dependency_aware": len(dependency_considerations) > 0,
                    "query_informed": total_queries > 0
                }
            }
            
        except Exception as e:
            logger.error(f"Error creating analysis summary: {str(e)}")
            return {
                "error": "Failed to create analysis summary",
                "details": str(e)
            }
    
    def _analyze_dataset_dependencies(self, datasets):
        """Analyze dependencies between datasets"""
        try:
            dependencies = []
            dependency_groups = {}
            
            for dataset in datasets:
                dataset_deps = {
                    "dataset_id": dataset.id,
                    "dataset_name": dataset.name,
                    "outgoing_relationships": [],
                    "incoming_relationships": [],
                    "dependency_strength": 0
                }
                
                # Analyze outgoing relationships
                for rel in dataset.relationships_from.filter(is_active=True):
                    rel_info = {
                        "target_dataset_id": rel.to_dataset.id if rel.to_dataset else None,
                        "target_dataset_name": rel.to_dataset.name if rel.to_dataset else None,
                        "relationship_type": rel.relationship_type,
                        "strength": rel.strength,
                        "description": rel.description
                    }
                    dataset_deps["outgoing_relationships"].append(rel_info)
                    dataset_deps["dependency_strength"] += rel.strength
                    
                    # Group related datasets
                    if rel.relationship_type in ['foreign_key', 'dependency']:
                        group_key = f"{min(dataset.id, rel.to_dataset.id if rel.to_dataset else 0)}_{max(dataset.id, rel.to_dataset.id if rel.to_dataset else 0)}"
                        if group_key not in dependency_groups:
                            dependency_groups[group_key] = set()
                        dependency_groups[group_key].add(dataset.id)
                        if rel.to_dataset:
                            dependency_groups[group_key].add(rel.to_dataset.id)
                
                # Analyze incoming relationships
                for rel in dataset.relationships_to.filter(is_active=True):
                    rel_info = {
                        "source_dataset_id": rel.from_dataset.id if rel.from_dataset else None,
                        "source_dataset_name": rel.from_dataset.name if rel.from_dataset else None,
                        "relationship_type": rel.relationship_type,
                        "strength": rel.strength,
                        "description": rel.description
                    }
                    dataset_deps["incoming_relationships"].append(rel_info)
                    dataset_deps["dependency_strength"] += rel.strength
                
                dependencies.append(dataset_deps)
            
            # Convert dependency groups to lists
            grouped_dependencies = [
                {
                    "group_id": i,
                    "dataset_ids": list(group),
                    "datasets_count": len(group),
                    "group_type": "strong_dependency" if len(group) > 2 else "paired_dependency"
                }
                for i, group in enumerate(dependency_groups.values()) if len(group) > 1
            ]
            
            return {
                "individual_dependencies": dependencies,
                "dependency_groups": grouped_dependencies,
                "total_relationships": sum(
                    len(dep["outgoing_relationships"]) + len(dep["incoming_relationships"]) 
                    for dep in dependencies
                ),
                "datasets_with_dependencies": sum(
                    1 for dep in dependencies 
                    if dep["outgoing_relationships"] or dep["incoming_relationships"]
                ),
                "recommendations": self._generate_dependency_recommendations(dependencies, grouped_dependencies)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing dataset dependencies: {str(e)}")
            return {"error": str(e)}
    
    def _analyze_query_patterns(self, datasets):
        """Analyze query patterns across datasets"""
        try:
            query_analysis = []
            overall_stats = {
                "total_queries": 0,
                "query_types": {},
                "frequency_patterns": {},
                "performance_patterns": {},
                "complexity_distribution": {}
            }
            
            for dataset in datasets:
                dataset_queries = {
                    "dataset_id": dataset.id,
                    "dataset_name": dataset.name,
                    "query_count": dataset.queries.count(),
                    "queries": [],
                    "patterns": {
                        "read_heavy": 0,
                        "write_heavy": 0,
                        "mixed": 0,
                        "analytical": 0
                    },
                    "performance_requirements": {
                        "high_frequency": 0,
                        "low_latency": 0,
                        "complex_queries": 0
                    }
                }
                
                for query in dataset.queries.all():
                    query_info = {
                        "query_id": query.id,
                        "name": query.name,
                        "type": query.query_type,
                        "frequency": query.frequency,
                        "avg_execution_time_ms": query.avg_execution_time_ms,
                        "description": query.description
                    }
                    dataset_queries["queries"].append(query_info)
                    
                    # Update overall statistics
                    overall_stats["total_queries"] += 1
                    overall_stats["query_types"][query.query_type] = overall_stats["query_types"].get(query.query_type, 0) + 1
                    overall_stats["frequency_patterns"][query.frequency] = overall_stats["frequency_patterns"].get(query.frequency, 0) + 1
                    
                    # Analyze patterns
                    if query.query_type in ['select']:
                        dataset_queries["patterns"]["read_heavy"] += 1
                    elif query.query_type in ['insert', 'update', 'delete']:
                        dataset_queries["patterns"]["write_heavy"] += 1
                    elif query.query_type in ['aggregate', 'complex']:
                        dataset_queries["patterns"]["analytical"] += 1
                    else:
                        dataset_queries["patterns"]["mixed"] += 1
                    
                    # Performance requirements
                    if query.frequency == 'high':
                        dataset_queries["performance_requirements"]["high_frequency"] += 1
                    if query.avg_execution_time_ms and query.avg_execution_time_ms < 50:
                        dataset_queries["performance_requirements"]["low_latency"] += 1
                    if query.query_type in ['aggregate', 'complex']:
                        dataset_queries["performance_requirements"]["complex_queries"] += 1
                
                # Determine dominant pattern
                max_pattern = max(dataset_queries["patterns"].items(), key=lambda x: x[1])
                dataset_queries["dominant_pattern"] = max_pattern[0]
                
                query_analysis.append(dataset_queries)
                
                # Update complexity distribution
                complexity = dataset.query_complexity or 'unknown'
                overall_stats["complexity_distribution"][complexity] = overall_stats["complexity_distribution"].get(complexity, 0) + 1
            
            return {
                "dataset_analysis": query_analysis,
                "overall_statistics": overall_stats,
                "recommendations": self._generate_query_recommendations(query_analysis),
                "datastore_requirements": self._analyze_datastore_requirements(query_analysis)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing query patterns: {str(e)}")
            return {"error": str(e)}
    
    def _generate_dependency_recommendations(self, dependencies, grouped_dependencies):
        """Generate recommendations based on dependency analysis"""
        recommendations = []
        
        # Recommend co-location for strong dependencies
        for group in grouped_dependencies:
            if group["datasets_count"] > 1:
                recommendations.append({
                    "type": "co_location",
                    "datasets": group["dataset_ids"],
                    "reason": f"Datasets have strong dependencies and should be co-located for referential integrity",
                    "priority": "high" if group["group_type"] == "strong_dependency" else "medium"
                })
        
        # Identify isolated datasets
        grouped_dataset_ids = set()
        for group in grouped_dependencies:
            grouped_dataset_ids.update(group["dataset_ids"])
        
        isolated_datasets = [
            dep["dataset_id"] for dep in dependencies 
            if dep["dataset_id"] not in grouped_dataset_ids and 
            not (dep["outgoing_relationships"] or dep["incoming_relationships"])
        ]
        
        if isolated_datasets:
            recommendations.append({
                "type": "independent_placement",
                "datasets": isolated_datasets,
                "reason": "Datasets have no dependencies and can be placed independently",
                "priority": "low"
            })
        
        return recommendations
    
    def _generate_query_recommendations(self, query_analysis):
        """Generate recommendations based on query pattern analysis"""
        recommendations = []
        
        for dataset_analysis in query_analysis:
            dataset_id = dataset_analysis["dataset_id"]
            dominant_pattern = dataset_analysis["dominant_pattern"]
            performance_reqs = dataset_analysis["performance_requirements"]
            
            if dominant_pattern == "read_heavy":
                recommendations.append({
                    "dataset_id": dataset_id,
                    "type": "read_optimized",
                    "reason": "Dataset has predominantly read queries - consider read-optimized datastores",
                    "suggested_datastores": ["column stores", "read replicas", "caching layers"]
                })
            
            elif dominant_pattern == "write_heavy":
                recommendations.append({
                    "dataset_id": dataset_id,
                    "type": "write_optimized",
                    "reason": "Dataset has predominantly write queries - consider write-optimized datastores",
                    "suggested_datastores": ["log-structured stores", "high-throughput databases"]
                })
            
            elif dominant_pattern == "analytical":
                recommendations.append({
                    "dataset_id": dataset_id,
                    "type": "analytical",
                    "reason": "Dataset has complex analytical queries - consider analytical datastores",
                    "suggested_datastores": ["column stores", "data warehouses", "analytical databases"]
                })
            
            if performance_reqs["high_frequency"] > 0 and performance_reqs["low_latency"] > 0:
                recommendations.append({
                    "dataset_id": dataset_id,
                    "type": "low_latency",
                    "reason": "Dataset requires low-latency, high-frequency access",
                    "suggested_datastores": ["in-memory databases", "caching layers", "SSD-based systems"]
                })
        
        return recommendations
    
    def _analyze_datastore_requirements(self, query_analysis):
        """Analyze what types of datastores are needed based on query patterns"""
        requirements = {
            "high_performance": [],
            "analytical": [],
            "transactional": [],
            "caching": [],
            "flexible_schema": []
        }
        
        for dataset_analysis in query_analysis:
            dataset_id = dataset_analysis["dataset_id"]
            
            if dataset_analysis["performance_requirements"]["low_latency"] > 0:
                requirements["high_performance"].append(dataset_id)
            
            if dataset_analysis["performance_requirements"]["complex_queries"] > 0:
                requirements["analytical"].append(dataset_id)
            
            if dataset_analysis["patterns"]["write_heavy"] > dataset_analysis["patterns"]["read_heavy"]:
                requirements["transactional"].append(dataset_id)
            
            if dataset_analysis["performance_requirements"]["high_frequency"] > 2:
                requirements["caching"].append(dataset_id)
        
        return requirements