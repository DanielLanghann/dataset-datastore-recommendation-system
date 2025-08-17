"""
Product Association Analytics Module

This module handles sophisticated product association analysis based on actual order data.
It's one of the most complex and valuable parts of the test data generation system.

Key Features:
- Real-time order pattern analysis
- Statistical aggregation and frequency calculation
- Market basket analysis
- Cross-category relationship discovery
- Performance-optimized queries for large datasets
"""

import psycopg2
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

@dataclass
class AssociationConfig:
    """Configuration for association analysis"""
    min_frequency_threshold: int = 2  # Minimum times products must be bought together
    max_associations_per_product: int = 50  # Limit associations to prevent explosion
    analysis_period_days: int = 365  # How far back to analyze orders
    cross_category_boost: float = 1.5  # Boost for cross-category associations
    same_brand_penalty: float = 0.8  # Reduce frequency for same-brand associations
    recency_weight: bool = True  # Weight recent orders more heavily

@dataclass
class AssociationStats:
    """Statistics from association analysis"""
    total_associations: int = 0
    new_associations: int = 0
    updated_associations: int = 0
    cross_category_associations: int = 0
    same_category_associations: int = 0
    analysis_period_start: Optional[datetime] = None
    analysis_period_end: Optional[datetime] = None
    processing_time_seconds: float = 0.0

class ProductAssociationAnalyzer:
    """
    Analyzes actual order data to generate intelligent product associations.
    
    This is much more sophisticated than simple test data generation - it's
    doing real business intelligence and market basket analysis.
    """
    
    def __init__(self, db_connection: psycopg2.extensions.connection, 
                 config: Optional[AssociationConfig] = None):
        self.connection = db_connection
        self.config = config or AssociationConfig()
        self.logger = logging.getLogger(__name__)
        
    def update_associations_from_orders(self) -> AssociationStats:
        """
        Main method: Analyze order patterns and update product associations
        
        This replaces the complex update_product_associations_from_orders() method
        with a more sophisticated, configurable, and maintainable approach.
        """
        start_time = datetime.now()
        self.logger.info("Starting product association analysis from order data")
        
        try:
            # Step 1: Analyze current order patterns
            raw_associations = self._analyze_order_patterns()
            
            # Step 2: Apply business rules and weighting
            processed_associations = self._apply_business_rules(raw_associations)
            
            # Step 3: Update database with new associations
            stats = self._update_association_table(processed_associations)
            
            # Step 4: Calculate final statistics
            stats.processing_time_seconds = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"Association analysis completed in {stats.processing_time_seconds:.2f}s")
            return stats
            
        except Exception as e:
            self.logger.error(f"Association analysis failed: {e}")
            raise

    def _analyze_order_patterns(self) -> List[Tuple[int, int, int, datetime]]:
        """
        Analyze order data to find products frequently bought together
        
        Returns: List of (product_a_id, product_b_id, frequency, last_order_date)
        """
        try:
            cursor = self.connection.cursor()
            
            # Calculate analysis period
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.config.analysis_period_days)
            
            # Complex query to find product associations with business intelligence
            query = """
                WITH order_pairs AS (
                    -- Find all product pairs within the same order
                    SELECT 
                        CASE WHEN oi1.product_id < oi2.product_id 
                             THEN oi1.product_id 
                             ELSE oi2.product_id 
                        END as product_a_id,
                        CASE WHEN oi1.product_id < oi2.product_id 
                             THEN oi2.product_id 
                             ELSE oi1.product_id 
                        END as product_b_id,
                        o.order_date,
                        o.order_id,
                        oi1.quantity as qty_a,
                        oi2.quantity as qty_b
                    FROM order_items oi1
                    JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                    JOIN orders o ON oi1.order_id = o.order_id
                    WHERE oi1.product_id != oi2.product_id
                      AND o.order_date >= %s
                      AND o.order_date <= %s
                      AND o.status IN ('completed', 'delivered', 'shipped')  -- Only successful orders
                ),
                weighted_associations AS (
                    -- Calculate frequency with recency weighting
                    SELECT 
                        product_a_id,
                        product_b_id,
                        COUNT(*) as raw_frequency,
                        MAX(order_date) as last_order_date,
                        COUNT(DISTINCT order_id) as unique_orders,
                        -- Recency weight: more recent orders count more
                        CASE WHEN %s THEN
                            SUM(
                                CASE 
                                    WHEN order_date >= (CURRENT_DATE - INTERVAL '30 days') THEN 2.0
                                    WHEN order_date >= (CURRENT_DATE - INTERVAL '90 days') THEN 1.5
                                    WHEN order_date >= (CURRENT_DATE - INTERVAL '180 days') THEN 1.2
                                    ELSE 1.0
                                END
                            )
                        ELSE COUNT(*)
                        END as weighted_frequency
                    FROM order_pairs
                    GROUP BY product_a_id, product_b_id
                )
                SELECT 
                    product_a_id,
                    product_b_id,
                    ROUND(weighted_frequency)::INTEGER as frequency,
                    last_order_date
                FROM weighted_associations
                WHERE weighted_frequency >= %s
                  AND unique_orders >= 2  -- Must appear in at least 2 different orders
                ORDER BY weighted_frequency DESC
                LIMIT 100000  -- Prevent runaway queries
            """
            
            cursor.execute(query, (
                start_date, 
                end_date, 
                self.config.recency_weight,
                self.config.min_frequency_threshold
            ))
            
            results = cursor.fetchall()
            cursor.close()
            
            self.logger.info(f"Found {len(results)} raw product associations from order data")
            return results
            
        except psycopg2.Error as e:
            self.logger.error(f"Error analyzing order patterns: {e}")
            raise

    def _apply_business_rules(self, raw_associations: List[Tuple]) -> List[Tuple]:
        """
        Apply business rules to refine associations
        
        Business rules:
        - Boost cross-category associations (more interesting)
        - Penalize same-brand associations (less surprising)
        - Limit associations per product to prevent explosion
        - Filter out low-quality associations
        """
        try:
            # Get product metadata for business rules
            product_metadata = self._get_product_metadata()
            
            processed = []
            product_association_counts = {}
            
            for product_a_id, product_b_id, frequency, last_order_date in raw_associations:
                
                # Skip if we don't have metadata
                if product_a_id not in product_metadata or product_b_id not in product_metadata:
                    continue
                
                # Apply business rule adjustments
                adjusted_frequency = self._apply_frequency_adjustments(
                    product_a_id, product_b_id, frequency, product_metadata
                )
                
                # Check association limits per product
                if self._within_association_limits(product_a_id, product_b_id, product_association_counts):
                    processed.append((product_a_id, product_b_id, adjusted_frequency, last_order_date))
                    
                    # Update counts
                    product_association_counts[product_a_id] = product_association_counts.get(product_a_id, 0) + 1
                    product_association_counts[product_b_id] = product_association_counts.get(product_b_id, 0) + 1
            
            self.logger.info(f"Applied business rules: {len(processed)} associations after filtering")
            return processed
            
        except Exception as e:
            self.logger.error(f"Error applying business rules: {e}")
            raise

    def _get_product_metadata(self) -> Dict[int, Dict[str, Any]]:
        """Get product metadata needed for business rules"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                SELECT 
                    p.product_id,
                    p.brand,
                    p.category_id,
                    c.category_name,
                    c.parent_category_id,
                    COALESCE(parent.category_name, c.category_name) as root_category
                FROM products p
                JOIN categories c ON p.category_id = c.category_id
                LEFT JOIN categories parent ON c.parent_category_id = parent.category_id
                WHERE p.is_active = true
            """
            
            cursor.execute(query)
            
            metadata = {}
            for row in cursor.fetchall():
                product_id, brand, category_id, category_name, parent_category_id, root_category = row
                metadata[product_id] = {
                    'brand': brand,
                    'category_id': category_id,
                    'category_name': category_name,
                    'parent_category_id': parent_category_id,
                    'root_category': root_category
                }
            
            cursor.close()
            return metadata
            
        except psycopg2.Error as e:
            self.logger.error(f"Error getting product metadata: {e}")
            return {}

    def _apply_frequency_adjustments(self, product_a_id: int, product_b_id: int, 
                                   frequency: int, metadata: Dict) -> int:
        """Apply business rule adjustments to frequency"""
        
        product_a = metadata[product_a_id]
        product_b = metadata[product_b_id]
        
        adjusted_frequency = float(frequency)
        
        # Cross-category boost (more interesting associations)
        if product_a['root_category'] != product_b['root_category']:
            adjusted_frequency *= self.config.cross_category_boost
            
        # Same-brand penalty (less surprising associations)
        if product_a['brand'] == product_b['brand'] and product_a['brand'] not in ['Generic', None]:
            adjusted_frequency *= self.config.same_brand_penalty
        
        # Ensure minimum frequency
        return max(1, int(round(adjusted_frequency)))

    def _within_association_limits(self, product_a_id: int, product_b_id: int, 
                                 current_counts: Dict[int, int]) -> bool:
        """Check if products are within association limits"""
        
        count_a = current_counts.get(product_a_id, 0)
        count_b = current_counts.get(product_b_id, 0)
        
        return (count_a < self.config.max_associations_per_product and 
                count_b < self.config.max_associations_per_product)

    def _update_association_table(self, associations: List[Tuple]) -> AssociationStats:
        """Update the database with new/updated associations"""
        
        if not associations:
            return AssociationStats()
        
        try:
            cursor = self.connection.cursor()
            
            # Get existing associations for comparison
            cursor.execute("SELECT COUNT(*) FROM product_associations")
            initial_count = cursor.fetchone()[0]
            
            # Batch upsert associations
            upsert_query = """
                INSERT INTO product_associations (product_a_id, product_b_id, frequency_count, last_calculated)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_a_id, product_b_id) 
                DO UPDATE SET 
                    frequency_count = EXCLUDED.frequency_count,
                    last_calculated = EXCLUDED.last_calculated
            """
            
            # Prepare data for batch insert
            upsert_data = [(a_id, b_id, freq, last_date) for a_id, b_id, freq, last_date in associations]
            
            # Execute batch upsert
            cursor.executemany(upsert_query, upsert_data)
            rows_affected = cursor.rowcount
            
            self.connection.commit()
            
            # Get final counts
            cursor.execute("SELECT COUNT(*) FROM product_associations")
            final_count = cursor.fetchone()[0]
            
            # Calculate cross-category vs same-category
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN c1.parent_category_id != c2.parent_category_id THEN 1 END) as cross_category,
                    COUNT(CASE WHEN c1.parent_category_id = c2.parent_category_id THEN 1 END) as same_category
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                JOIN categories c1 ON p1.category_id = c1.category_id
                JOIN categories c2 ON p2.category_id = c2.category_id
            """)
            
            cross_cat, same_cat = cursor.fetchone()
            cursor.close()
            
            # Build statistics
            stats = AssociationStats(
                total_associations=final_count,
                new_associations=max(0, final_count - initial_count),
                updated_associations=min(rows_affected, initial_count),
                cross_category_associations=cross_cat or 0,
                same_category_associations=same_cat or 0
            )
            
            self.logger.info(f"Updated associations: {stats.new_associations} new, {stats.updated_associations} updated")
            return stats
            
        except psycopg2.Error as e:
            self.logger.error(f"Error updating association table: {e}")
            self.connection.rollback()
            raise

    def get_association_insights(self) -> Dict[str, Any]:
        """Generate business insights from current associations"""
        try:
            cursor = self.connection.cursor()
            
            insights = {}
            
            # Top product pairs
            cursor.execute("""
                SELECT 
                    p1.product_name,
                    p2.product_name,
                    pa.frequency_count
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                ORDER BY pa.frequency_count DESC
                LIMIT 10
            """)
            insights['top_pairs'] = cursor.fetchall()
            
            # Category cross-associations
            cursor.execute("""
                SELECT 
                    c1.category_name as category_a,
                    c2.category_name as category_b,
                    COUNT(*) as association_count,
                    AVG(pa.frequency_count) as avg_frequency
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                JOIN categories c1 ON p1.category_id = c1.category_id
                JOIN categories c2 ON p2.category_id = c2.category_id
                WHERE c1.category_id != c2.category_id
                GROUP BY c1.category_name, c2.category_name
                ORDER BY association_count DESC
                LIMIT 10
            """)
            insights['category_cross_associations'] = cursor.fetchall()
            
            # Brand ecosystem analysis
            cursor.execute("""
                SELECT 
                    p1.brand as brand_a,
                    p2.brand as brand_b,
                    COUNT(*) as association_count
                FROM product_associations pa
                JOIN products p1 ON pa.product_a_id = p1.product_id
                JOIN products p2 ON pa.product_b_id = p2.product_id
                WHERE p1.brand != p2.brand
                  AND p1.brand IS NOT NULL 
                  AND p2.brand IS NOT NULL
                GROUP BY p1.brand, p2.brand
                ORDER BY association_count DESC
                LIMIT 10
            """)
            insights['brand_cross_associations'] = cursor.fetchall()
            
            cursor.close()
            return insights
            
        except psycopg2.Error as e:
            self.logger.error(f"Error generating insights: {e}")
            return {}

    def cleanup_stale_associations(self, days_old: int = 180) -> int:
        """Remove associations based on very old order data"""
        try:
            cursor = self.connection.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            cursor.execute("""
                DELETE FROM product_associations 
                WHERE last_calculated < %s
            """, (cutoff_date,))
            
            deleted_count = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            self.logger.info(f"Cleaned up {deleted_count} stale associations")
            return deleted_count
            
        except psycopg2.Error as e:
            self.logger.error(f"Error cleaning up associations: {e}")
            self.connection.rollback()
            return 0