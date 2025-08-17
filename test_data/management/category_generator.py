"""
Category Hierarchy Generator Module

This module provides intelligent hierarchical category generation for e-commerce test data.
It creates realistic category trees that scale from small datasets to millions of records
while maintaining proper parent-child relationships and balanced distribution.

Usage:
    from category_hierarchy_generator import CategoryHierarchyGenerator
    
    generator = CategoryHierarchyGenerator(db_connection)
    categories = generator.generate_categories(count=10000)
"""

import random
import psycopg2
from faker import Faker
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass

fake = Faker()

@dataclass
class HierarchyConfig:
    """Configuration for hierarchy generation"""
    max_depth: int = 4
    branching_factor: Dict[int, Tuple[int, int]] = None
    level_probability: Dict[int, float] = None
    base_ratios: Dict[int, float] = None
    
    def __post_init__(self):
        if self.branching_factor is None:
            self.branching_factor = {
                0: (3, 8),   # Root level: 3-8 main categories
                1: (2, 6),   # Main level: 2-6 subcategories each
                2: (1, 4),   # Sub level: 1-4 sub-subcategories each
                3: (0, 2)    # Leaf level: 0-2 final subcategories
            }
        
        if self.level_probability is None:
            self.level_probability = {
                0: 1.0,  # Always create main categories
                1: 0.8,  # 80% chance to create subcategories
                2: 0.6,  # 60% chance to create sub-subcategories
                3: 0.3   # 30% chance to create final level
            }
        
        if self.base_ratios is None:
            self.base_ratios = {
                0: 0.15,  # 15% root categories
                1: 0.30,  # 30% main subcategories  
                2: 0.35,  # 35% sub-subcategories
                3: 0.20   # 20% leaf categories
            }

@dataclass
class ExistingStructure:
    """Information about existing category structure"""
    total_existing: int = 0
    level_distribution: Dict[int, int] = None
    root_categories: List[Tuple[str, int]] = None
    root_names: List[str] = None
    needs_base_structure: bool = True
    missing_roots: List[str] = None
    max_existing_level: int = -1
    
    def __post_init__(self):
        if self.level_distribution is None:
            self.level_distribution = {}
        if self.root_categories is None:
            self.root_categories = []
        if self.root_names is None:
            self.root_names = []
        if self.missing_roots is None:
            self.missing_roots = []

class CategoryHierarchyGenerator:
    """
    Generates hierarchical category structures for e-commerce databases.
    
    Features:
    - Scalable from 1 to millions of categories
    - Maintains realistic industry hierarchies
    - Handles incremental data additions
    - Optimized for both PostgreSQL and Neo4j
    """
    
    def __init__(self, db_connection: psycopg2.extensions.connection, config: Optional[HierarchyConfig] = None):
        """
        Initialize the category hierarchy generator.
        
        Args:
            db_connection: Active PostgreSQL database connection
            config: Optional hierarchy configuration
        """
        self.connection = db_connection
        self.config = config or HierarchyConfig()
        self.execution_log = []
        
        # Predefined realistic category templates
        self.category_templates = {
            'Technology & Electronics': {
                'Mobile Devices': ['Smartphones', 'Tablets', 'Smartwatches', 'Phone Accessories'],
                'Computers': ['Laptops', 'Desktops', 'Gaming PCs', 'Workstations', 'Servers'],
                'Audio & Video': ['Headphones', 'Speakers', 'TVs', 'Cameras', 'Home Theater'],
                'Gaming': ['Consoles', 'PC Games', 'Gaming Accessories', 'VR Equipment'],
                'Smart Home': ['Security Systems', 'Smart Lighting', 'Climate Control', 'Smart Appliances']
            },
            'Fashion & Clothing': {
                'Men\'s Clothing': ['Shirts', 'Pants', 'Jackets', 'Underwear', 'Activewear'],
                'Women\'s Clothing': ['Dresses', 'Tops', 'Bottoms', 'Intimates', 'Maternity'],
                'Children\'s Clothing': ['Boys', 'Girls', 'Baby', 'School Uniforms'],
                'Shoes & Footwear': ['Athletic Shoes', 'Casual Shoes', 'Formal Shoes', 'Boots', 'Sandals'],
                'Accessories': ['Bags', 'Jewelry', 'Watches', 'Belts', 'Hats']
            },
            'Home & Living': {
                'Furniture': ['Living Room', 'Bedroom', 'Dining Room', 'Office', 'Outdoor'],
                'Kitchen & Dining': ['Appliances', 'Cookware', 'Utensils', 'Storage', 'Tableware'],
                'Home Decor': ['Lighting', 'Wall Art', 'Textiles', 'Ornaments', 'Mirrors'],
                'Garden & Outdoor': ['Garden Tools', 'Plants', 'Outdoor Furniture', 'Irrigation', 'Landscaping'],
                'Storage & Organization': ['Closet Systems', 'Shelving', 'Containers', 'Garage Storage']
            },
            'Health & Wellness': {
                'Fitness Equipment': ['Cardio Machines', 'Strength Training', 'Yoga & Pilates', 'Accessories'],
                'Health Supplements': ['Vitamins', 'Protein', 'Herbal', 'Sports Nutrition'],
                'Personal Care': ['Skincare', 'Hair Care', 'Oral Care', 'Body Care'],
                'Medical Supplies': ['First Aid', 'Mobility Aids', 'Monitoring Devices', 'Therapy Equipment']
            },
            'Books & Education': {
                'Technical Books': ['Programming', 'Data Science', 'Engineering', 'Mathematics', 'IT Certification'],
                'Fiction': ['Mystery & Thriller', 'Romance', 'Science Fiction', 'Fantasy', 'Literary Fiction'],
                'Non-Fiction': ['Biography', 'History', 'Self-Help', 'Business', 'Science'],
                'Educational Materials': ['Textbooks', 'Reference', 'Children\'s Books', 'Language Learning'],
                'Digital Media': ['E-books', 'Audiobooks', 'Online Courses', 'Software']
            },
            'Sports & Recreation': {
                'Team Sports': ['Soccer', 'Basketball', 'Baseball', 'Football', 'Hockey'],
                'Individual Sports': ['Running', 'Cycling', 'Swimming', 'Tennis', 'Golf'],
                'Outdoor Activities': ['Camping', 'Hiking', 'Fishing', 'Climbing', 'Water Sports'],
                'Winter Sports': ['Skiing', 'Snowboarding', 'Ice Skating', 'Winter Clothing'],
                'Fitness & Training': ['Gym Equipment', 'Personal Training', 'Nutrition', 'Recovery']
            }
        }
        
        # Industry suffixes for generating additional categories
        self.level_suffixes = {
            1: ["Premium", "Professional", "Basic", "Advanced", "Essential", "Pro", "Elite", "Standard"],
            2: ["Collection", "Series", "Line", "Range", "Set", "Kit", "Bundle", "System"],
            3: ["Type A", "Type B", "Model X", "Classic", "Modern", "Deluxe", "Express", "Plus"]
        }

    def log_message(self, message: str, level: str = 'INFO'):
        """Log messages for debugging and monitoring"""
        self.execution_log.append(f"[{level}] {message}")

    def generate_categories(self, count: int) -> List[Tuple[str, str, Optional[int]]]:
        """
        Main entry point for generating hierarchical categories.
        
        Args:
            count: Target number of categories to generate
            
        Returns:
            List of tuples (category_name, description, parent_category_id)
        """
        self.log_message(f"Starting hierarchical category generation for {count} categories")
        
        # Analyze existing structure
        existing_structure = self._analyze_existing_hierarchy()
        
        # Calculate how many new categories we need
        categories_needed = max(0, count - existing_structure.total_existing)
        
        if categories_needed == 0:
            self.log_message(f"Already have {existing_structure.total_existing} categories, no new ones needed")
            return []
        
        self.log_message(f"Generating {categories_needed} new categories to reach target of {count}")
        
        categories = []
        
        # Step 1: Create base hierarchy if needed
        if existing_structure.needs_base_structure:
            base_categories = self._generate_base_hierarchy(existing_structure)
            categories.extend(base_categories)
            categories_needed -= len(base_categories)
            self.log_message(f"Created {len(base_categories)} base hierarchy categories")
        
        # Step 2: Generate additional categories to reach target
        if categories_needed > 0:
            additional_categories = self._generate_scalable_categories(categories_needed, existing_structure)
            categories.extend(additional_categories)
            self.log_message(f"Generated {len(additional_categories)} additional categories")
        
        self.log_message(f"Completed category generation: {len(categories)} total categories created")
        return categories

    def _analyze_existing_hierarchy(self) -> ExistingStructure:
        """Analyze existing category structure in the database"""
        try:
            cursor = self.connection.cursor()
            
            # Get counts by level
            cursor.execute("""
                SELECT 
                    COALESCE(level_depth, 0) as level, 
                    COUNT(*) as count
                FROM Categories 
                GROUP BY level_depth 
                ORDER BY level_depth
            """)
            level_stats = cursor.fetchall()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM Categories")
            total_existing = cursor.fetchone()[0]
            
            # Get existing root categories
            cursor.execute("""
                SELECT category_name, category_id 
                FROM Categories 
                WHERE parent_category_id IS NULL 
                ORDER BY category_name
            """)
            root_categories = cursor.fetchall()
            
            cursor.close()
            
            # Build structure info
            level_distribution = {level: count for level, count in level_stats}
            root_names = [name for name, cat_id in root_categories]
            
            predefined_roots = set(self.category_templates.keys())
            existing_roots = set(root_names)
            needs_base_structure = len(predefined_roots - existing_roots) > 0
            
            structure = ExistingStructure(
                total_existing=total_existing,
                level_distribution=level_distribution,
                root_categories=root_categories,
                root_names=root_names,
                needs_base_structure=needs_base_structure,
                missing_roots=list(predefined_roots - existing_roots),
                max_existing_level=max(level_distribution.keys()) if level_distribution else -1
            )
            
            self.log_message(f"Analyzed existing hierarchy: {total_existing} categories across {len(level_distribution)} levels")
            return structure
            
        except psycopg2.Error as e:
            self.log_message(f"Error analyzing hierarchy: {e}", 'WARNING')
            return ExistingStructure(
                needs_base_structure=True,
                missing_roots=list(self.category_templates.keys())
            )

    def _generate_base_hierarchy(self, existing_structure: ExistingStructure) -> List[Tuple[str, str, Optional[int]]]:
        """Generate missing base hierarchy categories from templates"""
        categories = []
        
        for root_name in existing_structure.missing_roots:
            if root_name in self.category_templates:
                # Create root category
                root_category = (root_name, f"Main category for {root_name.lower()}", None)
                categories.append(root_category)
                
                # Create main subcategories from template
                template = self.category_templates[root_name]
                
                for main_cat_name, sub_categories in template.items():
                    # Create main subcategory
                    main_desc = f"{main_cat_name} under {root_name}"
                    main_category = (f"{root_name} - {main_cat_name}", main_desc, None)
                    categories.append(main_category)
                    
                    # Create some leaf subcategories (limit for base structure)
                    for sub_cat_name in sub_categories[:3]:  # Limit to first 3 for base
                        sub_desc = f"{sub_cat_name} in {main_cat_name}"
                        sub_category = (f"{main_cat_name} - {sub_cat_name}", sub_desc, None)
                        categories.append(sub_category)
        
        return categories

    def _generate_scalable_categories(self, count: int, existing_structure: ExistingStructure) -> List[Tuple[str, str, Optional[int]]]:
        """Generate additional categories using scalable distribution algorithm"""
        
        # Calculate target distribution across levels
        target_distribution = self._calculate_target_distribution(count, existing_structure)
        
        categories = []
        for level, target_count in target_distribution.items():
            if target_count > 0:
                level_categories = self._generate_categories_for_level(level, target_count, existing_structure)
                categories.extend(level_categories)
        
        return categories

    def _calculate_target_distribution(self, count: int, existing_structure: ExistingStructure) -> Dict[int, int]:
        """Calculate optimal distribution of categories across hierarchy levels"""
        
        current_dist = existing_structure.level_distribution
        total_existing = existing_structure.total_existing
        
        # Calculate current ratios if we have existing data
        current_ratios = {}
        if total_existing > 0:
            for level, count_existing in current_dist.items():
                current_ratios[level] = count_existing / total_existing
        
        # Calculate target distribution
        target_distribution = {}
        for level in range(self.config.max_depth):
            base_ratio = self.config.base_ratios.get(level, 0)
            
            # Blend with existing ratios for large datasets to maintain consistency
            if level in current_ratios and total_existing > 100:
                blend_factor = min(0.7, total_existing / 10000)
                target_ratio = (base_ratio * (1 - blend_factor)) + (current_ratios[level] * blend_factor)
            else:
                target_ratio = base_ratio
            
            target_count = max(0, int(count * target_ratio))
            target_distribution[level] = target_count
        
        # Ensure total matches requested count
        total_targeted = sum(target_distribution.values())
        if total_targeted != count:
            # Adjust the largest category proportionally
            max_level = max(target_distribution.keys(), key=lambda k: target_distribution[k])
            target_distribution[max_level] += count - total_targeted
        
        return target_distribution

    def _generate_categories_for_level(self, level: int, count: int, existing_structure: ExistingStructure) -> List[Tuple[str, str, Optional[int]]]:
        """Generate categories for a specific hierarchy level"""
        
        if level == 0:  # Root categories
            return self._generate_root_categories(count)
        else:  # Child categories
            return self._generate_child_categories(level, count)

    def _generate_root_categories(self, count: int) -> List[Tuple[str, str, Optional[int]]]:
        """Generate root-level categories"""
        categories = []
        
        # Extended industry templates for more root categories
        industry_templates = [
            "Technology & Electronics", "Fashion & Beauty", "Home & Living", "Sports & Recreation",
            "Health & Wellness", "Education & Learning", "Entertainment & Media", "Food & Beverage",
            "Travel & Tourism", "Automotive & Transportation", "Business & Finance", "Art & Culture",
            "Environment & Sustainability", "Pets & Animals", "Crafts & Hobbies", "Industrial & Manufacturing",
            "Agriculture & Farming", "Real Estate & Construction", "Legal & Professional Services",
            "Energy & Utilities", "Security & Safety", "Luxury & Premium", "Vintage & Collectibles"
        ]
        
        used_names = set()
        
        for i in range(count):
            if i < len(industry_templates):
                name = industry_templates[i]
            else:
                # Generate creative combinations for additional roots
                adjectives = ["Premium", "Global", "Modern", "Classic", "Innovation", "Digital", "Sustainable"]
                sectors = ["Solutions", "Services", "Products", "Systems", "Technologies", "Marketplace"]
                name = f"{random.choice(adjectives)} {random.choice(sectors)}"
                
                counter = 1
                while name in used_names:
                    name = f"{random.choice(adjectives)} {random.choice(sectors)} {counter}"
                    counter += 1
            
            if name not in used_names:
                used_names.add(name)
                description = f"Main category for {name.lower()} related products and services"
                categories.append((name, description, None))
        
        return categories

    def _generate_child_categories(self, level: int, count: int) -> List[Tuple[str, str, Optional[int]]]:
        """Generate child categories for a given level"""
        categories = []
        
        # Get potential parent categories from the previous level
        potential_parents = self._get_potential_parents(level - 1)
        
        if not potential_parents:
            self.log_message(f"No potential parents found for level {level}", 'WARNING')
            return categories
        
        # Distribute children across parents
        children_per_parent = max(1, count // len(potential_parents))
        remaining = count % len(potential_parents)
        
        for i, (parent_id, parent_name) in enumerate(potential_parents):
            # Some parents get one extra child for even distribution
            children_count = children_per_parent + (1 if i < remaining else 0)
            
            for j in range(children_count):
                child_name = self._generate_child_name(parent_name, level, j)
                description = f"Subcategory of {parent_name}"
                
                # Note: parent_id will be None here because we'll handle hierarchy
                # relationships through a separate update after insertion
                categories.append((child_name, description, None))
        
        return categories

    def _get_potential_parents(self, parent_level: int) -> List[Tuple[int, str]]:
        """Get categories that can serve as parents for the next level"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT category_id, category_name 
                FROM Categories 
                WHERE level_depth = %s 
                ORDER BY category_name
            """, (parent_level,))
            return cursor.fetchall()
        except psycopg2.Error as e:
            self.log_message(f"Error getting potential parents: {e}", 'WARNING')
            return []

    def _generate_child_name(self, parent_name: str, level: int, index: int) -> str:
        """Generate a realistic child category name based on parent"""
        
        # Extract base name from parent (remove prefixes)
        base_name = parent_name.split(" & ")[-1].split(" - ")[-1]
        
        # Generate child name patterns based on level and parent type
        if level == 1:
            if any(keyword in parent_name.lower() for keyword in ['electronics', 'technology']):
                child_bases = ["Mobile", "Computing", "Audio", "Gaming", "Smart Home", "Networking"]
            elif any(keyword in parent_name.lower() for keyword in ['fashion', 'clothing']):
                child_bases = ["Men's", "Women's", "Children's", "Accessories", "Footwear", "Activewear"]
            elif any(keyword in parent_name.lower() for keyword in ['home', 'living']):
                child_bases = ["Kitchen", "Bedroom", "Living Room", "Bathroom", "Garden", "Storage"]
            elif any(keyword in parent_name.lower() for keyword in ['health', 'wellness']):
                child_bases = ["Fitness", "Nutrition", "Personal Care", "Medical", "Mental Health"]
            else:
                child_bases = [f"{fake.word().title()}", f"{fake.word().title()}", f"{fake.word().title()}"]
            
            return f"{base_name} - {child_bases[index % len(child_bases)]}"
        
        else:
            # For deeper levels, use suffix patterns
            suffixes = self.level_suffixes.get(level, ["Category"])
            adjectives = ["Premium", "Basic", "Advanced", "Professional", "Standard", "Deluxe"]
            
            return f"{base_name} - {adjectives[index % len(adjectives)]} {suffixes[index % len(suffixes)]}"

    def update_hierarchy_relationships(self) -> bool:
        """Update parent-child relationships after bulk insert using naming patterns"""
        try:
            cursor = self.connection.cursor()
            
            # Update parent relationships for categories with " - " pattern
            # This assumes categories are named like "Parent - Child"
            cursor.execute("""
                UPDATE Categories 
                SET parent_category_id = parent.category_id
                FROM Categories parent
                WHERE Categories.category_name LIKE parent.category_name || ' - %'
                  AND Categories.parent_category_id IS NULL
                  AND parent.category_id != Categories.category_id
            """)
            
            updated_count = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            if updated_count > 0:
                self.log_message(f"Updated {updated_count} category parent relationships")
            
            return True
            
        except psycopg2.Error as e:
            self.log_message(f"Error updating hierarchy relationships: {e}", 'ERROR')
            self.connection.rollback()
            return False

    def get_hierarchy_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the generated hierarchy"""
        try:
            cursor = self.connection.cursor()
            
            # Level distribution
            cursor.execute("""
                SELECT 
                    COALESCE(level_depth, 0) as level, 
                    COUNT(*) as count
                FROM Categories 
                GROUP BY level_depth 
                ORDER BY level_depth
            """)
            level_distribution = dict(cursor.fetchall())
            
            # Root categories
            cursor.execute("""
                SELECT COUNT(*) 
                FROM Categories 
                WHERE parent_category_id IS NULL
            """)
            root_count = cursor.fetchone()[0]
            
            # Leaf categories
            cursor.execute("""
                SELECT COUNT(*) 
                FROM Categories 
                WHERE is_leaf = true
            """)
            leaf_count = cursor.fetchone()[0]
            
            # Total categories
            cursor.execute("SELECT COUNT(*) FROM Categories")
            total_count = cursor.fetchone()[0]
            
            # Average depth
            cursor.execute("""
                SELECT AVG(level_depth::float) 
                FROM Categories 
                WHERE level_depth IS NOT NULL
            """)
            avg_depth = cursor.fetchone()[0] or 0
            
            cursor.close()
            
            return {
                'total_categories': total_count,
                'root_categories': root_count,
                'leaf_categories': leaf_count,
                'level_distribution': level_distribution,
                'average_depth': float(avg_depth),
                'max_depth': max(level_distribution.keys()) if level_distribution else 0,
                'execution_log': self.execution_log
            }
            
        except psycopg2.Error as e:
            self.log_message(f"Error getting hierarchy statistics: {e}", 'ERROR')
            return {'error': str(e), 'execution_log': self.execution_log}

    def verify_hierarchy_integrity(self) -> Dict[str, Any]:
        """Verify the integrity of the generated hierarchy"""
        issues = []
        
        try:
            cursor = self.connection.cursor()
            
            # Check for orphaned categories (parent doesn't exist)
            cursor.execute("""
                SELECT c.category_name 
                FROM Categories c 
                WHERE c.parent_category_id IS NOT NULL 
                  AND NOT EXISTS (
                      SELECT 1 FROM Categories p 
                      WHERE p.category_id = c.parent_category_id
                  )
                LIMIT 10
            """)
            orphaned = cursor.fetchall()
            if orphaned:
                issues.append(f"Found {len(orphaned)} orphaned categories")
            
            # Check for circular references
            cursor.execute("""
                WITH RECURSIVE category_paths AS (
                    SELECT category_id, parent_category_id, ARRAY[category_id] as path
                    FROM Categories
                    WHERE parent_category_id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT c.category_id, c.parent_category_id, cp.path || c.category_id
                    FROM Categories c
                    JOIN category_paths cp ON c.category_id = cp.parent_category_id
                    WHERE c.category_id != ALL(cp.path)
                      AND array_length(cp.path, 1) < 10
                )
                SELECT COUNT(*) FROM category_paths WHERE category_id = ANY(path[1:array_length(path,1)-1])
            """)
            circular_count = cursor.fetchone()[0]
            if circular_count > 0:
                issues.append(f"Found {circular_count} circular references")
            
            cursor.close()
            
            return {
                'is_valid': len(issues) == 0,
                'issues': issues,
                'message': 'Hierarchy is valid' if len(issues) == 0 else 'Issues found in hierarchy'
            }
            
        except psycopg2.Error as e:
            return {
                'is_valid': False,
                'issues': [f"Database error during verification: {e}"],
                'message': 'Verification failed'
            }