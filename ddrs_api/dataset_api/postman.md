# Postman API Test Examples for Dataset API

## Environment Variables
Make sure you have these set in your Postman environment:
- `base_url`: `http://localhost:8000`
- `auth_token`: Your Django Token (generated via Django admin or script)

## Authentication
Add to Headers for all requests:
```
Authorization: Token {{auth_token}}
```

---

## 1. DATASET ENDPOINTS

### 1.1 Create Dataset (POST)
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "name": "Customer Transaction Data",
    "short_description": "Daily customer transaction records from e-commerce platform",
    "data_structure": "structured",
    "growth_rate": "high",
    "access_patterns": "read_heavy",
    "query_complexity": "medium",
    "properties": ["customer_id", "transaction_id", "amount", "timestamp", "product_category"],
    "sample_data": [
        ["12345", "TXN001", "99.99", "2025-01-15T10:30:00Z", "electronics"],
        ["12346", "TXN002", "149.50", "2025-01-15T11:15:00Z", "clothing"]
    ],
    "estimated_size_gb": 25.5,
    "avg_query_time_ms": 150.0,
    "queries_per_day": 5000.0,
    "current_datastore": 1
}
```

### 1.2 Create Dataset with Relationships and Queries
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "name": "User Profile Data",
    "short_description": "User demographic and preference data",
    "data_structure": "semi_structured",
    "growth_rate": "medium",
    "access_patterns": "read_write_heavy",
    "query_complexity": "low",
    "properties": ["user_id", "email", "age", "preferences", "registration_date"],
    "sample_data": [
        ["user001", "john@example.com", "28", "{\"newsletter\": true}", "2024-01-01"],
        ["user002", "jane@example.com", "34", "{\"newsletter\": false}", "2024-01-02"]
    ],
    "estimated_size_gb": 5.2,
    "avg_query_time_ms": 50.0,
    "queries_per_day": 1200.0,
    "queries": [
        {
            "name": "Get User by ID",
            "query_text": "SELECT * FROM users WHERE user_id = ?",
            "query_type": "select",
            "frequency": "high",
            "avg_execution_time_ms": 25.0,
            "description": "Retrieve user profile by ID"
        },
        {
            "name": "Update User Preferences",
            "query_text": "UPDATE users SET preferences = ? WHERE user_id = ?",
            "query_type": "update",
            "frequency": "medium",
            "avg_execution_time_ms": 75.0,
            "description": "Update user preference settings"
        }
    ],
    "relationships": [
        {
            "to_dataset": 1,
            "relationship_type": "foreign_key",
            "strength": 8,
            "description": "Users can have multiple transactions",
            "is_active": true
        }
    ]
}
```

### 1.3 List Datasets (GET)
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?page=1&page_size=10&search=customer&data_structure=structured&ordering=-created_at
```

### 1.4 Get Dataset Detail (GET)
**URL:** `{{base_url}}/api/datasets/1/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```

### 1.5 Update Dataset (PUT)
**URL:** `{{base_url}}/api/datasets/1/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "name": "Customer Transaction Data - Updated",
    "short_description": "Updated description with additional fields",
    "data_structure": "structured",
    "growth_rate": "high",
    "access_patterns": "analytical",
    "query_complexity": "high",
    "properties": ["customer_id", "transaction_id", "amount", "timestamp", "product_category", "payment_method"],
    "sample_data": [
        ["12345", "TXN001", "99.99", "2025-01-15T10:30:00Z", "electronics", "credit_card"],
        ["12346", "TXN002", "149.50", "2025-01-15T11:15:00Z", "clothing", "paypal"]
    ],
    "estimated_size_gb": 30.0,
    "avg_query_time_ms": 200.0,
    "queries_per_day": 7500.0
}
```

### 1.6 Partial Update Dataset (PATCH)
**URL:** `PATCH /api/datasets/1/`
**Body (JSON):**
```json
{
    "estimated_size_gb": 35.0,
    "avg_query_time_ms": 175.0
}
```

### 1.7 Delete Dataset (DELETE)
**URL:** `DELETE /api/datasets/1/`

---

## 2. QUERY ENDPOINTS

### 2.1 Create Query (POST)
**URL:** `POST /api/queries/`
**Body (JSON):**
```json
{
    "dataset": 1,
    "name": "Monthly Sales Report",
    "query_text": "SELECT product_category, SUM(amount) as total_sales FROM transactions WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 MONTH) GROUP BY product_category",
    "query_type": "aggregate",
    "frequency": "low",
    "avg_execution_time_ms": 2500.0,
    "description": "Generate monthly sales report by product category"
}
```

### 2.2 List Queries (GET)
**URL:** `{{base_url}}/api/queries/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?dataset_id=1&query_type=select&frequency=high&search=sales
```

### 2.3 Get Query Detail (GET)
**URL:** `{{base_url}}/api/queries/1/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```

### 2.4 Update Query (PUT)
**URL:** `{{base_url}}/api/queries/1/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "dataset": 1,
    "name": "Updated Monthly Sales Report",
    "query_text": "SELECT product_category, SUM(amount) as total_sales, COUNT(*) as transaction_count FROM transactions WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 MONTH) GROUP BY product_category ORDER BY total_sales DESC",
    "query_type": "aggregate",
    "frequency": "medium",
    "avg_execution_time_ms": 3000.0,
    "description": "Enhanced monthly sales report with transaction counts"
}
```

### 2.5 Delete Query (DELETE)
**URL:** `{{base_url}}/api/queries/1/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```

---

## 3. RELATIONSHIP ENDPOINTS

### 3.1 Create Relationship (POST)
**URL:** `{{base_url}}/api/relationships/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "from_dataset": 2,
    "to_dataset": 1,
    "relationship_type": "one_to_many",
    "strength": 9,
    "description": "Each user can have multiple transactions",
    "is_active": true
}
```

### 3.2 List Relationships (GET)
**URL:** `{{base_url}}/api/relationships/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?dataset_id=1&relationship_type=foreign_key&is_active=true&active_only=true
```

### 3.3 Get Relationship Detail (GET)
**URL:** `{{base_url}}/api/relationships/1/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```

### 3.4 Update Relationship (PUT)
**URL:** `{{base_url}}/api/relationships/1/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "from_dataset": 2,
    "to_dataset": 1,
    "relationship_type": "one_to_many",
    "strength": 10,
    "description": "Strong relationship: Each user can have multiple transactions with full referential integrity",
    "is_active": true
}
```

### 3.5 Delete Relationship (DELETE)
**URL:** `{{base_url}}/api/relationships/1/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```

---

## 4. ADVANCED FILTERING EXAMPLES

### 4.1 Complex Dataset Filtering
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?data_structure=structured&growth_rate=high&access_patterns=read_heavy&ordering=-estimated_size_gb&page_size=5
```

### 4.2 Search Datasets
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?search=transaction&ordering=name
```

### 4.3 Filter Queries by Dataset and Type
**URL:** `{{base_url}}/api/queries/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?dataset_id=1&query_type=select&frequency=high&ordering=-avg_execution_time_ms
```

### 4.4 Filter Relationships for Specific Dataset
**URL:** `{{base_url}}/api/relationships/`
**Headers:** 
```
Authorization: Token {{auth_token}}
```
**Query Parameters:**
```
?dataset_id=1&active_only=true&ordering=-strength
```

---

## 5. ERROR TESTING EXAMPLES

### 5.1 Invalid Data Structure
**URL:** `{{base_url}}/api/datasets/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "name": "Test Dataset",
    "data_structure": "invalid_structure",
    "properties": "not_a_list"
}
```

### 5.2 Missing Required Fields
**URL:** `{{base_url}}/api/queries/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "name": "Incomplete Query"
}
```

### 5.3 Invalid Relationship Strength
**URL:** `{{base_url}}/api/relationships/`
**Headers:** 
```
Content-Type: application/json
Authorization: Token {{auth_token}}
```
**Body (JSON):**
```json
{
    "from_dataset": 1,
    "to_dataset": 2,
    "relationship_type": "foreign_key",
    "strength": 15,
    "description": "Invalid strength value (should be 1-10)"
}
```

---

## 6. TESTING SEQUENCE

1. **Create a basic dataset** (Example 1.1)
2. **Create another dataset with queries and relationships** (Example 1.2)
3. **List datasets** to verify creation (Example 1.3)
4. **Get dataset details** to see relationships and queries (Example 1.4)
5. **Create additional queries** for the datasets (Example 2.1)
6. **Create additional relationships** (Example 3.1)
7. **Test filtering and searching** (Examples 4.1-4.4)
8. **Update datasets, queries, and relationships** (Examples 1.5, 2.4, 3.4)
9. **Test error cases** (Examples 5.1-5.3)

## Notes:
- Make sure your environment variables are set: `{{base_url}}` and `{{auth_token}}`
- Your Django Token authentication should be working (`Authorization: Token {{auth_token}}`)
- Adjust the `current_datastore` ID to match existing datastores in your system
- Dataset IDs in relationships should refer to existing datasets
- All timestamps should be in ISO format
- Make sure your Django app is running on `http://localhost:8000`

## Quick Setup Reminder:
1. **Generate Django Token** (if you haven't already):
   ```python
   from django.contrib.auth.models import User
   from rest_framework.authtoken.models import Token
   
   user = User.objects.get(username='your_username')
   token, created = Token.objects.get_or_create(user=user)
   print(f'Token: {token.key}')
   ```

2. **Set Postman Environment Variables**:
   - `base_url`: `http://localhost:8000`
   - `auth_token`: The token from step 1