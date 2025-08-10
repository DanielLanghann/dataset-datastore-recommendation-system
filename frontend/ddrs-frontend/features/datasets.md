# Login and Logout Feature
## User Story
As a user i want to create and see datasets in order to manage them.

## Acceptance Criterias
- On the Dashboard is a header with the the following items:
     - Matching Engine (currently empty)
     - Datastores
     - Datasets (currently empty)

- If the user is clicking on "Datasets" we show a loading spinner and the Datasets will be listed. These are the columns:
    - id
    - Created at (format: dd.mm.yyyy reformat this timestamp 2025-08-10T10:59:32.587511Z)
    - Updated at (format: dd.mm.yyyy)
    - Name
    - Short Description
    - Current Datastore
    - Data Structure
    - Growth rate
    - Access Patterns
    - Query complexity
    - Estimated Size (GB)
    - Relationships
    - Number of Queries
- On the upper right corner is a "Create New" Button (Secondary Button in blue).
- If the user is clicking on the Create Button we show a modal dialog with this input:
 - Title: Create a new Dataset
 - Inputfield "Name" (required)
 - Inputfield "Short Description" (Please consider the limit for this field based on the backend!, required)
 - Selectfield "Current Datastore": Only one choice. The user has to choose a datastore of the related Datastores or None.
 In the Select field the ids and the names of the Datastores are visible.
 - Selectfield "Data Structure": One Selection of the DATA_STRUCTURE_CHOICES in constants/datasetConstants.js (required)
 - Selectfield "Growth Rate": One Selection of the GROWTH_RATE_CHOICES in constants/datasetConstants.js (required)
 - Selectfield "Access Patterns": One Selection of the ACCESS_PATTERN_CHOICES in constants/datasetConstants.js (required)
 - Selectfield "Query Complexity": One Selection of the QUERY_COMPLEXITY_CHOICES in constants/datasetConstants.js (required)
 - A field and or button where the use can upload a JSON File with the Database "Properties" like: 
    "properties": [
            "customer_id",
            "transaction_id",
            "amount",
            "timestamp",
            "product_category"
        ],
 - A field and or button where the use can upload a JSON File with the Database "Sample Data" like: 
    sample_data": [
        [
            "12345",
            "TXN001",
            "99.99",
            "2025-01-15T10:30:00Z",
            "electronics"
        ],
        [
            "12346",
            "TXN002",
            "149.50",
            "2025-01-15T11:15:00Z",
            "clothing"
        ]
    ],
 - Inputfield "Estimated Size (GB)": Numbers only and > 0
 - Inputfield "Average Query Time": Numbers only and > 0
 - Inputfield "Queries per Day": Numbers only and > 0
 - Selectfield: The user can select zero, one or many "Relationships" to other Datasets. In the Selectfield the user can see the ids and names of other datasets to select.
 - A field and or button where the use can upload a JSON File with the Database "Queries" like: 
    queries": [
        [
            "SELECT * FROM ...."
        ],
        [
            "INSERT ....
        ]
    ],

 - There is also a "Cancel" Button to just close the dialog.
 - If the user hit the create button, we show a loading spinner.
 - If the request was successful we show a Success Toast "Dataset created successfully" and move back to the List.
 - If the request was not successful we show an Error Toast "Failed to create the Dataset: {error Message}" and stay in the
 Dialog and keeping the input in the dialog, so that the user don't need to set the data again.
- Also add a Delete and Update Function like in the Datatore Menu.

 - Bring all together in App.js

## Endpoints
- base_url = http://localhost:8000
- List Datasets: {{base_url}}/api/datasets/ (GET)
- Create Dataset: {{base_url}}/api/datasets/ (POST)

## Tools and Restrictions:
- Use the DatastoresScreen.js as an orientation.

## UI
- Use Tailwind
- Use the font MarkPro available in fonts
- Use the CustomTable Component

