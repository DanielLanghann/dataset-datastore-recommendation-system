# Login and Logout Feature
## User Story
As a user i want to create and see datastores in order to manage them.

## Acceptance Criterias
- On the Dashboard is a header with the the following items:
     - Matching Engine (currently empty)
     - Datastores
     - Datasets (currently empty)
- If the user is clicking on one of the items, the item will highlighted.

- If the user is clicking on "Datastores" we show a loading spinner and the datastores will be listed. These are the columns:
    - id
    - Created at (format: dd.mm.yyyy)
    - Updated at (format: dd.mm.yyyy)
    - Name
    - Type (using the type_display property)
    - System (using the system_display property)
    - Description
    - Active (yes or no)
    - Max Connections
    - Average Response Time
    - Storagy Capacity (GB)
- On the upper right corner is a "Create New" Button (Secondary Button in blue).
- If the user is clicking on the Create Button we show a modal dialog with this input:
 - Title: Create a new Datastore
 - Inputfield "Name" (required)
 - Selectfield "Type" (one selection only, using the values in datastoreConstants.js, required)
 - Selectfield "System" which is auto filtered based on the "Type", so that the user only can select
 valid systems based on the "Type" (required)
 - Inputfield "Description" consider the backend based character limit
 - Inputfield: "Server"
 - Inputfield: "Port"
 - Inputfield: "User"
 - Passwordfield: "Password"
 - Passwordfield: "Confirm Password"
 - Switch: "Active" default to true
 - Inputfield: "Max Connections" (Only numbers > 0 are allowed)
 - Inputfield: "Average Response Time" (Only numbers > 0 are allowed)
 - Inputfield: "Storage Capacity in GB" (Only numbers > 0 are allowed)
 - If the user has set all required fields a "Create" Button is enabled, and the user can create the 
 new Datastore. 
 - There is also a "Cancel" Button to just close the dialog.
 - If the user hit the create button, we show a loading spinner.
 - If the request was successful we show a Success Toast "Datastore created successfully" and move back to the List.
 - If the request was not successful we show a Error Toast "Failed to create the Datstore: {error Message}" and stay in the
 Dialog and keeping the input in the dialog, so that the user don't need to set the data again.

 - Bring all together in App.js

## Endpoints
- base_url = http://localhost:8000
- List Datastores: {{base_url}}/api/datastores/ (GET)
- Create Datastore: {{base_url}}/api/datastores/ (POST)

## Tools and Restrictions:
- Use the Login and Logout Feature as an orientation.

## UI
- Use Tailwind
- Use the font MarkPro available in fonts
- Use the CustomTable Component
- Create new Secondary Buttons with blue color

