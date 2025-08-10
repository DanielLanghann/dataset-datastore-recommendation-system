# Login and Logout Feature
## User Story
As a user i want to login and to get access to the application.
I also want to logout, for safety reasons.

## Acceptance Criterias
- The user can access the frontend application
- There is a login screen showing 
    - Label: "Welcome"
    - Input: "Username" - If the user enters the page, the focus is here
    - Input: "Password"
    - Button: Login
- When the fields Username and Password are filled the Button Login is enabled.
- When the user hits the Login Button there is a loading spinner indicating the login process.
- If the login was successfull, the user enters an empty with just a logout Button in the
upper right corner.
- If the login was not successfull 
    - we show an error toast with the error code from the backend.
    - We keep the input in the input fields, so that the user don't have to set all 
    the input again.
- If the user hits the logout button we call the logout request.
- If the logout was successfull we show a success toast and redirect the user to the login page.

## Endpoints
- base_url = http://localhost:8000
- {{base_url}}/api/auth/login/
- {{base_url}}/api/auth/logout/
We use simple Token System from Django. We need to delete the token from Browser Storage in 
any case after the user hits the logout button.

## Tools and Restrictions:
- Use Redux (already installed)
- Use the folder structure in ./src including the store.js for managing redux.
- Try to create and use reusable components as often as possible.
- Bring all together in App.js
- Add configuration so that we can store the base_url and endpoints in a .env file
and use them for runtime.


## UI
- Use Tailwind
- Use the font MarkPro available in fonts
- Use the already created LoginScreen, CustomPrimaryButton and CustomLoadingSpinner.

