import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Provider } from 'react-redux';
import store from './store';

import LoginScreen from './screens/LoginScreen';
import DashboardScreen from './screens/DashboardScreen';
import PrivateRoute from './components/PrivateRoute';
import CustomToastContainer from './components/CustomToast';

import './App.css';

function App() {
  return (
    <Provider store={store}>
      <Router>
        <div className="App">
          <Routes>
            <Route path="/login" element={<LoginScreen />} />
            <Route
              path="/dashboard"
              element={
                <PrivateRoute>
                  <DashboardScreen />
                </PrivateRoute>
              }
            />
            <Route path="/" element={<Navigate to="/login" replace />} />
          </Routes>
          <CustomToastContainer />
        </div>
      </Router>
    </Provider>
  );
}

export default App;
