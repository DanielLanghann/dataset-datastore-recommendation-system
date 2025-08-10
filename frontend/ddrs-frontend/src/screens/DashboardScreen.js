import React, { useState } from 'react';
import { useDispatch } from 'react-redux';
import { logout } from '../actions/userActions';
import { showSuccessToast } from '../components/CustomToast';
import CustomPrimaryButton from '../components/CustomPrimaryButton';
import DatastoresScreen from './DatastoresScreen';

const DashboardScreen = () => {
  const dispatch = useDispatch();
  const [activeTab, setActiveTab] = useState('dashboard');

  const handleLogout = () => {
    dispatch(logout());
    showSuccessToast('Successfully logged out!');
  };

  const renderContent = () => {
    switch (activeTab) {
      case 'datastores':
        return <DatastoresScreen />;
      case 'matching-engine':
        return (
          <div className="flex items-center justify-center min-h-screen">
            <div className="text-center">
              <h1 className="text-4xl font-bold text-blue-400 mb-4">
                Matching Engine
              </h1>
              <p className="text-xl text-gray-300">
                Coming soon...
              </p>
            </div>
          </div>
        );
      case 'datasets':
        return (
          <div className="flex items-center justify-center min-h-screen">
            <div className="text-center">
              <h1 className="text-4xl font-bold text-purple-400 mb-4">
                Datasets
              </h1>
              <p className="text-xl text-gray-300">
                Coming soon...
              </p>
            </div>
          </div>
        );
      default:
        return (
          <div className="flex items-center justify-center min-h-screen">
            <div className="text-center">
              <h1 className="text-4xl font-bold text-green-400 mb-4">
                Welcome to the Dashboard
              </h1>
              <p className="text-xl text-gray-300">
                You have successfully logged in!
              </p>
            </div>
          </div>
        );
    }
  };

  if (activeTab !== 'dashboard') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro">
        {/* Header Navigation */}
        <div className="bg-gray-800 shadow-lg">
          <div className="p-6">
            <div className="flex justify-between items-center">
              {/* Navigation Items */}
              <div className="flex space-x-8">
                <button
                  onClick={() => setActiveTab('matching-engine')}
                  className={`text-lg font-medium transition-colors duration-200 ${
                    activeTab === 'matching-engine'
                      ? 'text-blue-400 border-b-2 border-blue-400 pb-1'
                      : 'text-gray-300 hover:text-white'
                  }`}
                >
                  Matching Engine
                </button>
                <button
                  onClick={() => setActiveTab('datastores')}
                  className={`text-lg font-medium transition-colors duration-200 ${
                    activeTab === 'datastores'
                      ? 'text-blue-400 border-b-2 border-blue-400 pb-1'
                      : 'text-gray-300 hover:text-white'
                  }`}
                >
                  Datastores
                </button>
                <button
                  onClick={() => setActiveTab('datasets')}
                  className={`text-lg font-medium transition-colors duration-200 ${
                    activeTab === 'datasets'
                      ? 'text-blue-400 border-b-2 border-blue-400 pb-1'
                      : 'text-gray-300 hover:text-white'
                  }`}
                >
                  Datasets
                </button>
              </div>

              {/* Logout Button */}
              <CustomPrimaryButton
                onClick={handleLogout}
                size="md"
                textSize="text-lg"
              >
                Logout
              </CustomPrimaryButton>
            </div>
          </div>
        </div>

        {/* Content */}
        {renderContent()}
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro">
      <div className="p-6">
        {/* Header Navigation */}
        <div className="bg-gray-800 rounded-lg p-6 mb-8">
          <div className="flex justify-between items-center">
            {/* Navigation Items */}
            <div className="flex space-x-8">
              <button
                onClick={() => setActiveTab('matching-engine')}
                className="text-lg font-medium text-gray-300 hover:text-white transition-colors duration-200"
              >
                Matching Engine
              </button>
              <button
                onClick={() => setActiveTab('datastores')}
                className="text-lg font-medium text-gray-300 hover:text-white transition-colors duration-200"
              >
                Datastores
              </button>
              <button
                onClick={() => setActiveTab('datasets')}
                className="text-lg font-medium text-gray-300 hover:text-white transition-colors duration-200"
              >
                Datasets
              </button>
            </div>

            {/* Logout Button */}
            <CustomPrimaryButton
              onClick={handleLogout}
              size="md"
              textSize="text-lg"
            >
              Logout
            </CustomPrimaryButton>
          </div>
        </div>

        {/* Welcome Content */}
        <div className="flex items-center justify-center min-h-screen">
          <div className="text-center">
            <h1 className="text-4xl font-bold text-green-400 mb-4">
              Welcome to the Dashboard
            </h1>
            <p className="text-xl text-gray-300">
              You have successfully logged in!
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardScreen;
