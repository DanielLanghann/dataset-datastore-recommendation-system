import React from 'react';
import { useDispatch } from 'react-redux';
import { logout } from '../actions/userActions';
import { showSuccessToast } from '../components/CustomToast';
import CustomPrimaryButton from '../components/CustomPrimaryButton';

const DashboardScreen = () => {
  const dispatch = useDispatch();

  const handleLogout = () => {
    dispatch(logout());
    showSuccessToast('Successfully logged out!');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro">
      <div className="p-6">
        <div className="flex justify-end">
          <CustomPrimaryButton
            onClick={handleLogout}
            size="md"
            textSize="text-lg"
          >
            Logout
          </CustomPrimaryButton>
        </div>
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
