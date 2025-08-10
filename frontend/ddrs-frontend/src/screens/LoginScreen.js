import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useDispatch, useSelector } from "react-redux";
import { login } from "../actions/userActions";
import { showErrorToast } from "../components/CustomToast";

import CustomInput from "../components/CustomInput";
import CustomPrimaryButton from "../components/CustomPrimaryButton";
import CustomLoadingSpinner from "../components/CustomLoadingSpinner";

const LoginScreen = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const navigate = useNavigate();
  const dispatch = useDispatch();

  const userLogin = useSelector((state) => state.userLogin);
  const { loading, error, userInfo } = userLogin;

  useEffect(() => {
    if (userInfo) {
      navigate("/dashboard");
    }
  }, [navigate, userInfo]);

  useEffect(() => {
    if (error) {
      showErrorToast(error);
    }
  }, [error]);

  const handleLogin = (e) => {
    e.preventDefault();
    dispatch(login(username, password));
  };

  const isFormValid = username.trim() !== "" && password.trim() !== "";

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white flex items-center justify-center font-markpro">
      <div className="bg-gray-800 p-12 rounded-lg shadow-lg w-full max-w-4xl text-white">
        <div className="text-center mb-10">
          <h1 className="text-5xl font-bold text-green-400">
            Welcome
          </h1>
        </div>
        <form onSubmit={handleLogin} className="space-y-8">
          <CustomInput
            label="Username"
            id="username"
            type="text"
            placeholder="Enter your username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus={true}
          />
          <CustomInput
            label="Password"
            id="password"
            type="password"
            placeholder="Enter your password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          <CustomPrimaryButton
            type="submit"
            disabled={loading || !isFormValid}
            fullWidth={true}
          >
            {loading ? (
              <CustomLoadingSpinner size={6} text="Logging in..." />
            ) : (
              "Login"
            )}
          </CustomPrimaryButton>
        </form>
      </div>
    </div>
  );
};

export default LoginScreen;