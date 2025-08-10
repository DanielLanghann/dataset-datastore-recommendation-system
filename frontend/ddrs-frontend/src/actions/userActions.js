import axios from 'axios';
import {
  USER_LOGIN_REQUEST,
  USER_LOGIN_SUCCESS,
  USER_LOGIN_FAIL,
  USER_LOGOUT,
} from '../constants/userConstants';

const baseUrl = process.env.REACT_APP_BASE_URL;
const loginEndpoint = process.env.REACT_APP_LOGIN_ENDPOINT;
const logoutEndpoint = process.env.REACT_APP_LOGOUT_ENDPOINT;

export const login = (username, password) => async (dispatch) => {
  try {
    dispatch({
      type: USER_LOGIN_REQUEST,
    });

    const config = {
      headers: {
        'Content-Type': 'application/json',
      },
    };

    const { data } = await axios.post(
      `${baseUrl}${loginEndpoint}`,
      { username, password },
      config
    );

    dispatch({
      type: USER_LOGIN_SUCCESS,
      payload: data,
    });

    // Store token in localStorage
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (error) {
    dispatch({
      type: USER_LOGIN_FAIL,
      payload:
        error.response && error.response.data.detail
          ? error.response.data.detail
          : error.message,
    });
  }
};

export const logout = () => async (dispatch, getState) => {
  try {
    const {
      userLogin: { userInfo },
    } = getState();

    if (userInfo && userInfo.token) {
      const config = {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Token ${userInfo.token}`,
        },
      };

      await axios.post(`${baseUrl}${logoutEndpoint}`, {}, config);
    }
  } catch (error) {
    console.error('Logout error:', error);
  } finally {
    // Always clear local storage and dispatch logout action
    localStorage.removeItem('userInfo');
    dispatch({ type: USER_LOGOUT });
  }
};
