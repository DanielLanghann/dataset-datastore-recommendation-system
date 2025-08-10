import axios from 'axios';
import {
  DATASTORE_LIST_REQUEST,
  DATASTORE_LIST_SUCCESS,
  DATASTORE_LIST_FAIL,
  DATASTORE_CREATE_REQUEST,
  DATASTORE_CREATE_SUCCESS,
  DATASTORE_CREATE_FAIL,
  DATASTORE_CREATE_RESET,
  DATASTORE_UPDATE_REQUEST,
  DATASTORE_UPDATE_SUCCESS,
  DATASTORE_UPDATE_FAIL,
  DATASTORE_UPDATE_RESET,
  DATASTORE_DELETE_REQUEST,
  DATASTORE_DELETE_SUCCESS,
  DATASTORE_DELETE_FAIL,
} from '../constants/datastoreConstants';

const baseUrl = process.env.REACT_APP_BASE_URL || 'http://localhost:8000';

export const listDatastores = () => async (dispatch, getState) => {
  try {
    dispatch({
      type: DATASTORE_LIST_REQUEST,
    });

    const {
      userLogin: { userInfo },
    } = getState();

    const config = {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Token ${userInfo.token}`,
      },
    };

    const { data } = await axios.get(`${baseUrl}/api/datastores/`, config);

    dispatch({
      type: DATASTORE_LIST_SUCCESS,
      payload: data,
    });
  } catch (error) {
    dispatch({
      type: DATASTORE_LIST_FAIL,
      payload:
        error.response && error.response.data.detail
          ? error.response.data.detail
          : error.message,
    });
  }
};

export const createDatastore = (datastoreData) => async (dispatch, getState) => {
  try {
    dispatch({
      type: DATASTORE_CREATE_REQUEST,
    });

    const {
      userLogin: { userInfo },
    } = getState();

    const config = {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Token ${userInfo.token}`,
      },
    };

    const { data } = await axios.post(
      `${baseUrl}/api/datastores/`,
      datastoreData,
      config
    );

    dispatch({
      type: DATASTORE_CREATE_SUCCESS,
      payload: data,
    });
  } catch (error) {
    dispatch({
      type: DATASTORE_CREATE_FAIL,
      payload:
        error.response && error.response.data.detail
          ? error.response.data.detail
          : error.response && error.response.data
          ? error.response.data
          : error.message,
    });
  }
};

export const resetDatastoreCreate = () => ({
  type: DATASTORE_CREATE_RESET,
});

export const updateDatastore = (id, datastoreData) => async (dispatch, getState) => {
  try {
    dispatch({
      type: DATASTORE_UPDATE_REQUEST,
    });

    const {
      userLogin: { userInfo },
    } = getState();

    const config = {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Token ${userInfo.token}`,
      },
    };

    const { data } = await axios.put(
      `${baseUrl}/api/datastores/${id}/`,
      datastoreData,
      config
    );

    dispatch({
      type: DATASTORE_UPDATE_SUCCESS,
      payload: data,
    });
  } catch (error) {
    dispatch({
      type: DATASTORE_UPDATE_FAIL,
      payload:
        error.response && error.response.data.detail
          ? error.response.data.detail
          : error.response && error.response.data
          ? error.response.data
          : error.message,
    });
  }
};

export const resetDatastoreUpdate = () => ({
  type: DATASTORE_UPDATE_RESET,
});

export const deleteDatastore = (id) => async (dispatch, getState) => {
  try {
    dispatch({
      type: DATASTORE_DELETE_REQUEST,
    });

    const {
      userLogin: { userInfo },
    } = getState();

    const config = {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Token ${userInfo.token}`,
      },
    };

    await axios.delete(`${baseUrl}/api/datastores/${id}/`, config);

    dispatch({
      type: DATASTORE_DELETE_SUCCESS,
      payload: id,
    });
  } catch (error) {
    dispatch({
      type: DATASTORE_DELETE_FAIL,
      payload:
        error.response && error.response.data.detail
          ? error.response.data.detail
          : error.response && error.response.data
          ? error.response.data
          : error.message,
    });
  }
};
