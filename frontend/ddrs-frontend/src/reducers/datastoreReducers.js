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

export const datastoreListReducer = (state = { datastores: [] }, action) => {
  switch (action.type) {
    case DATASTORE_LIST_REQUEST:
      return { loading: true, datastores: [] };
    case DATASTORE_LIST_SUCCESS:
      return { loading: false, datastores: action.payload.results || action.payload };
    case DATASTORE_LIST_FAIL:
      return { loading: false, error: action.payload };
    case DATASTORE_DELETE_SUCCESS:
      return {
        ...state,
        datastores: state.datastores.filter(datastore => datastore.id !== action.payload)
      };
    default:
      return state;
  }
};

export const datastoreCreateReducer = (state = {}, action) => {
  switch (action.type) {
    case DATASTORE_CREATE_REQUEST:
      return { loading: true };
    case DATASTORE_CREATE_SUCCESS:
      return { loading: false, success: true, datastore: action.payload };
    case DATASTORE_CREATE_FAIL:
      return { loading: false, error: action.payload };
    case DATASTORE_CREATE_RESET:
      return {};
    default:
      return state;
  }
};

export const datastoreUpdateReducer = (state = {}, action) => {
  switch (action.type) {
    case DATASTORE_UPDATE_REQUEST:
      return { loading: true };
    case DATASTORE_UPDATE_SUCCESS:
      return { loading: false, success: true, datastore: action.payload };
    case DATASTORE_UPDATE_FAIL:
      return { loading: false, error: action.payload };
    case DATASTORE_UPDATE_RESET:
      return {};
    default:
      return state;
  }
};

export const datastoreDeleteReducer = (state = {}, action) => {
  switch (action.type) {
    case DATASTORE_DELETE_REQUEST:
      return { loading: true };
    case DATASTORE_DELETE_SUCCESS:
      return { loading: false, success: true };
    case DATASTORE_DELETE_FAIL:
      return { loading: false, error: action.payload };
    default:
      return state;
  }
};
