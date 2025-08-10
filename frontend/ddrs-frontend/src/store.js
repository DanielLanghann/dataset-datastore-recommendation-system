import { configureStore } from '@reduxjs/toolkit';
import { userLoginReducer } from './reducers/userReducers';
import { 
  datastoreListReducer, 
  datastoreCreateReducer, 
  datastoreUpdateReducer, 
  datastoreDeleteReducer 
} from './reducers/datastoreReducers';

// Get user info from localStorage if available
const userInfoFromStorage = localStorage.getItem('userInfo')
  ? JSON.parse(localStorage.getItem('userInfo'))
  : null;

const preloadedState = {
  userLogin: { userInfo: userInfoFromStorage },
};

const store = configureStore({
  reducer: {
    userLogin: userLoginReducer,
    datastoreList: datastoreListReducer,
    datastoreCreate: datastoreCreateReducer,
    datastoreUpdate: datastoreUpdateReducer,
    datastoreDelete: datastoreDeleteReducer,
  },
  preloadedState,
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false,
    }),
});

export default store;