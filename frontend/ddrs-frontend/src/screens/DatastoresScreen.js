import React, { useState, useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { 
  listDatastores, 
  createDatastore, 
  resetDatastoreCreate,
  updateDatastore,
  resetDatastoreUpdate,
  deleteDatastore
} from '../actions/datastoreActions';
import { showSuccessToast, showErrorToast } from '../components/CustomToast';
import CustomTable from '../components/CustomTable';
import CustomSecondaryButton from '../components/CustomSecondaryButton';
import CustomModal from '../components/CustomModal';
import CustomInput from '../components/CustomInput';
import CustomSelect from '../components/CustomSelect';
import CustomSwitch from '../components/CustomSwitch';
import CustomLoadingSpinner from '../components/CustomLoadingSpinner';
import BurgerMenu from '../components/BurgerMenu';
import DeleteConfirmationModal from '../components/DeleteConfirmationModal';
import {
  TYPE_CHOICES,
  getCompatibleSystems,
  getTypeLabel,
  getSystemLabel,
} from '../constants/datastoreConstants';

const DatastoresScreen = () => {
  const dispatch = useDispatch();
  
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isUpdateMode, setIsUpdateMode] = useState(false);
  const [selectedDatastore, setSelectedDatastore] = useState(null);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [datastoreToDelete, setDatastoreToDelete] = useState(null);
  const [passwordMismatch, setPasswordMismatch] = useState(false);
  const [formData, setFormData] = useState({
    name: '',
    type: '',
    system: '',
    description: '',
    server: '',
    port: '',
    username: '',
    password: '',
    password_confirm: '',
    is_active: true,
    max_connections: '',
    avg_response_time_ms: '',
    storage_capacity_gb: '',
  });

  const datastoreList = useSelector((state) => state.datastoreList);
  const { loading, error, datastores } = datastoreList;

  const datastoreCreate = useSelector((state) => state.datastoreCreate);
  const { 
    loading: createLoading, 
    error: createError, 
    success: createSuccess 
  } = datastoreCreate;

  const datastoreUpdate = useSelector((state) => state.datastoreUpdate);
  const { 
    loading: updateLoading, 
    error: updateError, 
    success: updateSuccess 
  } = datastoreUpdate;

  const datastoreDelete = useSelector((state) => state.datastoreDelete);
  const { 
    loading: deleteLoading, 
    error: deleteError, 
    success: deleteSuccess 
  } = datastoreDelete;

  useEffect(() => {
    dispatch(listDatastores());
  }, [dispatch]);

  useEffect(() => {
    if (createSuccess) {
      showSuccessToast('Datastore created successfully');
      setIsModalOpen(false);
      resetForm();
      dispatch(resetDatastoreCreate());
      dispatch(listDatastores()); // Refresh the list
    }
  }, [createSuccess, dispatch]);

  useEffect(() => {
    if (updateSuccess) {
      showSuccessToast('Datastore updated successfully');
      setIsModalOpen(false);
      resetForm();
      dispatch(resetDatastoreUpdate());
      dispatch(listDatastores()); // Refresh the list
    }
  }, [updateSuccess, dispatch]);

  useEffect(() => {
    if (deleteSuccess) {
      showSuccessToast('Datastore deleted successfully');
      setIsDeleteModalOpen(false);
      setDatastoreToDelete(null);
      dispatch(listDatastores()); // Refresh the list
    }
  }, [deleteSuccess, dispatch]);

  useEffect(() => {
    if (createError) {
      let errorMessage = 'Failed to create the Datastore';
      if (typeof createError === 'string') {
        errorMessage += `: ${createError}`;
      } else if (createError && typeof createError === 'object') {
        // Handle validation errors from backend
        const errorMessages = [];
        Object.keys(createError).forEach(key => {
          if (Array.isArray(createError[key])) {
            errorMessages.push(`${key}: ${createError[key].join(', ')}`);
          } else {
            errorMessages.push(`${key}: ${createError[key]}`);
          }
        });
        if (errorMessages.length > 0) {
          errorMessage += `: ${errorMessages.join('; ')}`;
        }
      }
      showErrorToast(errorMessage);
    }
  }, [createError]);

  useEffect(() => {
    if (updateError) {
      let errorMessage = 'Failed to update the Datastore';
      if (typeof updateError === 'string') {
        errorMessage += `: ${updateError}`;
      } else if (updateError && typeof updateError === 'object') {
        const errorMessages = [];
        Object.keys(updateError).forEach(key => {
          if (Array.isArray(updateError[key])) {
            errorMessages.push(`${key}: ${updateError[key].join(', ')}`);
          } else {
            errorMessages.push(`${key}: ${updateError[key]}`);
          }
        });
        if (errorMessages.length > 0) {
          errorMessage += `: ${errorMessages.join('; ')}`;
        }
      }
      showErrorToast(errorMessage);
    }
  }, [updateError]);

  useEffect(() => {
    if (deleteError) {
      showErrorToast(`Failed to delete the Datastore: ${deleteError}`);
    }
  }, [deleteError]);

  const resetForm = () => {
    setFormData({
      name: '',
      type: '',
      system: '',
      description: '',
      server: '',
      port: '',
      username: '',
      password: '',
      password_confirm: '',
      is_active: true,
      max_connections: '',
      avg_response_time_ms: '',
      storage_capacity_gb: '',
    });
    setPasswordMismatch(false);
    setIsUpdateMode(false);
    setSelectedDatastore(null);
  };

  const handleInputChange = (e) => {
    const { name, value, type, checked } = e.target;
    const newFormData = {
      ...formData,
      [name]: type === 'checkbox' ? checked : value,
      // Reset system when type changes
      ...(name === 'type' ? { system: '' } : {})
    };
    
    setFormData(newFormData);
    
    // Check password confirmation on the fly
    if (name === 'password' || name === 'password_confirm') {
      const password = name === 'password' ? value : newFormData.password;
      const confirmPassword = name === 'password_confirm' ? value : newFormData.password_confirm;
      
      // Only show mismatch if both fields have values and they don't match
      if (password && confirmPassword && password !== confirmPassword) {
        setPasswordMismatch(true);
      } else {
        setPasswordMismatch(false);
      }
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    
    // Validate required fields
    if (!formData.name || !formData.type || !formData.system) {
      showErrorToast('Please fill in all required fields');
      return;
    }

    // Validate password confirmation
    if (formData.password && formData.password !== formData.password_confirm) {
      showErrorToast('Passwords do not match');
      return;
    }

    // Validate numeric fields
    const numericFields = ['port', 'max_connections', 'avg_response_time_ms', 'storage_capacity_gb'];
    for (const field of numericFields) {
      if (formData[field] && (isNaN(formData[field]) || parseFloat(formData[field]) <= 0)) {
        showErrorToast(`${field.replace(/_/g, ' ')} must be a number greater than 0`);
        return;
      }
    }

    // Prepare data for submission
    const submitData = { ...formData };
    
    // Convert numeric fields
    numericFields.forEach(field => {
      if (submitData[field]) {
        submitData[field] = parseFloat(submitData[field]);
      } else {
        delete submitData[field];
      }
    });

    // Remove empty string fields and password confirmation
    Object.keys(submitData).forEach(key => {
      if (submitData[key] === '' || key === 'password_confirm') {
        delete submitData[key];
      }
    });

    if (isUpdateMode && selectedDatastore) {
      dispatch(updateDatastore(selectedDatastore.id, submitData));
    } else {
      dispatch(createDatastore(submitData));
    }
  };

  const openModal = () => {
    resetForm();
    dispatch(resetDatastoreCreate());
    setIsModalOpen(true);
  };

  const closeModal = () => {
    setIsModalOpen(false);
    resetForm();
    dispatch(resetDatastoreCreate());
    dispatch(resetDatastoreUpdate());
  };

  const handleUpdate = (datastore) => {
    setSelectedDatastore(datastore);
    setIsUpdateMode(true);
    setFormData({
      name: datastore.name || '',
      type: datastore.type || '',
      system: datastore.system || '',
      description: datastore.description || '',
      server: datastore.server || '',
      port: datastore.port || '',
      username: datastore.username || '',
      password: '',
      password_confirm: '',
      is_active: datastore.is_active ?? true,
      max_connections: datastore.max_connections || '',
      avg_response_time_ms: datastore.avg_response_time_ms || '',
      storage_capacity_gb: datastore.storage_capacity_gb || '',
    });
    dispatch(resetDatastoreUpdate());
    setIsModalOpen(true);
  };

  const handleDeleteClick = (datastore) => {
    setDatastoreToDelete(datastore);
    setIsDeleteModalOpen(true);
  };

  const handleDeleteConfirm = () => {
    if (datastoreToDelete) {
      dispatch(deleteDatastore(datastoreToDelete.id));
    }
  };

  const handleDeleteCancel = () => {
    setIsDeleteModalOpen(false);
    setDatastoreToDelete(null);
  };

  const isFormValid = () => {
    return formData.name && formData.type && formData.system;
  };

  const availableSystems = formData.type ? getCompatibleSystems(formData.type) : [];

  const columns = [
    { 
      Header: 'Created At', 
      accessor: 'created_at',
      Cell: ({ value }) => new Date(value).toLocaleDateString('de-DE')
    },
    { 
      Header: 'Updated At', 
      accessor: 'updated_at',
      Cell: ({ value }) => new Date(value).toLocaleDateString('de-DE')
    },
    { Header: 'Name', accessor: 'name' },
    { 
      Header: 'Type', 
      accessor: 'type_display',
      Cell: ({ value, row }) => row.type_display || getTypeLabel(row.type)
    },
    { 
      Header: 'System', 
      accessor: 'system_display',
      Cell: ({ value, row }) => row.system_display || getSystemLabel(row.system)
    },
    { Header: 'Description', accessor: 'description' },
    { 
      Header: 'Active', 
      accessor: 'is_active',
      Cell: ({ value }) => value ? 'Yes' : 'No'
    },
    { Header: 'Max Connections', accessor: 'max_connections' },
    { Header: 'Average Response Time', accessor: 'avg_response_time_ms' },
    { Header: 'Storage Capacity (GB)', accessor: 'storage_capacity_gb' },
    {
      Header: 'Actions',
      accessor: 'actions',
      is_Action: true,
      Cell: ({ row }) => (
        <BurgerMenu
          onUpdate={() => handleUpdate(row)}
          onDelete={() => handleDeleteClick(row)}
        />
      ),
    },
  ];

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro flex items-center justify-center">
        <CustomLoadingSpinner />
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-red-400 mb-4">Error Loading Datastores</h2>
          <p className="text-gray-300">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-950 text-white font-markpro">
      <div className="p-6">
        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-3xl font-bold">Datastores</h1>
          <CustomSecondaryButton
            onClick={openModal}
            color="blue"
            size="md"
          >
            Create New
          </CustomSecondaryButton>
        </div>

        {/* Table */}
        <div className="bg-gray-800 rounded-lg p-6">
          <CustomTable
            columns={columns}
            data={datastores}
            itemsPerPage={10}
          />
        </div>

        {/* Create/Update Modal */}
        <CustomModal
          isOpen={isModalOpen}
          onClose={closeModal}
          title={isUpdateMode ? "Update Datastore" : "Create a new Datastore"}
          size="lg"
        >
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <CustomInput
                label="Name *"
                id="name"
                name="name"
                value={formData.name}
                onChange={handleInputChange}
                required
                placeholder="Enter datastore name"
              />

              <CustomSelect
                label="Type *"
                id="type"
                name="type"
                value={formData.type}
                onChange={handleInputChange}
                options={TYPE_CHOICES}
                required
                placeholder="Select datastore type"
              />

              <CustomSelect
                label="System *"
                id="system"
                name="system"
                value={formData.system}
                onChange={handleInputChange}
                options={availableSystems}
                required
                disabled={!formData.type}
                placeholder="Select system"
              />

              <div className="md:col-span-2">
                <CustomInput
                  label="Description"
                  id="description"
                  name="description"
                  value={formData.description}
                  onChange={handleInputChange}
                  placeholder="Enter description"
                />
              </div>

              <CustomInput
                label="Server"
                id="server"
                name="server"
                value={formData.server}
                onChange={handleInputChange}
                placeholder="Enter server address"
              />

              <CustomInput
                label="Port"
                id="port"
                name="port"
                type="number"
                value={formData.port}
                onChange={handleInputChange}
                placeholder="Enter port number"
              />

              <CustomInput
                label="User"
                id="username"
                name="username"
                value={formData.username}
                onChange={handleInputChange}
                placeholder="Enter username"
              />

              <div className="flex items-center pt-8">
                <CustomSwitch
                  label="Active"
                  id="is_active"
                  name="is_active"
                  checked={formData.is_active}
                  onChange={handleInputChange}
                />
              </div>

              <CustomInput
                label="Password"
                id="password"
                name="password"
                type="password"
                value={formData.password}
                onChange={handleInputChange}
                placeholder="Enter password"
              />

              <CustomInput
                label="Confirm Password"
                id="password_confirm"
                name="password_confirm"
                type="password"
                value={formData.password_confirm}
                onChange={handleInputChange}
                placeholder="Confirm password"
              />
              
              {passwordMismatch && (
                <div className="text-red-400 text-sm font-markpro flex items-center">
                  <svg className="w-4 h-4 mr-2" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                  </svg>
                  Passwords do not match
                </div>
              )}

              <CustomInput
                label="Max Connections"
                id="max_connections"
                name="max_connections"
                type="number"
                value={formData.max_connections}
                onChange={handleInputChange}
                placeholder="Enter max connections"
              />

              <CustomInput
                label="Average Response Time"
                id="avg_response_time_ms"
                name="avg_response_time_ms"
                type="number"
                step="0.01"
                value={formData.avg_response_time_ms}
                onChange={handleInputChange}
                placeholder="Enter response time (ms)"
              />

              <CustomInput
                label="Storage Capacity in GB"
                id="storage_capacity_gb"
                name="storage_capacity_gb"
                type="number"
                step="0.01"
                value={formData.storage_capacity_gb}
                onChange={handleInputChange}
                placeholder="Enter storage capacity"
              />
            </div>

            <div className="flex justify-end space-x-4 pt-6">
              <CustomSecondaryButton
                type="button"
                onClick={closeModal}
                color="gray"
                disabled={createLoading || updateLoading}
              >
                Cancel
              </CustomSecondaryButton>
              <CustomSecondaryButton
                type="submit"
                color="blue"
                disabled={!isFormValid() || createLoading || updateLoading}
              >
                {(createLoading || updateLoading) ? (
                  <div className="flex items-center space-x-2">
                    <CustomLoadingSpinner size={4} text="" />
                    <span>{isUpdateMode ? 'Updating...' : 'Creating...'}</span>
                  </div>
                ) : (
                  isUpdateMode ? 'Update' : 'Create'
                )}
              </CustomSecondaryButton>
            </div>
          </form>
        </CustomModal>

        {/* Delete Confirmation Modal */}
        <DeleteConfirmationModal
          isOpen={isDeleteModalOpen}
          onClose={handleDeleteCancel}
          onConfirm={handleDeleteConfirm}
          datastoreName={datastoreToDelete?.name || ''}
          loading={deleteLoading}
        />
      </div>
    </div>
  );
};

export default DatastoresScreen;
