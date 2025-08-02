import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './index.css';

function App() {
  const [datastores, setDatastores] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        console.log('Fetching data from API...');
        
        // Fetch datastores and datasets from your Django API
        const [datastoreResponse, datasetResponse] = await Promise.all([
          axios.get('/api/datastores/'),
          axios.get('/api/datasets/')
        ]);
        
        console.log('Datastore response:', datastoreResponse.data);
        console.log('Dataset response:', datasetResponse.data);
        
        // Handle paginated response format
        const datastoreData = datastoreResponse.data.results || datastoreResponse.data;
        const datasetData = datasetResponse.data.results || datasetResponse.data;
        
        setDatastores(Array.isArray(datastoreData) ? datastoreData : []);
        setDatasets(Array.isArray(datasetData) ? datasetData : []);
        setLoading(false);
      } catch (err) {
        console.error('API Error:', err);
        setError(`Failed to fetch data: ${err.message}`);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div className="App">
      <div className="header">
        <h1>Dataset Datastore Recommendation System</h1>
        <p>Manage and recommend datastores for your datasets</p>
      </div>
      
      <div className="container">
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
          <div>
            <h2>Datastores ({datastores.length})</h2>
            <div style={{ textAlign: 'left' }}>
              {datastores.length > 0 ? (
                datastores.map((datastore, index) => (
                  <div key={datastore.id || index} style={{ 
                    border: '1px solid #ddd', 
                    padding: '10px', 
                    margin: '10px 0',
                    borderRadius: '5px',
                    backgroundColor: datastore.is_active ? '#f0f8ff' : '#f5f5f5'
                  }}>
                    <h3>
                      {datastore.name || `Datastore ${index + 1}`}
                      {datastore.is_active ? ' ✅' : ' ❌'}
                    </h3>
                    <p><strong>Type:</strong> {datastore.type_display || datastore.type}</p>
                    <p><strong>System:</strong> {datastore.system_display || datastore.system}</p>
                    <p>{datastore.description || 'No description available'}</p>
                    {datastore.avg_response_time_ms && (
                      <p><small>Avg Response: {datastore.avg_response_time_ms}ms</small></p>
                    )}
                  </div>
                ))
              ) : (
                <p>No datastores found. Create some through the Django admin or API.</p>
              )}
            </div>
          </div>
          
          <div>
            <h2>Datasets ({datasets.length})</h2>
            <div style={{ textAlign: 'left' }}>
              {datasets.length > 0 ? (
                datasets.map((dataset, index) => (
                  <div key={dataset.id || index} style={{ 
                    border: '1px solid #ddd', 
                    padding: '10px', 
                    margin: '10px 0',
                    borderRadius: '5px'
                  }}>
                    <h3>{dataset.name || `Dataset ${index + 1}`}</h3>
                    <p><strong>Structure:</strong> {dataset.data_structure}</p>
                    <p><strong>Access Pattern:</strong> {dataset.access_patterns}</p>
                    <p>{dataset.short_description || 'No description available'}</p>
                    {dataset.estimated_size_gb && (
                      <p><small>Size: {dataset.estimated_size_gb}GB</small></p>
                    )}
                  </div>
                ))
              ) : (
                <p>No datasets found. Create some through the Django admin or API.</p>
              )}
            </div>
          </div>
        </div>
        
        <div style={{ marginTop: '40px', textAlign: 'center' }}>
          <h3>Quick Links</h3>
          <div style={{ display: 'flex', justifyContent: 'center', gap: '20px' }}>
            <a 
              href="/admin" 
              target="_blank" 
              rel="noopener noreferrer"
              style={{ 
                padding: '10px 20px', 
                backgroundColor: '#007bff', 
                color: 'white', 
                textDecoration: 'none',
                borderRadius: '5px'
              }}
            >
              Django Admin
            </a>
            <a 
              href="/api" 
              target="_blank" 
              rel="noopener noreferrer"
              style={{ 
                padding: '10px 20px', 
                backgroundColor: '#28a745', 
                color: 'white', 
                textDecoration: 'none',
                borderRadius: '5px'
              }}
            >
              API Documentation
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
