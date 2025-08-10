// Datastore Type Choices
export const TYPE_CHOICES = [
  { value: "sql", label: "SQL Database" },
  { value: "document", label: "Document Store" },
  { value: "keyvalue", label: "Key-Value Store" },
  { value: "graph", label: "Graph Database" },
  { value: "column", label: "Column Store" },
];

// Datastore System Choices
export const SYSTEM_CHOICES = [
  { value: "postgres", label: "PostgreSQL" },
  { value: "mysql", label: "MySQL" },
  { value: "mongodb", label: "MongoDB" },
  { value: "redis", label: "Redis" },
  { value: "neo4j", label: "Neo4J" },
  { value: "cassandra", label: "Cassandra" },
];

// System compatibility mapping
export const SYSTEM_COMPATIBILITY = {
  sql: ["postgres", "mysql"],
  document: ["mongodb"],
  keyvalue: ["redis"],
  graph: ["neo4j"],
  column: ["cassandra"],
};

// Helper functions to get labels from values
export const getTypeLabel = (value) => {
  const choice = TYPE_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

export const getSystemLabel = (value) => {
  const choice = SYSTEM_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

// Helper function to get compatible systems for a type
export const getCompatibleSystems = (type) => {
  const compatibleValues = SYSTEM_COMPATIBILITY[type] || [];
  return SYSTEM_CHOICES.filter(system => compatibleValues.includes(system.value));
};

// Redux action types
export const DATASTORE_LIST_REQUEST = 'DATASTORE_LIST_REQUEST';
export const DATASTORE_LIST_SUCCESS = 'DATASTORE_LIST_SUCCESS';
export const DATASTORE_LIST_FAIL = 'DATASTORE_LIST_FAIL';

export const DATASTORE_CREATE_REQUEST = 'DATASTORE_CREATE_REQUEST';
export const DATASTORE_CREATE_SUCCESS = 'DATASTORE_CREATE_SUCCESS';
export const DATASTORE_CREATE_FAIL = 'DATASTORE_CREATE_FAIL';
export const DATASTORE_CREATE_RESET = 'DATASTORE_CREATE_RESET';

export const DATASTORE_UPDATE_REQUEST = 'DATASTORE_UPDATE_REQUEST';
export const DATASTORE_UPDATE_SUCCESS = 'DATASTORE_UPDATE_SUCCESS';
export const DATASTORE_UPDATE_FAIL = 'DATASTORE_UPDATE_FAIL';
export const DATASTORE_UPDATE_RESET = 'DATASTORE_UPDATE_RESET';

export const DATASTORE_DELETE_REQUEST = 'DATASTORE_DELETE_REQUEST';
export const DATASTORE_DELETE_SUCCESS = 'DATASTORE_DELETE_SUCCESS';
export const DATASTORE_DELETE_FAIL = 'DATASTORE_DELETE_FAIL';

// Helper functions to get all values or labels
export const getTypeValues = () => TYPE_CHOICES.map(choice => choice.value);
export const getTypeLabels = () => TYPE_CHOICES.map(choice => choice.label);
export const getSystemValues = () => SYSTEM_CHOICES.map(choice => choice.value);
export const getSystemLabels = () => SYSTEM_CHOICES.map(choice => choice.label);

// Object format for easy lookups (alternative approach)
export const TYPE_CHOICES_MAP = {
  sql: "SQL Database",
  document: "Document Store",
  keyvalue: "Key-Value Store",
  graph: "Graph Database",
  column: "Column Store",
};

export const SYSTEM_CHOICES_MAP = {
  postgres: "PostgreSQL",
  mysql: "MySQL",
  mongodb: "MongoDB",
  redis: "Redis",
  neo4j: "Neo4J",
  cassandra: "Cassandra",
};

// Reverse mapping for getting values from labels
export const TYPE_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(TYPE_CHOICES_MAP).map(([key, value]) => [value, key])
);

export const SYSTEM_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(SYSTEM_CHOICES_MAP).map(([key, value]) => [value, key])
);