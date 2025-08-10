// src/constants/datasetConstants.js

// Dataset Data Structure Choices
export const DATA_STRUCTURE_CHOICES = [
  { value: "structured", label: "Structured" },
  { value: "semi_structured", label: "Semi-Structured" },
  { value: "unstructured", label: "Unstructured" },
];

// Dataset Growth Rate Choices
export const GROWTH_RATE_CHOICES = [
  { value: "high", label: "High" },
  { value: "medium", label: "Medium" },
  { value: "low", label: "Low" },
];

// Dataset Access Pattern Choices
export const ACCESS_PATTERN_CHOICES = [
  { value: "read_heavy", label: "Read Heavy" },
  { value: "write_heavy", label: "Write Heavy" },
  { value: "read_write_heavy", label: "Read/Write Heavy" },
  { value: "analytical", label: "Analytical" },
  { value: "transactional", label: "Transactional" },
];

// Dataset Query Complexity Choices
export const QUERY_COMPLEXITY_CHOICES = [
  { value: "high", label: "High" },
  { value: "medium", label: "Medium" },
  { value: "low", label: "Low" },
];

// Helper functions to get labels from values
export const getDataStructureLabel = (value) => {
  const choice = DATA_STRUCTURE_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

export const getGrowthRateLabel = (value) => {
  const choice = GROWTH_RATE_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

export const getAccessPatternLabel = (value) => {
  const choice = ACCESS_PATTERN_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

export const getQueryComplexityLabel = (value) => {
  const choice = QUERY_COMPLEXITY_CHOICES.find(choice => choice.value === value);
  return choice ? choice.label : value;
};

// Helper functions to get all values or labels
export const getDataStructureValues = () => DATA_STRUCTURE_CHOICES.map(choice => choice.value);
export const getDataStructureLabels = () => DATA_STRUCTURE_CHOICES.map(choice => choice.label);
export const getGrowthRateValues = () => GROWTH_RATE_CHOICES.map(choice => choice.value);
export const getGrowthRateLabels = () => GROWTH_RATE_CHOICES.map(choice => choice.label);
export const getAccessPatternValues = () => ACCESS_PATTERN_CHOICES.map(choice => choice.value);
export const getAccessPatternLabels = () => ACCESS_PATTERN_CHOICES.map(choice => choice.label);
export const getQueryComplexityValues = () => QUERY_COMPLEXITY_CHOICES.map(choice => choice.value);
export const getQueryComplexityLabels = () => QUERY_COMPLEXITY_CHOICES.map(choice => choice.label);

// Object format for easy lookups (alternative approach)
export const DATA_STRUCTURE_CHOICES_MAP = {
  structured: "Structured",
  semi_structured: "Semi-Structured",
  unstructured: "Unstructured",
};

export const GROWTH_RATE_CHOICES_MAP = {
  high: "High",
  medium: "Medium",
  low: "Low",
};

export const ACCESS_PATTERN_CHOICES_MAP = {
  read_heavy: "Read Heavy",
  write_heavy: "Write Heavy",
  read_write_heavy: "Read/Write Heavy",
  analytical: "Analytical",
  transactional: "Transactional",
};

export const QUERY_COMPLEXITY_CHOICES_MAP = {
  high: "High",
  medium: "Medium",
  low: "Low",
};

// Reverse mapping for getting values from labels
export const DATA_STRUCTURE_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(DATA_STRUCTURE_CHOICES_MAP).map(([key, value]) => [value, key])
);

export const GROWTH_RATE_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(GROWTH_RATE_CHOICES_MAP).map(([key, value]) => [value, key])
);

export const ACCESS_PATTERN_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(ACCESS_PATTERN_CHOICES_MAP).map(([key, value]) => [value, key])
);

export const QUERY_COMPLEXITY_LABELS_TO_VALUES = Object.fromEntries(
  Object.entries(QUERY_COMPLEXITY_CHOICES_MAP).map(([key, value]) => [value, key])
);

// Color coding for UI representation (optional - for badges, charts, etc.)
export const DATA_STRUCTURE_COLORS = {
  structured: "#10B981", // green
  semi_structured: "#F59E0B", // amber
  unstructured: "#EF4444", // red
};

export const GROWTH_RATE_COLORS = {
  high: "#EF4444", // red
  medium: "#F59E0B", // amber
  low: "#10B981", // green
};

export const ACCESS_PATTERN_COLORS = {
  read_heavy: "#3B82F6", // blue
  write_heavy: "#8B5CF6", // purple
  read_write_heavy: "#EC4899", // pink
  analytical: "#06B6D4", // cyan
  transactional: "#84CC16", // lime
};

export const QUERY_COMPLEXITY_COLORS = {
  high: "#EF4444", // red
  medium: "#F59E0B", // amber
  low: "#10B981", // green
};